// Package rpcnet is the real transport: net/rpc over TCP. It carries the same
// opaque (method, payload) envelopes as simnet, through a single RPC method.
package rpcnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"

	"github.com/silviacioroiu/raftkv/transport"
)

const deliverMethod = "Transport.Deliver"

// Envelope and Reply are the wire format. They are exported only because
// net/rpc requires exported argument types.
type Envelope struct {
	From    transport.NodeID
	Method  string
	Payload []byte
}

type Reply struct {
	Payload []byte
	Failed  bool
	Err     string
}

// Service is the receiver registered with net/rpc.
type Service struct {
	t *Transport
}

// Deliver always returns nil: handler errors travel inside Reply, so that a
// non-nil error on the client side always means the connection is broken.
func (s *Service) Deliver(env *Envelope, reply *Reply) error {
	h := s.t.currentHandler()
	if h == nil {
		reply.Failed, reply.Err = true, "no handler installed"
		return nil
	}
	out, err := h(s.t.ctx, env.From, env.Method, env.Payload)
	if err != nil {
		reply.Failed, reply.Err = true, err.Error()
		return nil
	}
	reply.Payload = out
	return nil
}

type Transport struct {
	id     transport.NodeID
	ln     net.Listener
	srv    *rpc.Server
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	handler transport.Handler
	peers   map[transport.NodeID]string
	clients map[transport.NodeID]*rpc.Client
	conns   map[net.Conn]struct{} // accepted, still open
	closed  bool

	wg sync.WaitGroup // accept loop and per-connection servers
}

var _ transport.Transport = (*Transport)(nil)

// Listen starts serving on addr. Use port 0 to let the OS pick one and read
// it back with Addr. Peers are added afterwards with SetPeer.
func Listen(id transport.NodeID, addr string) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rpcnet: listen %s: %w", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		id:      id,
		ln:      ln,
		srv:     rpc.NewServer(),
		ctx:     ctx,
		cancel:  cancel,
		peers:   make(map[transport.NodeID]string),
		clients: make(map[transport.NodeID]*rpc.Client),
		conns:   make(map[net.Conn]struct{}),
	}
	if err := t.srv.RegisterName("Transport", &Service{t: t}); err != nil {
		cancel()
		ln.Close()
		return nil, fmt.Errorf("rpcnet: register: %w", err)
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

func (t *Transport) ID() transport.NodeID { return t.id }

// Addr is the address this transport is listening on.
func (t *Transport) Addr() string { return t.ln.Addr().String() }

// SetPeer records or changes the address of another node.
func (t *Transport) SetPeer(id transport.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.peers[id] == addr {
		return
	}
	t.peers[id] = addr
	if c := t.clients[id]; c != nil {
		c.Close()
		delete(t.clients, id)
	}
}

func (t *Transport) Serve(h transport.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = h
}

func (t *Transport) currentHandler() transport.Handler {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.handler
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // listener closed
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			return
		}
		t.conns[conn] = struct{}{}
		t.wg.Add(1)
		t.mu.Unlock()

		go func() {
			defer t.wg.Done()
			t.srv.ServeConn(conn) // returns when the connection closes
			t.mu.Lock()
			delete(t.conns, conn)
			t.mu.Unlock()
		}()
	}
}

func (t *Transport) Call(ctx context.Context, to transport.NodeID, method string, payload []byte) ([]byte, error) {
	env := &Envelope{From: t.id, Method: method, Payload: payload}
	for attempt := 0; ; attempt++ {
		c, err := t.client(ctx, to)
		if err != nil {
			return nil, err
		}
		var reply Reply
		call := c.Go(deliverMethod, env, &reply, make(chan *rpc.Call, 1))

		select {
		case <-call.Done:
			if call.Error == nil {
				if reply.Failed {
					return nil, &transport.RemoteError{Msg: reply.Err}
				}
				return reply.Payload, nil
			}
			// The connection is unusable; drop it so the next call redials.
			t.forget(to, c)
			// ErrShutdown means the cached connection was already dead,
			// typically because the peer restarted since it was last used.
			// The peer may well be back, so reconnect once before reporting
			// it as unreachable.
			if attempt == 0 && errors.Is(call.Error, rpc.ErrShutdown) {
				continue
			}
			return nil, fmt.Errorf("rpcnet: call %s: %w", to, call.Error)
		case <-ctx.Done():
			// The request may still be delivered and executed remotely.
			return nil, ctx.Err()
		case <-t.ctx.Done():
			return nil, transport.ErrClosed
		}
	}
}

// client returns the cached connection to a peer, dialling if needed. The
// dial happens outside the lock so one unreachable peer cannot stall calls
// to the others.
func (t *Transport) client(ctx context.Context, to transport.NodeID) (*rpc.Client, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, transport.ErrClosed
	}
	if c := t.clients[to]; c != nil {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.peers[to]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", transport.ErrUnknownPeer, to)
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rpcnet: dial %s: %w", to, err)
	}
	c := rpc.NewClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		c.Close()
		return nil, transport.ErrClosed
	}
	if existing := t.clients[to]; existing != nil {
		// Another goroutine dialled first; keep theirs.
		c.Close()
		return existing, nil
	}
	t.clients[to] = c
	return c, nil
}

func (t *Transport) forget(to transport.NodeID, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	c.Close()
}

// Close stops accepting, drops every connection and waits for running
// handlers, which must honour their ctx.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	err := t.ln.Close()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
	for conn := range t.conns {
		conn.Close()
	}
	t.mu.Unlock()

	t.wg.Wait()
	return err
}

// Package cluster runs a whole key-value cluster inside one process, for
// tests and benchmarks. Nodes talk either over the simulated network, where
// faults can be injected, or over real TCP sockets on localhost.
package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/kv"
	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/storage"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/rpcnet"
	"github.com/silviacioroiu/raftkv/transport/simnet"
)

type Options struct {
	Size int

	// Net configures the simulated network. Ignored when TCP is set.
	Net simnet.Config
	// TCP makes nodes and clients use real sockets on 127.0.0.1.
	TCP bool

	// DataDir, if set, gives every node a file-backed store in its own
	// subdirectory. Otherwise state is kept in memory, which still survives
	// Crash followed by Start.
	DataDir string
	// NoFsync skips forcing writes to disk. Only for measuring what fsync costs.
	NoFsync bool

	Raft   func(*raft.Config) // adjusts each node's Raft settings
	KV     func(*kv.Options)  // adjusts each node's server settings
	Logger *slog.Logger
}

type Cluster struct {
	IDs []transport.NodeID
	// Net is the simulated network, or nil when running over TCP.
	Net *simnet.Network

	opts Options

	mu         sync.Mutex
	servers    map[transport.NodeID]*kv.Server
	transports map[transport.NodeID]transport.Transport
	stores     map[transport.NodeID]raft.Persister
	addrs      map[transport.NodeID]string // TCP only
	starts     int64
	clients    []transport.Transport
}

// New creates the cluster and starts every node.
func New(opts Options) (*Cluster, error) {
	if opts.Size < 1 {
		return nil, errors.New("cluster: Size must be at least 1")
	}
	c := &Cluster{
		opts:       opts,
		servers:    make(map[transport.NodeID]*kv.Server),
		transports: make(map[transport.NodeID]transport.Transport),
		stores:     make(map[transport.NodeID]raft.Persister),
		addrs:      make(map[transport.NodeID]string),
	}
	if !opts.TCP {
		c.Net = simnet.New(opts.Net)
	}
	for i := 1; i <= opts.Size; i++ {
		id := transport.NodeID(fmt.Sprintf("n%d", i))
		c.IDs = append(c.IDs, id)

		if opts.DataDir != "" {
			store, err := storage.OpenFile(filepath.Join(opts.DataDir, string(id)), !opts.NoFsync)
			if err != nil {
				return nil, err
			}
			c.stores[id] = store
		} else {
			c.stores[id] = storage.NewMem()
		}

		if opts.TCP {
			// Every node must know every address before any of them starts,
			// so pick the ports up front.
			addr, err := freeAddr()
			if err != nil {
				return nil, err
			}
			c.addrs[id] = addr
		}
	}
	for _, id := range c.IDs {
		if err := c.Start(id); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

// Start boots a node from whatever its store holds. Use it after Crash to
// simulate a restart.
func (c *Cluster) Start(id transport.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[id] != nil {
		return fmt.Errorf("cluster: %s is already running", id)
	}
	store, ok := c.stores[id]
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}

	var tr transport.Transport
	if c.opts.TCP {
		t, err := rpcnet.Listen(id, c.addrs[id])
		if err != nil {
			return err
		}
		for peer, addr := range c.addrs {
			if peer != id {
				t.SetPeer(peer, addr)
			}
		}
		tr = t
	} else {
		tr = c.Net.Join(id)
	}

	c.starts++
	ko := kv.Options{
		Raft: raft.Config{
			ID:    id,
			Peers: c.IDs,
			// Distinct per incarnation, or nodes would pick identical
			// election timeouts and keep splitting the vote.
			Seed: c.starts*7919 + 1,
		},
		Transport: tr,
		Store:     store,
		Logger:    c.opts.Logger,
	}
	if c.opts.Raft != nil {
		c.opts.Raft(&ko.Raft)
	}
	if c.opts.KV != nil {
		c.opts.KV(&ko)
	}
	srv, err := kv.Start(ko)
	if err != nil {
		tr.Close()
		return err
	}
	c.servers[id] = srv
	c.transports[id] = tr
	return nil
}

// Crash stops a node abruptly. Only what it persisted survives.
func (c *Cluster) Crash(id transport.NodeID) {
	c.mu.Lock()
	srv, tr := c.servers[id], c.transports[id]
	delete(c.servers, id)
	delete(c.transports, id)
	c.mu.Unlock()
	if srv == nil {
		return
	}
	// Cut the node off first, so that nothing it does while shutting down
	// reaches the others.
	tr.Close()
	srv.Close()
}

// Server returns a running node, or nil if it is down.
func (c *Cluster) Server(id transport.NodeID) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

// Live lists the nodes currently running.
func (c *Cluster) Live() []transport.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []transport.NodeID
	for _, id := range c.IDs {
		if c.servers[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the running node that leads the highest term, if any.
// During a partition a deposed leader can still believe it leads an older
// term; the highest term identifies the one that can actually commit.
func (c *Cluster) Leader() (transport.NodeID, bool) {
	var leader transport.NodeID
	var top uint64
	found := false
	for _, id := range c.Live() {
		srv := c.Server(id)
		if srv == nil {
			continue
		}
		st := srv.Raft().Status()
		if st.Role == raft.Leader && (!found || st.Term > top) {
			leader, top, found = id, st.Term, true
		}
	}
	return leader, found
}

func (c *Cluster) WaitLeader(timeout time.Duration) (transport.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for {
		if id, ok := c.Leader(); ok {
			return id, nil
		}
		if time.Now().After(deadline) {
			return "", errors.New("cluster: no leader elected")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// NewClient returns a client that knows every node.
func (c *Cluster) NewClient() (*kv.Client, error) {
	return c.NewClientFor(c.IDs...)
}

// NewClientFor returns a client that only ever contacts the given nodes.
func (c *Cluster) NewClientFor(servers ...transport.NodeID) (*kv.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := transport.NodeID(fmt.Sprintf("client-%d", len(c.clients)+1))

	var tr transport.Transport
	if c.opts.TCP {
		t, err := rpcnet.Listen(id, "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		for peer, addr := range c.addrs {
			t.SetPeer(peer, addr)
		}
		tr = t
	} else {
		tr = c.Net.Join(id)
	}
	c.clients = append(c.clients, tr)
	return kv.NewClient(tr, servers), nil
}

// Close stops every node and client.
func (c *Cluster) Close() {
	for _, id := range c.IDs {
		c.Crash(id)
	}
	c.mu.Lock()
	clients := c.clients
	c.clients = nil
	c.mu.Unlock()
	for _, tr := range clients {
		tr.Close()
	}
	if c.Net != nil {
		c.Net.Close()
	}
	// Release file handles, or the data directory cannot be removed on Windows.
	for _, store := range c.stores {
		if closer, ok := store.(io.Closer); ok {
			closer.Close()
		}
	}
}

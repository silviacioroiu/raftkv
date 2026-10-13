// Package simnet is an in-memory network for tests. It can drop, delay,
// reorder and duplicate messages, cut individual links, and crash nodes.
//
// Every fault decision comes from one seeded PRNG, so a seed reproduces the
// same fault pattern. It does not reproduce goroutine scheduling: message
// delays use real time and handlers run on real goroutines.
package simnet

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
)

// Config controls fault injection. The zero value is a reliable, instant
// network. Rates are probabilities in [0, 1] applied to each message; a
// request and its reply are separate messages with independent fates.
type Config struct {
	Seed int64

	// DropRate is the chance a message is lost.
	DropRate float64

	// Each message is delayed by a uniform random time in [MinDelay, MaxDelay].
	MinDelay time.Duration
	MaxDelay time.Duration

	// ReorderRate is the chance a message gets an extra delay of up to
	// ReorderDelay, so that it arrives well after messages sent later.
	ReorderRate  float64
	ReorderDelay time.Duration

	// DuplicateRate is the chance a request is delivered twice. The caller
	// still sees a single reply.
	DuplicateRate float64
}

// Stats counts messages, where a request and a reply are one message each.
type Stats struct {
	Sent       uint64
	Delivered  uint64
	Dropped    uint64 // lost to DropRate
	Blocked    uint64 // lost to a cut link or a dead destination
	Duplicated uint64
}

type link struct {
	from, to transport.NodeID
}

type Network struct {
	mu      sync.Mutex
	cfg     Config
	rng     *rand.Rand
	nodes   map[transport.NodeID]*Endpoint
	blocked map[link]bool
	stats   Stats
	closed  bool

	done chan struct{}
	wg   sync.WaitGroup // in-flight deliveries
}

func New(cfg Config) *Network {
	return &Network{
		cfg:     cfg,
		rng:     rand.New(rand.NewSource(cfg.Seed)),
		nodes:   make(map[transport.NodeID]*Endpoint),
		blocked: make(map[link]bool),
		done:    make(chan struct{}),
	}
}

// Join attaches a node and returns its transport. Joining an ID that is
// already attached replaces it, which models a process restart: the old
// endpoint is closed and messages still in flight reach the new one.
func (n *Network) Join(id transport.NodeID) *Endpoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		panic("simnet: Join on closed network")
	}
	if old := n.nodes[id]; old != nil {
		old.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	ep := &Endpoint{id: id, net: n, ctx: ctx, cancel: cancel}
	n.nodes[id] = ep
	return ep
}

// Remove detaches a node, modelling a crash. Messages addressed to it are
// lost, so callers see a timeout rather than an immediate error.
func (n *Network) Remove(id transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ep := n.nodes[id]; ep != nil {
		ep.cancel()
		delete(n.nodes, id)
	}
}

// Update changes fault settings while the network is running. The seed
// cannot be changed.
func (n *Network) Update(fn func(c *Config)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	seed := n.cfg.Seed
	fn(&n.cfg)
	n.cfg.Seed = seed
}

// Block cuts the link in one direction only. An RPC needs both directions:
// with only to->from cut, the handler on `to` still runs and its reply is lost.
func (n *Network) Block(from, to transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[link{from, to}] = true
}

func (n *Network) Unblock(from, to transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, link{from, to})
}

// Partition replaces all cut links with a split into the given groups: nodes
// in the same group can talk, nodes in different groups cannot. Nodes not
// listed in any group are unaffected.
func (n *Network) Partition(groups ...[]transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[link]bool)
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.blocked[link{a, b}] = true
				}
			}
		}
	}
}

// Heal restores every cut link.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[link]bool)
}

func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// Close detaches every node and waits for in-flight deliveries to finish.
// It blocks until running handlers return, so handlers must honour their ctx.
func (n *Network) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	close(n.done)
	for id, ep := range n.nodes {
		ep.cancel()
		delete(n.nodes, id)
	}
	n.mu.Unlock()
	n.wg.Wait()
}

// fate is what happens to one message.
type fate struct {
	drop  bool
	dup   bool
	delay time.Duration
}

// plan decides the fate of one message. n.mu must be held. It always consumes
// the same number of random values, whatever the config, so that changing a
// rate mid-test does not shift every later decision.
func (n *Network) plan() fate {
	c := n.cfg
	pDrop, pDelay, pReorder, pExtra, pDup :=
		n.rng.Float64(), n.rng.Float64(), n.rng.Float64(), n.rng.Float64(), n.rng.Float64()

	f := fate{
		drop:  pDrop < c.DropRate,
		dup:   pDup < c.DuplicateRate,
		delay: c.MinDelay,
	}
	if c.MaxDelay > c.MinDelay {
		f.delay += time.Duration(pDelay * float64(c.MaxDelay-c.MinDelay))
	}
	if pReorder < c.ReorderRate {
		f.delay += time.Duration(pExtra * float64(c.ReorderDelay))
	}
	return f
}

// sleep waits for d and reports false if the network closed first.
func (n *Network) sleep(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-n.done:
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-n.done:
		return false
	}
}

type result struct {
	payload []byte
	err     error
}

// deliver carries one copy of a request to its destination, runs the handler,
// and carries the reply back. It is deliberately independent of the caller's
// ctx: once a request is on the wire, the caller giving up does not recall it.
func (n *Network) deliver(src *Endpoint, to transport.NodeID, method string, payload []byte, delay time.Duration, res chan<- result) {
	defer n.wg.Done()

	if !n.sleep(delay) {
		return
	}

	n.mu.Lock()
	dst := n.nodes[to]
	if dst == nil || n.blocked[link{src.id, to}] {
		n.stats.Blocked++
		n.mu.Unlock()
		return
	}
	n.stats.Delivered++
	n.mu.Unlock()

	var r result
	if h := dst.currentHandler(); h == nil {
		r.err = &transport.RemoteError{Msg: "no handler installed"}
	} else if out, err := h(dst.ctx, src.id, method, bytes.Clone(payload)); err != nil {
		r.err = &transport.RemoteError{Msg: err.Error()}
	} else {
		r.payload = bytes.Clone(out)
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	f := n.plan()
	n.stats.Sent++
	if f.drop {
		n.stats.Dropped++
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	if !n.sleep(f.delay) {
		return
	}

	n.mu.Lock()
	// The reply was already sent, so the responder crashing since then does
	// not matter; only the return link and the caller being alive do.
	if n.nodes[src.id] != src || n.blocked[link{to, src.id}] {
		n.stats.Blocked++
		n.mu.Unlock()
		return
	}
	n.stats.Delivered++
	n.mu.Unlock()

	// Non-blocking: a duplicated request produces two replies and the caller
	// keeps only the first.
	select {
	case res <- r:
	default:
	}
}

// Endpoint is one node's attachment to a Network. It implements
// transport.Transport.
type Endpoint struct {
	id     transport.NodeID
	net    *Network
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	handler transport.Handler
}

var _ transport.Transport = (*Endpoint)(nil)

func (e *Endpoint) ID() transport.NodeID { return e.id }

func (e *Endpoint) Serve(h transport.Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handler = h
}

func (e *Endpoint) currentHandler() transport.Handler {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.handler
}

func (e *Endpoint) Call(ctx context.Context, to transport.NodeID, method string, payload []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("simnet: nil context")
	}
	n := e.net
	res := make(chan result, 1)

	n.mu.Lock()
	if n.closed || n.nodes[e.id] != e {
		n.mu.Unlock()
		return nil, transport.ErrClosed
	}
	f := n.plan()
	n.stats.Sent++
	var delays []time.Duration
	if f.drop {
		n.stats.Dropped++
	} else {
		delays = append(delays, f.delay)
		if f.dup {
			n.stats.Duplicated++
			delays = append(delays, n.plan().delay)
		}
	}
	// Add under the lock so it cannot race with Close's Wait.
	n.wg.Add(len(delays))
	n.mu.Unlock()

	for _, d := range delays {
		go n.deliver(e, to, method, payload, d, res)
	}

	// A dropped request looks exactly like a slow one: wait for the deadline.
	select {
	case r := <-res:
		return r.payload, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, transport.ErrClosed
	}
}

// Close detaches this endpoint from the network.
func (e *Endpoint) Close() error {
	n := e.net
	n.mu.Lock()
	defer n.mu.Unlock()
	e.cancel()
	if n.nodes[e.id] == e {
		delete(n.nodes, e.id)
	}
	return nil
}

package raft_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/storage"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/simnet"
)

// recorder is a state machine whose state is the list of commands applied.
type recorder struct {
	mu      sync.Mutex
	applied []string
	seen    map[string]bool
}

func (r *recorder) Apply(e raft.Entry) {
	if e.NoOp {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, string(e.Data))
	r.seen[string(e.Data)] = true
}

func (r *recorder) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r.applied); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (r *recorder) Restore(data []byte) {
	var applied []string
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&applied); err != nil {
			panic(err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = applied
	r.seen = make(map[string]bool, len(applied))
	for _, cmd := range applied {
		r.seen[cmd] = true
	}
}

func (r *recorder) has(cmd string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[cmd]
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.applied...)
}

// cluster runs Raft nodes over a simulated network. Each node's Mem store
// outlives the node, so crash followed by restart behaves like a reboot.
type cluster struct {
	t     *testing.T
	net   *simnet.Network
	ids   []transport.NodeID
	tweak func(*raft.Config)

	mu       sync.Mutex
	stores   map[transport.NodeID]*storage.Mem
	nodes    map[transport.NodeID]*raft.Node
	fsms     map[transport.NodeID]*recorder
	restarts int64
}

func newCluster(t *testing.T, size int, netCfg simnet.Config, tweak func(*raft.Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:      t,
		net:    simnet.New(netCfg),
		tweak:  tweak,
		stores: make(map[transport.NodeID]*storage.Mem),
		nodes:  make(map[transport.NodeID]*raft.Node),
		fsms:   make(map[transport.NodeID]*recorder),
	}
	for i := 1; i <= size; i++ {
		id := transport.NodeID(fmt.Sprintf("n%d", i))
		c.ids = append(c.ids, id)
		c.stores[id] = storage.NewMem()
	}
	t.Cleanup(c.shutdown)
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func (c *cluster) start(id transport.NodeID) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nodes[id] != nil {
		c.t.Fatalf("node %s is already running", id)
	}
	c.restarts++
	cfg := raft.Config{
		ID:    id,
		Peers: c.ids,
		// Every incarnation needs its own seed: nodes sharing one would pick
		// identical election timeouts and split the vote forever.
		Seed: c.restarts*7919 + 1,
	}
	if c.tweak != nil {
		c.tweak(&cfg)
	}

	ep := c.net.Join(id)
	mux := transport.NewMux()
	fsm := &recorder{seen: make(map[string]bool)}
	node, err := raft.New(cfg, raft.Deps{Transport: ep, Mux: mux, Store: c.stores[id], FSM: fsm})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	ep.Serve(mux.Handle)
	node.Start()
	c.nodes[id] = node
	c.fsms[id] = fsm
}

// crash stops a node abruptly, keeping only what it persisted.
func (c *cluster) crash(id transport.NodeID) {
	c.mu.Lock()
	node := c.nodes[id]
	delete(c.nodes, id)
	delete(c.fsms, id)
	c.mu.Unlock()
	if node == nil {
		return
	}
	c.net.Remove(id)
	node.Stop()
}

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		c.crash(id)
	}
	c.net.Close()
}

func (c *cluster) node(id transport.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) live() []transport.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []transport.NodeID
	for _, id := range c.ids {
		if c.nodes[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) recorders() map[transport.NodeID]*recorder {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[transport.NodeID]*recorder, len(c.fsms))
	for id, r := range c.fsms {
		out[id] = r
	}
	return out
}

func (c *cluster) status(id transport.NodeID) raft.Status {
	c.t.Helper()
	n := c.node(id)
	if n == nil {
		c.t.Fatalf("node %s is not running", id)
	}
	return n.Status()
}

func (c *cluster) others(exclude ...transport.NodeID) []transport.NodeID {
	var out []transport.NodeID
	for _, id := range c.ids {
		skip := false
		for _, x := range exclude {
			skip = skip || x == id
		}
		if !skip {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitLeader returns the leader with the highest term among the given nodes
// (all live nodes if none are given). It fails the test if two nodes ever
// claim leadership of the same term, which is Raft's Election Safety property.
func (c *cluster) waitLeader(among ...transport.NodeID) transport.NodeID {
	c.t.Helper()
	var leader transport.NodeID
	ok := c.waitFor(10*time.Second, func() bool {
		ids := among
		if len(ids) == 0 {
			ids = c.live()
		}
		byTerm := make(map[uint64]transport.NodeID)
		var top uint64
		found := false
		for _, id := range ids {
			n := c.node(id)
			if n == nil {
				continue
			}
			st := n.Status()
			if st.Role != raft.Leader {
				continue
			}
			if other, dup := byTerm[st.Term]; dup {
				c.t.Fatalf("election safety violated: %s and %s both lead term %d", other, id, st.Term)
			}
			byTerm[st.Term] = id
			if !found || st.Term > top {
				top, leader, found = st.Term, id, true
			}
		}
		return found
	})
	if !ok {
		c.t.Fatal("no leader was elected")
	}
	return leader
}

// applied reports how many live nodes have applied cmd. Nodes only apply
// committed entries, so one is enough to know the command is committed.
func (c *cluster) applied(cmd string) int {
	count := 0
	for _, r := range c.recorders() {
		if r.has(cmd) {
			count++
		}
	}
	return count
}

// commit proposes cmd to whichever node accepts it and waits until it is
// committed, retrying across leader changes. A retry can put the command in
// the log twice, which these tests tolerate. Safe to call from any goroutine.
func (c *cluster) commit(cmd string, among ...transport.NodeID) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ids := among
		if len(ids) == 0 {
			ids = c.live()
		}
		for _, id := range ids {
			n := c.node(id)
			if n == nil {
				continue
			}
			if _, _, ok := n.Propose([]byte(cmd)); !ok {
				continue
			}
			if c.waitFor(2*time.Second, func() bool { return c.applied(cmd) > 0 }) {
				return nil
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("command %q was not committed", cmd)
}

func (c *cluster) mustCommit(cmd string, among ...transport.NodeID) {
	c.t.Helper()
	if err := c.commit(cmd, among...); err != nil {
		c.t.Fatal(err)
	}
}

// waitApplied waits until every live node has applied all of cmds.
func (c *cluster) waitApplied(cmds []string) {
	c.t.Helper()
	var missing string
	ok := c.waitFor(20*time.Second, func() bool {
		for id, r := range c.recorders() {
			for _, cmd := range cmds {
				if !r.has(cmd) {
					missing = fmt.Sprintf("%s has not applied %q", id, cmd)
					return false
				}
			}
		}
		return true
	})
	if !ok {
		c.t.Fatalf("nodes did not converge: %s", missing)
	}
}

// checkConsistent verifies State Machine Safety: any two nodes applied the
// same commands in the same order, as far as both have got.
func (c *cluster) checkConsistent() {
	c.t.Helper()
	lists := make(map[transport.NodeID][]string)
	for id, r := range c.recorders() {
		lists[id] = r.list()
	}
	for a, la := range lists {
		for b, lb := range lists {
			if a >= b {
				continue
			}
			for i := 0; i < len(la) && i < len(lb); i++ {
				if la[i] != lb[i] {
					c.t.Fatalf("state machine safety violated: position %d is %q on %s but %q on %s",
						i, la[i], a, lb[i], b)
				}
			}
		}
	}
}

func commands(prefix string, count int) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

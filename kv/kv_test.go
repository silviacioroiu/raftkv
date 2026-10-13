package kv_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/internal/cluster"
	"github.com/silviacioroiu/raftkv/kv"
	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/simnet"
)

func newCluster(t *testing.T, opts cluster.Options) *cluster.Cluster {
	t.Helper()
	c, err := cluster.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	return c
}

func newClient(t *testing.T, c *cluster.Cluster, servers ...transport.NodeID) *kv.Client {
	t.Helper()
	var cl *kv.Client
	var err error
	if len(servers) == 0 {
		cl, err = c.NewClient()
	} else {
		cl, err = c.NewClientFor(servers...)
	}
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func ctxFor(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func mustPut(t *testing.T, cl *kv.Client, key, value string) {
	t.Helper()
	ctx, cancel := ctxFor(20 * time.Second)
	defer cancel()
	if err := cl.Put(ctx, key, value); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustAppend(t *testing.T, cl *kv.Client, key, value string) {
	t.Helper()
	ctx, cancel := ctxFor(20 * time.Second)
	defer cancel()
	if err := cl.Append(ctx, key, value); err != nil {
		t.Fatalf("Append(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, cl *kv.Client, key string) string {
	t.Helper()
	ctx, cancel := ctxFor(20 * time.Second)
	defer cancel()
	v, err := cl.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v
}

func expect(t *testing.T, cl *kv.Client, key, want string) {
	t.Helper()
	if got := mustGet(t, cl, key); got != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func TestBasicOperations(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 3})
	cl := newClient(t, c)

	expect(t, cl, "missing", "")
	mustPut(t, cl, "k", "a")
	expect(t, cl, "k", "a")
	mustAppend(t, cl, "k", "b")
	expect(t, cl, "k", "ab")
	mustPut(t, cl, "k", "z")
	expect(t, cl, "k", "z")
	mustAppend(t, cl, "new", "x")
	expect(t, cl, "new", "x")
}

// Writes from one client must be visible to every other client right away.
func TestClientsSeeEachOthersWrites(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 3})
	a, b := newClient(t, c), newClient(t, c)

	for i := 0; i < 20; i++ {
		v := fmt.Sprint(i)
		mustPut(t, a, "k", v)
		expect(t, b, "k", v)
	}
}

func TestConcurrentAppends(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 3})
	const clients, each = 8, 25

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		cl := newClient(t, c)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				ctx, cancel := ctxFor(20 * time.Second)
				err := cl.Append(ctx, "log", fmt.Sprintf("<%d.%d>", i, j))
				cancel()
				if err != nil {
					t.Errorf("client %d append %d: %v", i, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	checkAppends(t, mustGet(t, newClient(t, c), "log"), clients, each)
}

// checkAppends verifies that every token <client.seq> appears exactly once
// and that each client's tokens are in the order it sent them.
func checkAppends(t *testing.T, value string, clients, each int) {
	t.Helper()
	for i := 0; i < clients; i++ {
		last := -1
		for j := 0; j < each; j++ {
			token := fmt.Sprintf("<%d.%d>", i, j)
			if n := strings.Count(value, token); n != 1 {
				t.Fatalf("token %s appears %d times, want exactly once", token, n)
			}
			pos := strings.Index(value, token)
			if pos < last {
				t.Fatalf("token %s is out of order", token)
			}
			last = pos
		}
	}
}

// On a network that loses and duplicates messages, clients retry constantly.
// Each append must still take effect exactly once.
func TestExactlyOnceOnLossyNetwork(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 3, Net: simnet.Config{
		Seed:          7,
		DropRate:      0.15,
		MaxDelay:      10 * time.Millisecond,
		DuplicateRate: 0.1,
	}})
	const clients, each = 4, 20

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		cl := newClient(t, c)
		cl.AttemptTimeout = 150 * time.Millisecond
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				ctx, cancel := ctxFor(30 * time.Second)
				err := cl.Append(ctx, "log", fmt.Sprintf("<%d.%d>", i, j))
				cancel()
				if err != nil {
					t.Errorf("client %d append %d: %v", i, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if s := c.Net.Stats(); s.Dropped == 0 || s.Duplicated == 0 {
		t.Fatalf("the network injected no faults: %+v", s)
	}
	checkAppends(t, mustGet(t, newClient(t, c), "log"), clients, each)
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 3})
	cl := newClient(t, c)
	mustPut(t, cl, "k", "before")

	leader, _ := c.Leader()
	c.Crash(leader)
	expect(t, cl, "k", "before")
	mustPut(t, cl, "k", "during")

	if err := c.Start(leader); err != nil {
		t.Fatal(err)
	}
	expect(t, cl, "k", "during")
	mustAppend(t, cl, "k", "+after")
	expect(t, cl, "k", "during+after")
}

// A leader cut off from the majority must not answer reads, because the
// majority may have accepted newer writes it knows nothing about.
func TestIsolatedLeaderServesNoStaleReads(t *testing.T) {
	c := newCluster(t, cluster.Options{Size: 5})
	all := newClient(t, c)
	mustPut(t, all, "k", "old")

	oldLeader, _ := c.Leader()
	var majority []transport.NodeID
	for _, id := range c.IDs {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}
	c.Net.Partition([]transport.NodeID{oldLeader}, majority)

	viaMajority := newClient(t, c, majority...)
	mustPut(t, viaMajority, "k", "new")

	// This client can only reach the deposed leader, which still believes
	// it is in charge.
	stale := newClient(t, c, oldLeader)
	ctx, cancel := ctxFor(time.Second)
	v, err := stale.Get(ctx, "k")
	cancel()
	if err == nil {
		t.Fatalf("isolated leader answered a read with %q", v)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error from isolated leader: %v", err)
	}

	// Once the partition heals, the old leader steps down and adopts the
	// majority's log, so the whole cluster agrees on the new value.
	c.Net.Heal()
	expect(t, all, "k", "new")
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := c.Server(oldLeader).Raft().Status()
		if leader, ok := c.Leader(); ok && st.LastApplied >= c.Server(leader).Raft().Status().CommitIndex && st.Term == c.Server(leader).Raft().Status().Term {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old leader did not rejoin: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSnapshotsAndFullRestart(t *testing.T) {
	c := newCluster(t, cluster.Options{
		Size: 3,
		Raft: func(cfg *raft.Config) { cfg.SnapshotThreshold = 20 },
	})
	cl := newClient(t, c)

	var want strings.Builder
	for i := 0; i < 150; i++ {
		token := fmt.Sprintf("[%d]", i)
		mustAppend(t, cl, "k", token)
		want.WriteString(token)
	}
	for _, id := range c.IDs {
		if idx := c.Server(id).Raft().Status().SnapshotIndex; idx == 0 {
			t.Fatalf("%s never compacted its log", id)
		}
	}

	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		if err := c.Start(id); err != nil {
			t.Fatal(err)
		}
	}
	expect(t, cl, "k", want.String())
	mustAppend(t, cl, "k", "!")
	expect(t, cl, "k", want.String()+"!")
}

// The same cluster over real TCP sockets with state on disk.
func TestTCPWithDiskStorage(t *testing.T) {
	c := newCluster(t, cluster.Options{
		Size:    3,
		TCP:     true,
		DataDir: t.TempDir(),
		Raft:    func(cfg *raft.Config) { cfg.SnapshotThreshold = 50 },
	})
	cl := newClient(t, c)
	cl.AttemptTimeout = 2 * time.Second

	var want strings.Builder
	for i := 0; i < 80; i++ {
		token := fmt.Sprintf("[%d]", i)
		mustAppend(t, cl, "k", token)
		want.WriteString(token)
	}
	expect(t, cl, "k", want.String())

	// Stop everything; the only copy of the data is now on disk.
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		if err := c.Start(id); err != nil {
			t.Fatal(err)
		}
	}
	expect(t, cl, "k", want.String())
}

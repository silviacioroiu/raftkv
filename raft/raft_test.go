package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/simnet"
)

// --- Milestone 2: leader election ---

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	leader := c.waitLeader()
	term := c.status(leader).Term

	// With no failures the leader must keep its position.
	time.Sleep(time.Second)
	if got := c.waitLeader(); got != leader {
		t.Fatalf("leader changed from %s to %s on a healthy network", leader, got)
	}
	for _, id := range c.ids {
		st := c.status(id)
		if st.Term != term {
			t.Errorf("%s is in term %d, leader is in term %d", id, st.Term, term)
		}
		if st.LeaderID != leader {
			t.Errorf("%s thinks the leader is %q, want %s", id, st.LeaderID, leader)
		}
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	first := c.waitLeader()
	firstTerm := c.status(first).Term

	c.crash(first)
	second := c.waitLeader()
	if second == first {
		t.Fatal("crashed node is still reported as leader")
	}
	if got := c.status(second).Term; got <= firstTerm {
		t.Fatalf("new leader's term %d is not above the old term %d", got, firstTerm)
	}

	// The old leader comes back and must fall in line as a follower.
	c.start(first)
	ok := c.waitFor(5*time.Second, func() bool {
		leader := c.waitLeader()
		st := c.status(first)
		return leader != first || st.Term > firstTerm
	})
	if !ok {
		t.Fatal("restarted node still leads its old term")
	}

	// Without a majority nobody may win an election.
	leader := c.waitLeader()
	survivor := c.others(leader)[0]
	c.crash(leader)
	c.crash(c.others(leader, survivor)[0])
	time.Sleep(time.Second)
	if st := c.status(survivor); st.Role == raft.Leader {
		t.Fatalf("%s became leader of term %d with 1 of 3 nodes alive", survivor, st.Term)
	}

	// A majority again: a leader must emerge.
	c.start(leader)
	c.waitLeader()
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, simnet.Config{}, nil)
	c.waitLeader()
	cmds := commands("solo", 5)
	for _, cmd := range cmds {
		c.mustCommit(cmd)
	}
	c.waitApplied(cmds)
}

// --- Milestone 3: log replication ---

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	c.waitLeader()

	cmds := commands("cmd", 20)
	for _, cmd := range cmds {
		c.mustCommit(cmd)
	}
	c.waitApplied(cmds)
	c.checkConsistent()

	for id, r := range c.recorders() {
		got := r.list()
		if len(got) != len(cmds) {
			t.Fatalf("%s applied %d commands, want %d: %v", id, len(got), len(cmds), got)
		}
		for i := range cmds {
			if got[i] != cmds[i] {
				t.Fatalf("%s applied %q at position %d, want %q", id, got[i], i, cmds[i])
			}
		}
	}
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	leader := c.waitLeader()
	lagging := c.others(leader)[0]
	rest := c.others(lagging)

	c.net.Partition([]transport.NodeID{lagging}, rest)
	cmds := commands("while-away", 10)
	for _, cmd := range cmds {
		c.mustCommit(cmd, rest...)
	}
	if n := len(c.recorders()[lagging].list()); n != 0 {
		t.Fatalf("partitioned follower applied %d commands", n)
	}

	c.net.Heal()
	c.mustCommit("after-heal")
	c.waitApplied(append(cmds, "after-heal"))
	c.checkConsistent()
}

// A leader cut off with a minority keeps accepting proposals but can never
// commit them, and they are discarded once it rejoins (§5.3).
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, simnet.Config{}, nil)
	oldLeader := c.waitLeader()
	c.mustCommit("before")

	followers := c.others(oldLeader)
	minority := []transport.NodeID{oldLeader, followers[0]}
	majority := followers[1:]
	c.net.Partition(minority, majority)

	if _, _, ok := c.node(oldLeader).Propose([]byte("lost")); !ok {
		t.Fatal("isolated leader refused a proposal before it could know it was deposed")
	}

	newLeader := c.waitLeader(majority...)
	if newLeader == oldLeader {
		t.Fatal("minority leader was reported as the majority's leader")
	}
	kept := commands("kept", 5)
	for _, cmd := range kept {
		c.mustCommit(cmd, majority...)
	}
	if n := c.applied("lost"); n != 0 {
		t.Fatalf("%d nodes applied a command that only a minority stored", n)
	}

	c.net.Heal()
	c.mustCommit("after-heal")
	c.waitApplied(append(kept, "before", "after-heal"))
	c.checkConsistent()
	if n := c.applied("lost"); n != 0 {
		t.Fatalf("%d nodes applied the minority leader's entry after healing", n)
	}
}

func TestUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 5, simnet.Config{
		Seed:          11,
		DropRate:      0.1,
		MaxDelay:      20 * time.Millisecond,
		ReorderRate:   0.1,
		ReorderDelay:  60 * time.Millisecond,
		DuplicateRate: 0.05,
	}, nil)
	c.waitLeader()

	const writers, each = 3, 10
	var all []string
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		cmds := commands(fmt.Sprintf("w%d", w), each)
		all = append(all, cmds...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, cmd := range cmds {
				if err := c.commit(cmd); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	c.net.Update(func(cfg *simnet.Config) { *cfg = simnet.Config{} })
	c.waitApplied(all)
	c.checkConsistent()
}

// --- Milestone 4: persistence ---

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	leader := c.waitLeader()
	termBefore := c.status(leader).Term

	first := commands("first", 10)
	for _, cmd := range first {
		c.mustCommit(cmd)
	}
	c.waitApplied(first)

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}

	leader = c.waitLeader()
	if got := c.status(leader).Term; got <= termBefore {
		t.Fatalf("term went from %d to %d across a restart; currentTerm was not persisted", termBefore, got)
	}
	// Committed entries must survive and be re-applied to the fresh state machines.
	c.waitApplied(first)

	second := commands("second", 5)
	for _, cmd := range second {
		c.mustCommit(cmd)
	}
	c.waitApplied(append(first, second...))
	c.checkConsistent()
}

func TestRollingRestarts(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, nil)
	c.waitLeader()

	var all []string
	for round, id := range c.ids {
		cmds := commands(fmt.Sprintf("round%d", round), 5)
		all = append(all, cmds...)
		c.crash(id)
		for _, cmd := range cmds {
			c.mustCommit(cmd)
		}
		c.start(id)
	}
	c.waitApplied(all)
	c.checkConsistent()
}

// --- Milestone 5: snapshots ---

func withSnapshots(threshold uint64) func(*raft.Config) {
	return func(cfg *raft.Config) { cfg.SnapshotThreshold = threshold }
}

func TestSnapshotBringsFollowerUpToDate(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, withSnapshots(10))
	leader := c.waitLeader()
	lagging := c.others(leader)[0]
	rest := c.others(lagging)

	c.net.Partition([]transport.NodeID{lagging}, rest)
	cmds := commands("snap", 40)
	for _, cmd := range cmds {
		c.mustCommit(cmd, rest...)
	}
	// Both remaining nodes must have compacted past what the follower holds,
	// so that only InstallSnapshot can bring it back.
	ok := c.waitFor(5*time.Second, func() bool {
		for _, id := range rest {
			if c.status(id).SnapshotIndex < 10 {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatal("log was not compacted")
	}

	c.net.Heal()
	c.mustCommit("after-heal")
	c.waitApplied(append(cmds, "after-heal"))
	c.checkConsistent()
	if idx := c.status(lagging).SnapshotIndex; idx == 0 {
		t.Fatal("follower caught up without installing a snapshot")
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, withSnapshots(10))
	c.waitLeader()

	cmds := commands("durable", 35)
	for _, cmd := range cmds {
		c.mustCommit(cmd)
	}
	c.waitApplied(cmds)

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	for _, id := range c.ids {
		if c.status(id).SnapshotIndex == 0 {
			t.Fatalf("%s restarted without its snapshot", id)
		}
	}

	c.waitLeader()
	c.mustCommit("after-restart")
	c.waitApplied(append(cmds, "after-restart"))
	c.checkConsistent()
}

func TestCompactionBoundsLogSize(t *testing.T) {
	c := newCluster(t, 3, simnet.Config{}, withSnapshots(20))
	leader := c.waitLeader()

	for _, cmd := range commands("grow", 100) {
		c.mustCommit(cmd)
	}
	st := c.status(leader)
	if held := st.LastLogIndex - st.SnapshotIndex; held > 40 {
		t.Fatalf("leader still holds %d log entries with a snapshot threshold of 20", held)
	}
}

// --- Everything at once ---

// TestChurn crashes and restarts random nodes on a lossy network while
// clients keep writing, then checks that every acknowledged command survived
// and that all nodes agree.
func TestChurn(t *testing.T) {
	c := newCluster(t, 5, simnet.Config{
		Seed:     23,
		DropRate: 0.05,
		MaxDelay: 10 * time.Millisecond,
	}, withSnapshots(15))
	c.waitLeader()

	stop := make(chan struct{})
	var mu sync.Mutex
	var acked []string
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				cmd := fmt.Sprintf("churn-%d-%d", w, i)
				if err := c.commit(cmd); err == nil {
					mu.Lock()
					acked = append(acked, cmd)
					mu.Unlock()
				}
			}
		}(w)
	}

	rng := rand.New(rand.NewSource(5))
	down := make(map[transport.NodeID]bool)
	for step := 0; step < 12; step++ {
		time.Sleep(time.Duration(200+rng.Intn(300)) * time.Millisecond)
		id := c.ids[rng.Intn(len(c.ids))]
		switch {
		case down[id]:
			c.start(id)
			delete(down, id)
		case len(down) < 2: // never lose the majority
			c.crash(id)
			down[id] = true
		}
		c.checkConsistent()
	}

	for id := range down {
		c.start(id)
	}
	close(stop)
	wg.Wait()

	c.net.Update(func(cfg *simnet.Config) { *cfg = simnet.Config{} })
	c.mustCommit("final")
	mu.Lock()
	want := append(acked, "final")
	mu.Unlock()
	if len(want) < 10 {
		t.Fatalf("only %d commands were committed during the churn", len(want))
	}
	c.waitApplied(want)
	c.checkConsistent()
}

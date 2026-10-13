// Package chaos runs the key-value cluster under injected faults while
// clients issue random operations, records every operation with its start
// and end time, and then asks Porcupine whether that history is linearizable:
// whether there is some order of the operations, consistent with real time,
// that a single correct key-value map could have produced.
//
//	go test ./chaos/ -v
//	go test ./chaos/ -v -chaos.duration=30s -chaos.seed=7
package chaos

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/silviacioroiu/raftkv/internal/cluster"
	"github.com/silviacioroiu/raftkv/internal/hrtime"
	"github.com/silviacioroiu/raftkv/kv"
	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/simnet"
)

var (
	duration = flag.Duration("chaos.duration", 5*time.Second, "how long each scenario injects faults")
	seed     = flag.Int64("chaos.seed", 1, "seed for fault schedules and client workloads")
)

type input struct {
	kind  kv.OpKind
	key   string
	value string
}

type output struct {
	value string // for gets
}

// model is the sequential specification the history is checked against: a
// plain map, split per key because keys are independent.
var model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(input).key
			byKey[key] = append(byKey[key], op)
		}
		parts := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return "" },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		value, op := state.(string), in.(input)
		switch op.kind {
		case kv.OpGet:
			return out.(output).value == value, value
		case kv.OpPut:
			return true, op.value
		default:
			return true, value + op.value
		}
	},
	DescribeOperation: func(in, out interface{}) string {
		op := in.(input)
		switch op.kind {
		case kv.OpGet:
			return fmt.Sprintf("get(%s) -> %q", op.key, out.(output).value)
		case kv.OpPut:
			return fmt.Sprintf("put(%s, %q)", op.key, op.value)
		default:
			return fmt.Sprintf("append(%s, %q)", op.key, op.value)
		}
	},
}

// nemesis injects faults until ctx is cancelled. It may leave nodes crashed
// and links cut; the runner repairs everything afterwards.
type nemesis func(ctx context.Context, c *cluster.Cluster, rng *rand.Rand)

type scenario struct {
	size    int
	net     simnet.Config
	nemesis nemesis
}

const (
	numClients = 7
	// The last numReaders clients only ever read. A client that writes
	// notices a deposed leader as soon as a write stalls and moves on, so
	// without dedicated readers nothing keeps talking to an isolated leader
	// for long enough to catch it serving stale data.
	numReaders = 2
	numKeys    = 3
)

func run(t *testing.T, sc scenario) {
	c, err := cluster.New(cluster.Options{
		Size: sc.size,
		Net:  sc.net,
		// A low threshold so that compaction and InstallSnapshot are
		// exercised constantly.
		Raft: func(cfg *raft.Config) { cfg.SnapshotThreshold = 25 },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	begin := hrtime.Now()
	var mu sync.Mutex
	var history []porcupine.Operation

	// execute runs one operation to completion and records it. Clients retry
	// until they get an answer, so every operation has a definite outcome.
	execute := func(client int, cl *kv.Client, in input) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		var out output
		var err error
		call := (hrtime.Now() - begin).Nanoseconds()
		switch in.kind {
		case kv.OpGet:
			out.value, err = cl.Get(ctx, in.key)
		case kv.OpPut:
			err = cl.Put(ctx, in.key, in.value)
		default:
			err = cl.Append(ctx, in.key, in.value)
		}
		ret := (hrtime.Now() - begin).Nanoseconds()
		if err != nil {
			return fmt.Errorf("client %d %v(%s): %w", client, in.kind, in.key, err)
		}
		mu.Lock()
		history = append(history, porcupine.Operation{
			ClientId: client, Input: in, Call: call, Output: out, Return: ret,
		})
		mu.Unlock()
		return nil
	}

	stopClients := make(chan struct{})
	var clients sync.WaitGroup
	for i := 0; i < numClients; i++ {
		cl, err := c.NewClient()
		if err != nil {
			t.Fatal(err)
		}
		cl.AttemptTimeout = 100 * time.Millisecond
		clients.Add(1)
		go func(i int) {
			defer clients.Done()
			rng := rand.New(rand.NewSource(*seed*1000 + int64(i)))
			for n := 0; ; n++ {
				select {
				case <-stopClients:
					return
				default:
				}
				in := input{key: fmt.Sprintf("k%d", rng.Intn(numKeys))}
				switch p := rng.Intn(100); {
				case p < 50 || i >= numClients-numReaders:
					in.kind = kv.OpGet
				case p < 75:
					in.kind, in.value = kv.OpPut, fmt.Sprintf("%d.%d", i, n)
				default:
					in.kind, in.value = kv.OpAppend, fmt.Sprintf("+%d.%d", i, n)
				}
				if err := execute(i, cl, in); err != nil {
					t.Error(err)
					return
				}
				time.Sleep(time.Duration(1+rng.Intn(8)) * time.Millisecond)
			}
		}(i)
	}

	ctx, stopNemesis := context.WithCancel(context.Background())
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		if sc.nemesis != nil {
			sc.nemesis(ctx, c, rand.New(rand.NewSource(*seed)))
		}
	}()

	time.Sleep(*duration)
	stopNemesis()
	<-nemesisDone

	// Repair everything so that operations still in flight can finish.
	c.Net.Heal()
	c.Net.Update(func(cfg *simnet.Config) { *cfg = simnet.Config{} })
	for _, id := range c.IDs {
		if c.Server(id) == nil {
			if err := c.Start(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stopClients)
	clients.Wait()
	if t.Failed() {
		return
	}

	// One last read of every key pins down the final state.
	reader, err := c.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < numKeys; k++ {
		if err := execute(numClients, reader, input{kind: kv.OpGet, key: fmt.Sprintf("k%d", k)}); err != nil {
			t.Fatal(err)
		}
	}

	stats := c.Net.Stats()
	result, info := porcupine.CheckOperationsVerbose(model, history, 2*time.Minute)
	t.Logf("%d operations; network sent %d messages, dropped %d, blocked %d, duplicated %d; result: %s",
		len(history), stats.Sent, stats.Dropped, stats.Blocked, stats.Duplicated, result)

	switch result {
	case porcupine.Ok:
	case porcupine.Illegal:
		path := fmt.Sprintf("violation-%s.html", t.Name())
		if err := porcupine.VisualizePath(model, info, path); err == nil {
			t.Logf("history visualisation written to chaos/%s", path)
		}
		t.Fatal("history is NOT linearizable")
	default:
		t.Fatal("linearizability check timed out; the result is inconclusive")
	}
}

// pause sleeps for a random time in [min, max) and reports false if ctx
// ended first.
func pause(ctx context.Context, rng *rand.Rand, min, max time.Duration) bool {
	d := min + time.Duration(rng.Int63n(int64(max-min)))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// killLeader repeatedly crashes whichever node is leader and restarts it a
// little later.
func killLeader(ctx context.Context, c *cluster.Cluster, rng *rand.Rand) {
	for pause(ctx, rng, 400*time.Millisecond, 800*time.Millisecond) {
		leader, ok := c.Leader()
		if !ok {
			continue
		}
		c.Crash(leader)
		if !pause(ctx, rng, 200*time.Millisecond, 500*time.Millisecond) {
			return
		}
		if err := c.Start(leader); err != nil {
			panic(err)
		}
	}
}

// partitions repeatedly splits the cluster in two at random, half of the
// time making sure the current leader lands in the smaller part.
func partitions(ctx context.Context, c *cluster.Cluster, rng *rand.Rand) {
	for pause(ctx, rng, 300*time.Millisecond, 600*time.Millisecond) {
		c.Net.Partition(randomSplit(c, rng))
		if !pause(ctx, rng, 300*time.Millisecond, 700*time.Millisecond) {
			return
		}
		c.Net.Heal()
	}
}

func randomSplit(c *cluster.Cluster, rng *rand.Rand) (a, b []transport.NodeID) {
	ids := append([]transport.NodeID(nil), c.IDs...)
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	cut := 1 + rng.Intn(len(ids)-1)
	if leader, ok := c.Leader(); ok && rng.Intn(2) == 0 {
		// Put the leader first and keep its side a minority.
		for i, id := range ids {
			if id == leader {
				ids[0], ids[i] = ids[i], ids[0]
			}
		}
		cut = 1 + rng.Intn((len(ids)-1)/2)
	}
	return ids[:cut], ids[cut:]
}

// everything mixes crashes, restarts and partitions, never taking down more
// than a minority at once.
func everything(ctx context.Context, c *cluster.Cluster, rng *rand.Rand) {
	for pause(ctx, rng, 150*time.Millisecond, 450*time.Millisecond) {
		live := c.Live()
		down := len(c.IDs) - len(live)
		switch rng.Intn(4) {
		case 0:
			if down < (len(c.IDs)-1)/2 {
				c.Crash(live[rng.Intn(len(live))])
			}
		case 1:
			for _, id := range c.IDs {
				if c.Server(id) == nil {
					if err := c.Start(id); err != nil {
						panic(err)
					}
					break
				}
			}
		case 2:
			c.Net.Partition(randomSplit(c, rng))
		case 3:
			c.Net.Heal()
		}
	}
}

func lossy() simnet.Config {
	return simnet.Config{
		Seed:          *seed,
		DropRate:      0.05,
		MaxDelay:      5 * time.Millisecond,
		ReorderRate:   0.1,
		ReorderDelay:  20 * time.Millisecond,
		DuplicateRate: 0.05,
	}
}

func TestBaseline(t *testing.T) {
	run(t, scenario{size: 3})
}

func TestKillLeader(t *testing.T) {
	run(t, scenario{size: 3, nemesis: killLeader})
}

func TestPartitions(t *testing.T) {
	run(t, scenario{size: 5, nemesis: partitions})
}

func TestMessageLoss(t *testing.T) {
	run(t, scenario{size: 3, net: lossy()})
}

func TestEverything(t *testing.T) {
	run(t, scenario{size: 5, net: lossy(), nemesis: everything})
}

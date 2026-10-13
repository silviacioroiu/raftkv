package simnet_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/simnet"
	"github.com/silviacioroiu/raftkv/transport/transporttest"
)

type ping struct {
	Seq int
}

type pong struct {
	Seq int
}

// join attaches a node whose "ping" handler calls onPing (if set) and echoes
// the sequence number.
func join(n *simnet.Network, id transport.NodeID, onPing func(seq int)) *simnet.Endpoint {
	ep := n.Join(id)
	mux := transport.NewMux()
	transport.Register(mux, "ping", func(ctx context.Context, from transport.NodeID, req *ping) (*pong, error) {
		if onPing != nil {
			onPing(req.Seq)
		}
		return &pong{Seq: req.Seq}, nil
	})
	ep.Serve(mux.Handle)
	return ep
}

func send(from transport.Transport, to transport.NodeID, seq int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := transport.Invoke[ping, pong](ctx, from, to, "ping", &ping{Seq: seq})
	if err != nil {
		return err
	}
	if resp.Seq != seq {
		return fmt.Errorf("got seq %d, want %d", resp.Seq, seq)
	}
	return nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConformance(t *testing.T) {
	transporttest.Run(t, func(t *testing.T, ids []transport.NodeID) []transport.Transport {
		n := simnet.New(simnet.Config{})
		t.Cleanup(n.Close)
		out := make([]transport.Transport, len(ids))
		for i, id := range ids {
			out[i] = n.Join(id)
		}
		return out
	})
}

func TestReliableByDefault(t *testing.T) {
	n := simnet.New(simnet.Config{})
	defer n.Close()
	a := join(n, "a", nil)
	join(n, "b", nil)

	for i := 0; i < 200; i++ {
		if err := send(a, "b", i, time.Second); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	s := n.Stats()
	if s.Sent != 400 || s.Delivered != 400 || s.Dropped != 0 || s.Blocked != 0 {
		t.Fatalf("unexpected stats on a reliable network: %+v", s)
	}
}

func TestDropRate(t *testing.T) {
	const calls, rate = 1000, 0.2
	n := simnet.New(simnet.Config{Seed: 1, DropRate: rate})
	defer n.Close()
	a := join(n, "a", nil)
	join(n, "b", nil)

	var failed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			if err := send(a, "b", seq, 500*time.Millisecond); err != nil {
				failed.Add(1)
			}
		}(i)
	}
	wg.Wait()

	s := n.Stats()
	observed := float64(s.Dropped) / float64(s.Sent)
	if math.Abs(observed-rate) > 0.05 {
		t.Errorf("observed drop rate %.3f, configured %.2f (stats %+v)", observed, rate, s)
	}
	// A call fails exactly when its request or its reply was dropped.
	if uint64(failed.Load()) != s.Dropped {
		t.Errorf("%d calls failed but %d messages were dropped", failed.Load(), s.Dropped)
	}
}

func TestDelayBounds(t *testing.T) {
	const lo, hi = 10 * time.Millisecond, 30 * time.Millisecond
	n := simnet.New(simnet.Config{Seed: 2, MinDelay: lo, MaxDelay: hi})
	defer n.Close()
	a := join(n, "a", nil)
	join(n, "b", nil)

	for i := 0; i < 15; i++ {
		begin := time.Now()
		if err := send(a, "b", i, 2*time.Second); err != nil {
			t.Fatal(err)
		}
		took := time.Since(begin)
		// A round trip is two messages. The upper bound is loose because OS
		// timers can fire late, never early.
		if took < 2*lo {
			t.Errorf("round trip %v is shorter than twice the minimum delay", took)
		}
		if took > 2*hi+250*time.Millisecond {
			t.Errorf("round trip %v is far longer than twice the maximum delay", took)
		}
	}
}

func TestReorder(t *testing.T) {
	const calls = 40
	n := simnet.New(simnet.Config{Seed: 3, MaxDelay: 30 * time.Millisecond})
	defer n.Close()

	var mu sync.Mutex
	var arrived []int
	a := join(n, "a", nil)
	join(n, "b", func(seq int) {
		mu.Lock()
		arrived = append(arrived, seq)
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			if err := send(a, "b", seq, 2*time.Second); err != nil {
				t.Errorf("call %d: %v", seq, err)
			}
		}(i)
		time.Sleep(200 * time.Microsecond) // fix the send order
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(arrived) != calls {
		t.Fatalf("%d of %d requests arrived", len(arrived), calls)
	}
	if sort.IntsAreSorted(arrived) {
		t.Fatal("requests arrived in send order; expected reordering")
	}
}

func TestDuplicate(t *testing.T) {
	n := simnet.New(simnet.Config{Seed: 4, DuplicateRate: 1})
	defer n.Close()

	var handled atomic.Int64
	a := join(n, "a", nil)
	join(n, "b", func(int) { handled.Add(1) })

	const calls = 20
	for i := 0; i < calls; i++ {
		if err := send(a, "b", i, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "every request to be handled twice", func() bool {
		return handled.Load() == 2*calls
	})
}

// With only the return path cut, the handler runs but the caller never hears
// back. This is the case that makes client request dedup necessary.
func TestOneWayBlockLosesReply(t *testing.T) {
	n := simnet.New(simnet.Config{})
	defer n.Close()

	var onA, onB atomic.Int64
	a := join(n, "a", func(int) { onA.Add(1) })
	b := join(n, "b", func(int) { onB.Add(1) })
	n.Block("b", "a")

	if err := send(a, "b", 1, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a -> b: got %v, want a timeout", err)
	}
	if onB.Load() != 1 {
		t.Fatalf("handler on b ran %d times, want 1", onB.Load())
	}

	if err := send(b, "a", 2, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("b -> a: got %v, want a timeout", err)
	}
	if onA.Load() != 0 {
		t.Fatalf("handler on a ran %d times across a cut link", onA.Load())
	}

	n.Unblock("b", "a")
	if err := send(a, "b", 3, time.Second); err != nil {
		t.Fatalf("a -> b after unblock: %v", err)
	}
	if err := send(b, "a", 4, time.Second); err != nil {
		t.Fatalf("b -> a after unblock: %v", err)
	}
}

func TestPartitionAndHeal(t *testing.T) {
	n := simnet.New(simnet.Config{})
	defer n.Close()
	a := join(n, "a", nil)
	b := join(n, "b", nil)
	c := join(n, "c", nil)

	n.Partition([]transport.NodeID{"a"}, []transport.NodeID{"b", "c"})

	if err := send(b, "c", 1, time.Second); err != nil {
		t.Errorf("b -> c inside the majority: %v", err)
	}
	if err := send(c, "b", 2, time.Second); err != nil {
		t.Errorf("c -> b inside the majority: %v", err)
	}
	if err := send(a, "b", 3, 100*time.Millisecond); err == nil {
		t.Error("a -> b crossed the partition")
	}
	if err := send(c, "a", 4, 100*time.Millisecond); err == nil {
		t.Error("c -> a crossed the partition")
	}

	n.Heal()
	if err := send(a, "b", 5, time.Second); err != nil {
		t.Errorf("a -> b after heal: %v", err)
	}
	if err := send(c, "a", 6, time.Second); err != nil {
		t.Errorf("c -> a after heal: %v", err)
	}
}

func TestCrashAndRejoin(t *testing.T) {
	n := simnet.New(simnet.Config{})
	defer n.Close()
	a := join(n, "a", nil)
	join(n, "b", nil)

	n.Remove("b")
	if err := send(a, "b", 1, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call to a crashed node: got %v, want a timeout", err)
	}

	join(n, "b", nil)
	if err := send(a, "b", 2, time.Second); err != nil {
		t.Fatalf("call after rejoin: %v", err)
	}
}

func TestClosingCallerUnblocksCall(t *testing.T) {
	n := simnet.New(simnet.Config{})
	defer n.Close()
	a := join(n, "a", nil)
	join(n, "b", nil)
	n.Partition([]transport.NodeID{"a"}, []transport.NodeID{"b"})

	errc := make(chan error, 1)
	go func() { errc <- send(a, "b", 1, 10*time.Second) }()

	time.Sleep(20 * time.Millisecond)
	a.Close()

	select {
	case err := <-errc:
		if !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call still blocked after its endpoint was closed")
	}
}

func TestCloseStopsHandlersAndCallers(t *testing.T) {
	n := simnet.New(simnet.Config{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	a := n.Join("a")
	b := n.Join("b")

	var started atomic.Int64
	b.Serve(func(ctx context.Context, from transport.NodeID, method string, payload []byte) ([]byte, error) {
		started.Add(1)
		<-ctx.Done() // a handler stuck until shutdown
		return nil, ctx.Err()
	})

	const calls = 10
	errc := make(chan error, calls)
	for i := 0; i < calls; i++ {
		go func() {
			_, err := a.Call(context.Background(), "b", "hang", nil)
			errc <- err
		}()
	}
	eventually(t, "all handlers to start", func() bool { return started.Load() == calls })

	closed := make(chan struct{})
	go func() {
		n.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return: a delivery goroutine leaked")
	}

	for i := 0; i < calls; i++ {
		select {
		case err := <-errc:
			if !errors.Is(err, transport.ErrClosed) {
				t.Errorf("caller got %v, want ErrClosed", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a caller is still blocked after Close")
		}
	}
}

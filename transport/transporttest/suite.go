// Package transporttest holds the behaviour every Transport must have on a
// healthy network. Each implementation runs this same suite.
package transporttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
)

// Factory builds one connected, not-yet-serving transport per ID and
// registers its own cleanup with t.
type Factory func(t *testing.T, ids []transport.NodeID) []transport.Transport

type EchoArgs struct {
	Seq  int
	Text string
}

type EchoReply struct {
	Seq  int
	Text string
	From transport.NodeID
}

type Empty struct {
	Unused bool
}

func start(t *testing.T, mk Factory, ids ...transport.NodeID) []transport.Transport {
	t.Helper()
	nodes := mk(t, ids)
	if len(nodes) != len(ids) {
		t.Fatalf("factory returned %d transports for %d ids", len(nodes), len(ids))
	}
	for _, node := range nodes {
		id := node.ID()
		mux := transport.NewMux()
		transport.Register(mux, "echo", func(ctx context.Context, from transport.NodeID, req *EchoArgs) (*EchoReply, error) {
			return &EchoReply{Seq: req.Seq, Text: req.Text, From: id}, nil
		})
		transport.Register(mux, "fail", func(ctx context.Context, from transport.NodeID, req *Empty) (*Empty, error) {
			return nil, errors.New("boom")
		})
		transport.Register(mux, "slow", func(ctx context.Context, from transport.NodeID, req *Empty) (*Empty, error) {
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			return &Empty{}, nil
		})
		transport.Register(mux, "whoami", func(ctx context.Context, from transport.NodeID, req *Empty) (*EchoReply, error) {
			return &EchoReply{From: from}, nil
		})
		node.Serve(mux.Handle)
	}
	return nodes
}

func echo(tr transport.Transport, to transport.NodeID, seq int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	text := fmt.Sprintf("msg-%d", seq)
	resp, err := transport.Invoke[EchoArgs, EchoReply](ctx, tr, to, "echo", &EchoArgs{Seq: seq, Text: text})
	if err != nil {
		return err
	}
	if resp.Seq != seq || resp.Text != text || resp.From != to {
		return fmt.Errorf("echo %d to %s: got %+v", seq, to, *resp)
	}
	return nil
}

// Run executes the suite against the transports built by mk.
func Run(t *testing.T, mk Factory) {
	t.Run("RoundTrip", func(t *testing.T) {
		nodes := start(t, mk, "a", "b", "c")
		for _, from := range nodes {
			for _, to := range nodes {
				if from.ID() == to.ID() {
					continue
				}
				if err := echo(from, to.ID(), 1); err != nil {
					t.Errorf("%s -> %s: %v", from.ID(), to.ID(), err)
				}
			}
		}
	})

	t.Run("SenderIdentity", func(t *testing.T) {
		nodes := start(t, mk, "a", "b")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := transport.Invoke[Empty, EchoReply](ctx, nodes[0], "b", "whoami", &Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if resp.From != "a" {
			t.Fatalf("handler saw sender %q, want %q", resp.From, "a")
		}
	})

	t.Run("ConcurrentCalls", func(t *testing.T) {
		nodes := start(t, mk, "a", "b", "c")
		var wg sync.WaitGroup
		for i := 0; i < 90; i++ {
			from := nodes[i%len(nodes)]
			to := nodes[(i+1)%len(nodes)].ID()
			wg.Add(1)
			go func(seq int) {
				defer wg.Done()
				if err := echo(from, to, seq); err != nil {
					t.Errorf("call %d: %v", seq, err)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("RemoteError", func(t *testing.T) {
		nodes := start(t, mk, "a", "b")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := transport.Invoke[Empty, Empty](ctx, nodes[0], "b", "fail", &Empty{})
		var remote *transport.RemoteError
		if !errors.As(err, &remote) {
			t.Fatalf("got %v, want a RemoteError", err)
		}
		if !strings.Contains(remote.Msg, "boom") {
			t.Fatalf("remote error %q does not carry the handler's message", remote.Msg)
		}
	})

	t.Run("UnknownMethod", func(t *testing.T) {
		nodes := start(t, mk, "a", "b")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nodes[0].Call(ctx, "b", "no-such-method", nil)
		var remote *transport.RemoteError
		if !errors.As(err, &remote) {
			t.Fatalf("got %v, want a RemoteError", err)
		}
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		nodes := start(t, mk, "a", "b")
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		begin := time.Now()
		_, err := transport.Invoke[Empty, Empty](ctx, nodes[0], "b", "slow", &Empty{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want context.DeadlineExceeded", err)
		}
		if took := time.Since(begin); took > time.Second {
			t.Fatalf("call returned after %v, long past its 50ms deadline", took)
		}
		// The transport must still work after a timed-out call.
		if err := echo(nodes[0], "b", 2); err != nil {
			t.Fatalf("echo after timeout: %v", err)
		}
	})

	t.Run("CallAfterClose", func(t *testing.T) {
		nodes := start(t, mk, "a", "b")
		if err := nodes[0].Close(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := nodes[0].Call(ctx, "b", "echo", nil)
		if !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	})
}

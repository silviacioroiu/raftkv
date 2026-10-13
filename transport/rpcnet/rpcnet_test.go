package rpcnet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/rpcnet"
	"github.com/silviacioroiu/raftkv/transport/transporttest"
)

func cluster(t *testing.T, ids []transport.NodeID) []*rpcnet.Transport {
	t.Helper()
	nodes := make([]*rpcnet.Transport, len(ids))
	for i, id := range ids {
		tr, err := rpcnet.Listen(id, "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tr.Close() })
		nodes[i] = tr
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.SetPeer(b.ID(), b.Addr())
			}
		}
	}
	return nodes
}

func TestConformance(t *testing.T) {
	transporttest.Run(t, func(t *testing.T, ids []transport.NodeID) []transport.Transport {
		nodes := cluster(t, ids)
		out := make([]transport.Transport, len(nodes))
		for i, n := range nodes {
			out[i] = n
		}
		return out
	})
}

func TestUnknownPeer(t *testing.T) {
	nodes := cluster(t, []transport.NodeID{"a"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := nodes[0].Call(ctx, "ghost", "echo", nil)
	if !errors.Is(err, transport.ErrUnknownPeer) {
		t.Fatalf("got %v, want ErrUnknownPeer", err)
	}
}

// After a peer restarts, the connection cached from before is dead. The
// first call must notice and reconnect instead of failing.
func TestReconnectsAfterPeerRestart(t *testing.T) {
	nodes := cluster(t, []transport.NodeID{"a", "b"})
	a, b := nodes[0], nodes[1]
	echo := func(ctx context.Context, from transport.NodeID, method string, payload []byte) ([]byte, error) {
		return payload, nil
	}
	b.Serve(echo)

	call := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := a.Call(ctx, "b", "echo", []byte("hi"))
		return err
	}
	if err := call(); err != nil {
		t.Fatalf("call before restart: %v", err)
	}

	addr := b.Addr()
	b.Close()
	reborn, err := rpcnet.Listen("b", addr)
	if err != nil {
		t.Fatalf("restart on %s: %v", addr, err)
	}
	defer reborn.Close()
	reborn.Serve(echo)

	time.Sleep(100 * time.Millisecond) // let a's side of the old connection see the close
	if err := call(); err != nil {
		t.Fatalf("first call after the peer restarted: %v", err)
	}
}

func TestPeerDown(t *testing.T) {
	nodes := cluster(t, []transport.NodeID{"a", "b"})
	a, b := nodes[0], nodes[1]
	b.Serve(func(ctx context.Context, from transport.NodeID, method string, payload []byte) ([]byte, error) {
		return payload, nil
	})

	call := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := a.Call(ctx, "b", "echo", []byte("hi"))
		return err
	}

	if err := call(); err != nil {
		t.Fatalf("call while b is up: %v", err)
	}
	b.Close()

	// First call hits the dead cached connection, second has to redial.
	for i := 0; i < 2; i++ {
		err := call()
		if err == nil {
			t.Fatalf("call %d to a stopped peer succeeded", i)
		}
		var remote *transport.RemoteError
		if errors.As(err, &remote) {
			t.Fatalf("call %d to a stopped peer reported a handler error: %v", i, err)
		}
	}
}

package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
)

// Client talks to the cluster and hides leader changes and lost messages:
// it retries each operation, on every server in turn, until it gets an
// answer. Because every retry carries the same (ClientID, Seq), the
// operation takes effect exactly once however many times it is sent.
//
// A Client runs one operation at a time; use one Client per concurrent caller.
type Client struct {
	tr      transport.Transport
	servers []transport.NodeID
	id      uint64

	// AttemptTimeout bounds one try against one server. Default 500ms.
	AttemptTimeout time.Duration

	mu       sync.Mutex
	seq      uint64
	leader   int               // index into servers of the server to try first
	failedAt map[int]time.Time // when each server last refused a connection
}

func NewClient(tr transport.Transport, servers []transport.NodeID) *Client {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return &Client{
		tr:             tr,
		servers:        append([]transport.NodeID(nil), servers...),
		id:             binary.LittleEndian.Uint64(b[:]),
		AttemptTimeout: 500 * time.Millisecond,
		failedAt:       make(map[int]time.Time),
	}
}

// ID is the identifier the cluster deduplicates this client's requests by.
func (c *Client) ID() uint64 { return c.id }

// Get returns the value of key, or "" if it was never written.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.do(ctx, Op{Kind: OpGet, Key: key})
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, Op{Kind: OpPut, Key: key, Value: value})
	return err
}

// Append adds value to the end of key's current value.
func (c *Client) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, Op{Kind: OpAppend, Key: key, Value: value})
	return err
}

// do retries op until it succeeds or ctx ends. If ctx ends first the
// outcome is unknown: the operation may or may not have taken effect.
func (c *Client) do(ctx context.Context, op Op) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	op.ClientID, op.Seq = c.id, c.seq
	req := &Request{Op: op}

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		target := c.servers[c.leader]
		attempt, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		resp, err := transport.Invoke[Request, Response](attempt, c.tr, target, MethodExecute, req)
		cancel()

		if err == nil && resp.Code == CodeOK {
			return resp.Value, nil
		}

		// A refused or reset connection is strong evidence that the server
		// is down. A timeout is not: the message may just have been lost.
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			c.failedAt[c.leader] = time.Now()
		}
		c.leader = c.nextServer(resp, err)

		// Do not spin while an election is in progress.
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// suspectFor is how long a server that refused a connection is avoided.
const suspectFor = time.Second

// nextServer picks who to try after a failed attempt on c.leader. It follows
// the leader hint when there is one, except when the hint points at a server
// that just refused a connection: right after a leader crashes, the
// survivors still name it as leader until their election timeouts fire, and
// bouncing back to it would waste an attempt each time.
func (c *Client) nextServer(resp *Response, err error) int {
	suspect := func(i int) bool {
		t, ok := c.failedAt[i]
		return ok && time.Since(t) < suspectFor
	}
	if err == nil && resp.LeaderHint != "" {
		for i, id := range c.servers {
			if id == resp.LeaderHint && i != c.leader && !suspect(i) {
				return i
			}
		}
	}
	for step := 1; step <= len(c.servers); step++ {
		if i := (c.leader + step) % len(c.servers); !suspect(i) {
			return i
		}
	}
	return (c.leader + 1) % len(c.servers)
}

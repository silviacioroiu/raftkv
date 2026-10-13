// Package transport defines how nodes talk to each other without committing
// to a concrete network. Raft depends only on the Transport interface, so the
// same code runs over the fault-injecting simulator (simnet) and over real
// TCP (rpcnet).
package transport

import (
	"context"
	"errors"
)

// NodeID names a node in the cluster.
type NodeID string

// Handler processes one incoming request and returns the encoded response.
// ctx is cancelled when the receiving node shuts down; long-running handlers
// must return when that happens.
type Handler func(ctx context.Context, from NodeID, method string, payload []byte) ([]byte, error)

// Transport is one node's view of the network.
//
// Call is a request/response exchange with at-most-once *delivery of the
// reply*, not of the request: the remote handler may have run even when Call
// returns an error, and may run more than once. Callers must therefore make
// their requests idempotent.
type Transport interface {
	ID() NodeID

	// Call sends payload to node `to` and blocks until a response arrives,
	// ctx ends, or the transport is closed. Always pass a ctx with a deadline:
	// a lost message is indistinguishable from a slow one.
	Call(ctx context.Context, to NodeID, method string, payload []byte) ([]byte, error)

	// Serve installs the handler for incoming calls, replacing any previous one.
	Serve(h Handler)

	Close() error
}

var (
	// ErrClosed is returned by calls on a transport that has been shut down.
	ErrClosed = errors.New("transport: closed")
	// ErrUnknownPeer is returned when no address is known for the target node.
	ErrUnknownPeer = errors.New("transport: unknown peer")
)

// RemoteError means the request reached the remote node and its handler
// returned an error. Any other error from Call means the outcome is unknown.
type RemoteError struct {
	Msg string
}

func (e *RemoteError) Error() string { return "remote: " + e.Msg }

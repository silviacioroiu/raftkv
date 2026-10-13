package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

// Mux routes incoming calls to typed handlers by method name. Pass mux.Handle
// to Transport.Serve.
type Mux struct {
	mu       sync.RWMutex
	handlers map[string]rawHandler
}

type rawHandler func(ctx context.Context, from NodeID, payload []byte) ([]byte, error)

func NewMux() *Mux {
	return &Mux{handlers: make(map[string]rawHandler)}
}

// Handle implements Handler.
func (m *Mux) Handle(ctx context.Context, from NodeID, method string, payload []byte) ([]byte, error) {
	m.mu.RLock()
	h, ok := m.handlers[method]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown method %q", method)
	}
	return h(ctx, from, payload)
}

// Register binds a typed handler to a method name. It is a function rather
// than a method on Mux because Go methods cannot have type parameters.
//
// Messages are gob-encoded, so only exported (capitalised) struct fields are
// transmitted; unexported fields silently arrive as zero values.
func Register[Req, Resp any](m *Mux, method string, fn func(ctx context.Context, from NodeID, req *Req) (*Resp, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[method] = func(ctx context.Context, from NodeID, payload []byte) ([]byte, error) {
		var req Req
		if err := decode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := fn(ctx, from, &req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, errors.New("handler returned nil response")
		}
		return encode(resp)
	}
}

// Invoke is the typed client side of Register. Resp cannot be inferred from
// the arguments, so call it as Invoke[Req, Resp](ctx, t, to, method, &req).
func Invoke[Req, Resp any](ctx context.Context, t Transport, to NodeID, method string, req *Req) (*Resp, error) {
	if req == nil {
		return nil, errors.New("transport: nil request")
	}
	payload, err := encode(req)
	if err != nil {
		return nil, err
	}
	out, err := t.Call(ctx, to, method, payload)
	if err != nil {
		return nil, err
	}
	var resp Resp
	if err := decode(out, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("transport: encode %T: %w", v, err)
	}
	return buf.Bytes(), nil
}

func decode(data []byte, v any) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return fmt.Errorf("transport: decode %T: %w", v, err)
	}
	return nil
}

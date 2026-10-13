// Package kv is a linearizable key-value store replicated with Raft.
//
// Every operation, reads included, goes through the Raft log and is executed
// when it is applied. All replicas therefore execute the same operations in
// the same order, and a client only gets an answer once its operation is
// committed.
package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/transport"
)

type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpAppend:
		return "append"
	}
	return "unknown"
}

// Op is the command stored in the Raft log. ClientID and Seq identify the
// request, so that a retry of an operation that already committed is
// recognised and not executed a second time.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

type Code uint8

const (
	CodeOK Code = iota
	// CodeWrongLeader: this server is not the leader, or lost leadership
	// before the operation committed. The operation may or may not have been
	// committed; retry it, with the same Seq, on another server.
	CodeWrongLeader
	// CodeTimeout: the operation did not commit in time. Same advice.
	CodeTimeout
)

const MethodExecute = "KV.Execute"

type Request struct {
	Op Op
}

type Response struct {
	Code       Code
	Value      string           // for Get
	LeaderHint transport.NodeID // who this server believes the leader is
}

// session remembers the last operation executed for one client.
type session struct {
	Seq   uint64
	Value string
}

type result struct {
	code  Code
	value string
}

// pending is one client request waiting for its log entry to be applied.
type pending struct {
	data  []byte
	index uint64 // set once proposed; guarded by Server.mu
	term  uint64
	ch    chan result // buffered; receives exactly one result
}

type Options struct {
	Raft      raft.Config
	Transport transport.Transport
	Store     raft.Persister
	Logger    *slog.Logger

	// OpTimeout bounds how long a request waits to be committed. Default 1s.
	OpTimeout time.Duration
	// MaxBatch is the most requests proposed to Raft in one batch. Default 128.
	MaxBatch int
}

// Server is one replica: a Raft node plus the state machine it drives.
type Server struct {
	node      *raft.Node
	opTimeout time.Duration
	maxBatch  int

	mu       sync.Mutex
	data     map[string]string
	sessions map[uint64]session
	waiters  map[uint64]*pending // by log index

	proposeCh chan *pending
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// Start builds a replica, recovers its state from opts.Store and joins the
// cluster through opts.Transport.
func Start(opts Options) (*Server, error) {
	if opts.OpTimeout == 0 {
		opts.OpTimeout = time.Second
	}
	if opts.MaxBatch == 0 {
		opts.MaxBatch = 128
	}
	s := &Server{
		opTimeout: opts.OpTimeout,
		maxBatch:  opts.MaxBatch,
		data:      make(map[string]string),
		sessions:  make(map[uint64]session),
		waiters:   make(map[uint64]*pending),
		proposeCh: make(chan *pending, 4096),
		stopCh:    make(chan struct{}),
	}

	mux := transport.NewMux()
	node, err := raft.New(opts.Raft, raft.Deps{
		Transport: opts.Transport,
		Mux:       mux,
		Store:     opts.Store,
		FSM:       s,
		Logger:    opts.Logger,
	})
	if err != nil {
		return nil, err
	}
	s.node = node
	transport.Register(mux, MethodExecute, s.handleExecute)

	opts.Transport.Serve(mux.Handle)
	node.Start()
	s.wg.Add(1)
	go s.batchLoop()
	return s, nil
}

// Close stops the replica. The transport and store belong to the caller.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	s.node.Stop()
}

// Raft exposes the underlying node, for status and tests.
func (s *Server) Raft() *raft.Node { return s.node }

func (s *Server) handleExecute(ctx context.Context, from transport.NodeID, req *Request) (*Response, error) {
	// Fail fast instead of queueing when we already know we cannot commit.
	if st := s.node.Status(); st.Role != raft.Leader {
		return &Response{Code: CodeWrongLeader, LeaderHint: st.LeaderID}, nil
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&req.Op); err != nil {
		return nil, fmt.Errorf("kv: encode op: %w", err)
	}
	p := &pending{data: buf.Bytes(), ch: make(chan result, 1)}

	select {
	case s.proposeCh <- p:
	case <-s.stopCh:
		return nil, errors.New("kv: server stopped")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	timer := time.NewTimer(s.opTimeout)
	defer timer.Stop()
	select {
	case r := <-p.ch:
		return s.respond(r), nil
	case <-timer.C:
	case <-s.stopCh:
	case <-ctx.Done():
	}

	// Gave up waiting. The entry may still commit later; the client's retry
	// carries the same Seq, so it will not be executed twice.
	s.mu.Lock()
	if p.index != 0 && s.waiters[p.index] == p {
		delete(s.waiters, p.index)
	}
	s.mu.Unlock()
	return s.respond(result{code: CodeTimeout}), nil
}

func (s *Server) respond(r result) *Response {
	resp := &Response{Code: r.code, Value: r.value}
	if r.code != CodeOK {
		resp.LeaderHint = s.node.Status().LeaderID
	}
	return resp
}

// batchLoop hands queued requests to Raft. While one batch is being written
// to disk the next one accumulates, so under load many requests share a
// single disk sync.
func (s *Server) batchLoop() {
	defer s.wg.Done()
	for {
		var batch []*pending
		select {
		case <-s.stopCh:
			return
		case p := <-s.proposeCh:
			batch = append(batch, p)
		}
	drain:
		for len(batch) < s.maxBatch {
			select {
			case p := <-s.proposeCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}

		cmds := make([][]byte, len(batch))
		for i, p := range batch {
			cmds[i] = p.data
		}

		// mu is held across ProposeBatch so that Apply cannot see one of
		// these entries before its waiter is registered.
		s.mu.Lock()
		first, term, ok := s.node.ProposeBatch(cmds)
		if !ok {
			s.mu.Unlock()
			for _, p := range batch {
				p.ch <- result{code: CodeWrongLeader}
			}
			continue
		}
		for i, p := range batch {
			index := first + uint64(i)
			if old := s.waiters[index]; old != nil {
				// The index was proposed before under an earlier leadership
				// and that entry was overwritten.
				old.ch <- result{code: CodeWrongLeader}
			}
			p.index, p.term = index, term
			s.waiters[index] = p
		}
		s.mu.Unlock()
	}
}

// Apply implements raft.StateMachine. It executes one committed operation
// and wakes the request waiting for it, if this replica proposed it.
func (s *Server) Apply(e raft.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var r result
	if !e.NoOp {
		var op Op
		if err := gob.NewDecoder(bytes.NewReader(e.Data)).Decode(&op); err != nil {
			panic(fmt.Sprintf("kv: corrupt log entry %d: %v", e.Index, err))
		}
		r = s.execute(op)
	}

	p := s.waiters[e.Index]
	if p == nil {
		return
	}
	delete(s.waiters, e.Index)
	// Same index and term means the same entry (Log Matching, §5.3). A
	// different term means our entry was replaced by another leader's.
	if e.NoOp || p.term != e.Term {
		r = result{code: CodeWrongLeader}
	}
	p.ch <- r
}

// execute runs op against the map unless it is a duplicate. mu must be held.
func (s *Server) execute(op Op) result {
	last := s.sessions[op.ClientID]
	if op.Seq <= last.Seq {
		// Already executed: a retry after a lost reply or a leader change.
		// Answer from the session table and leave the data untouched.
		return result{value: last.Value}
	}

	var value string
	switch op.Kind {
	case OpGet:
		value = s.data[op.Key]
	case OpPut:
		s.data[op.Key] = op.Value
	case OpAppend:
		s.data[op.Key] += op.Value
	}
	s.sessions[op.ClientID] = session{Seq: op.Seq, Value: value}
	return result{value: value}
}

type snapshot struct {
	Data     map[string]string
	Sessions map[uint64]session
}

// Snapshot implements raft.StateMachine. The session table is part of the
// state: without it a replica restored from a snapshot would re-execute
// retried operations.
func (s *Server) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot{Data: s.data, Sessions: s.sessions}); err != nil {
		panic(fmt.Sprintf("kv: encode snapshot: %v", err))
	}
	return buf.Bytes()
}

// Restore implements raft.StateMachine.
func (s *Server) Restore(data []byte) {
	snap := snapshot{}
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
			panic(fmt.Sprintf("kv: corrupt snapshot: %v", err))
		}
	}
	if snap.Data == nil {
		snap.Data = make(map[string]string)
	}
	if snap.Sessions == nil {
		snap.Sessions = make(map[uint64]session)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.sessions = snap.Data, snap.Sessions
	// Only a follower that fell behind receives a snapshot, so whatever it
	// proposed as leader has been superseded.
	for index, p := range s.waiters {
		delete(s.waiters, index)
		p.ch <- result{code: CodeWrongLeader}
	}
}

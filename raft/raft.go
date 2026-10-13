package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/internal/clock"
	"github.com/silviacioroiu/raftkv/transport"
)

// Node is one Raft server. All fields below mu are guarded by it. Every RPC
// is sent with mu released; it is only re-acquired to process the reply.
type Node struct {
	cfg   Config
	tr    transport.Transport
	store Persister
	fsm   StateMachine
	clk   clock.Clock
	log   *slog.Logger

	mu sync.Mutex

	// Persistent state (Figure 2). entries[0] is a sentinel carrying the
	// index and term of the last entry covered by the snapshot, so the log
	// is never empty and index arithmetic needs no special cases. Entry i
	// lives at entries[i - entries[0].Index].
	currentTerm uint64
	votedFor    transport.NodeID // "" when no vote was cast this term
	entries     []Entry
	snapshot    []byte // state machine snapshot through entries[0].Index
	dirtyFrom   uint64 // lowest log index changed since the last persist; 0 if none

	// Volatile state.
	role        Role
	leaderID    transport.NodeID
	commitIndex uint64
	lastApplied uint64

	// Election timer: an election starts when no valid leader contact or
	// vote grant has happened for electionTimeout.
	lastHeard       time.Time
	electionTimeout time.Duration
	rng             *rand.Rand

	// Leader state, reinitialised on election (§5.3).
	nextIndex  map[transport.NodeID]uint64
	matchIndex map[transport.NodeID]uint64
	inflight   map[transport.NodeID]bool // a replicate goroutine is running

	// Set when a snapshot from the leader has been installed in the log but
	// not yet handed to the state machine. The applier does the hand-off,
	// so that it stays the only goroutine touching the FSM.
	restorePending bool

	applyCond *sync.Cond
	stopCh    chan struct{}
	stopped   bool
	wg        sync.WaitGroup

	// ctx is cancelled by Stop so that outgoing RPCs do not outlive the node.
	ctx    context.Context
	cancel context.CancelFunc
}

// walRecord is one change to the Figure 2 persistent state. Every record
// carries the current term and vote, which are tiny, plus whichever part of
// the log changed:
//
//   - Reset: the log is replaced by Base (the sentinel) followed by Entries.
//     Written when the log is compacted.
//   - From != 0: everything from index From onwards is replaced by Entries.
//     An append is the case where From is one past the last entry; a
//     follower resolving a conflict has a smaller From.
//   - neither: only the term or vote changed.
type walRecord struct {
	Term     uint64
	VotedFor transport.NodeID
	Reset    bool
	Base     Entry
	From     uint64
	Entries  []Entry
}

// snapshotRecord is the persisted form of a snapshot. Index and Term are
// stored with the data so that a crash between saving the snapshot and the
// log can be reconciled on restart.
type snapshotRecord struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// New loads durable state, restores the state machine from the latest
// snapshot and registers the RPC handlers. Call Start to begin participating.
func New(cfg Config, d Deps) (*Node, error) {
	cfg = cfg.withDefaults()
	if d.Transport == nil || d.Mux == nil || d.Store == nil || d.FSM == nil {
		return nil, errors.New("raft: Transport, Mux, Store and FSM are required")
	}
	if cfg.ElectionTimeoutMin > cfg.ElectionTimeoutMax || cfg.HeartbeatInterval >= cfg.ElectionTimeoutMin {
		return nil, fmt.Errorf("raft: need heartbeat %v < election timeout min %v <= max %v",
			cfg.HeartbeatInterval, cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax)
	}
	found := false
	for _, p := range cfg.Peers {
		found = found || p == cfg.ID
	}
	if !found {
		return nil, fmt.Errorf("raft: node %q is not in Peers %v", cfg.ID, cfg.Peers)
	}
	if d.Clock == nil {
		d.Clock = clock.Real{}
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	n := &Node{
		cfg:        cfg,
		tr:         d.Transport,
		store:      d.Store,
		fsm:        d.FSM,
		clk:        d.Clock,
		log:        d.Logger.With("node", string(cfg.ID)),
		entries:    []Entry{{Index: 0, Term: 0}},
		rng:        rand.New(rand.NewSource(cfg.Seed)),
		nextIndex:  make(map[transport.NodeID]uint64),
		matchIndex: make(map[transport.NodeID]uint64),
		inflight:   make(map[transport.NodeID]bool),
		stopCh:     make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.ctx, n.cancel = context.WithCancel(context.Background())

	if err := n.load(); err != nil {
		n.cancel()
		return nil, err
	}
	n.commitIndex = n.firstIndex()
	n.lastApplied = n.firstIndex()
	n.resetElectionTimer()

	transport.Register(d.Mux, MethodRequestVote, n.handleRequestVote)
	transport.Register(d.Mux, MethodAppendEntries, n.handleAppendEntries)
	transport.Register(d.Mux, MethodInstallSnapshot, n.handleInstallSnapshot)
	return n, nil
}

// load rebuilds the persistent state (§5.6 / Figure 2) by replaying the
// write-ahead log, then reconciles the log with the snapshot, which may be
// newer than the log if a crash interrupted a compaction.
func (n *Node) load() error {
	snap, records, err := n.store.Load()
	if err != nil {
		return fmt.Errorf("raft: load: %w", err)
	}
	for i, raw := range records {
		var rec walRecord
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&rec); err != nil {
			return fmt.Errorf("raft: decode log record %d: %w", i, err)
		}
		n.currentTerm, n.votedFor = rec.Term, rec.VotedFor
		switch {
		case rec.Reset:
			n.entries = append([]Entry{rec.Base}, rec.Entries...)
		case rec.From != 0:
			if rec.From <= n.firstIndex() || rec.From > n.lastIndex()+1 {
				return fmt.Errorf("raft: log record %d starts at index %d, but the log covers %d-%d",
					i, rec.From, n.firstIndex(), n.lastIndex())
			}
			n.entries = append(n.entries[:rec.From-n.firstIndex()], rec.Entries...)
		}
	}

	if len(snap) == 0 {
		return nil
	}
	var rec snapshotRecord
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&rec); err != nil {
		return fmt.Errorf("raft: decode snapshot: %w", err)
	}
	if rec.Index < n.firstIndex() {
		// Compaction writes the snapshot before the log, so the log can
		// never be ahead of the snapshot.
		return fmt.Errorf("raft: snapshot at %d is older than log start %d", rec.Index, n.firstIndex())
	}
	n.snapshot = rec.Data
	if rec.Index > n.firstIndex() {
		// A crash hit after the new snapshot was saved but before the log
		// was rewritten to match. Finish that compaction now, so the WAL
		// never has to describe a log with a gap in it.
		n.truncateBefore(rec.Index, rec.Term)
		n.persistWithSnapshot()
	}
	n.fsm.Restore(rec.Data)
	return nil
}

// Start launches the node's goroutines. The transport must be serving the
// mux the node registered with before peers can reach it.
func (n *Node) Start() {
	n.wg.Add(3)
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
}

// Stop halts the node and waits for its goroutines. It does not close the
// transport or the persister, which belong to the caller.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.cancel()
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends a command to the log if this node is the leader and starts
// replicating it. It returns the entry's index and term; the command is
// committed once a later Status reports CommitIndex >= index, and it was
// applied as proposed only if the entry at that index still has that term.
// If the leader loses its position first, another entry may take the index.
func (n *Node) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	return n.ProposeBatch([][]byte{cmd})
}

// ProposeBatch appends several commands at consecutive indices starting at
// first, with a single write to stable storage. Batching is what lets
// throughput exceed one disk sync per command (group commit).
func (n *Node) ProposeBatch(cmds [][]byte) (first, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.stopped || len(cmds) == 0 {
		return 0, 0, false
	}
	first = n.lastIndex() + 1
	for i, cmd := range cmds {
		n.entries = append(n.entries, Entry{Index: first + uint64(i), Term: n.currentTerm, Data: cmd})
	}
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.markDirty(first)
	n.persist()
	n.advanceCommitIndex() // a single-node cluster commits immediately
	n.broadcast()
	return first, n.currentTerm, true
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	last := n.entries[len(n.entries)-1]
	return Status{
		ID:            n.cfg.ID,
		Term:          n.currentTerm,
		Role:          n.role,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  last.Index,
		LastLogTerm:   last.Term,
		SnapshotIndex: n.entries[0].Index,
	}
}

// Log index helpers. All require mu to be held.

func (n *Node) firstIndex() uint64 { return n.entries[0].Index }
func (n *Node) lastIndex() uint64  { return n.entries[len(n.entries)-1].Index }

// termAt returns the term of entry i, which must be in [firstIndex, lastIndex].
func (n *Node) termAt(i uint64) uint64 {
	return n.entries[i-n.firstIndex()].Term
}

// entriesFrom copies up to max entries starting at index from.
func (n *Node) entriesFrom(from uint64, max int) []Entry {
	src := n.entries[from-n.firstIndex():]
	if len(src) > max {
		src = src[:max]
	}
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// truncateBefore drops every entry up to and including index, making
// {index, term} the new sentinel. Entries after index are kept if they are
// consistent with it, otherwise the log is reset to just the sentinel (§7).
func (n *Node) truncateBefore(index, term uint64) {
	if index <= n.lastIndex() && index >= n.firstIndex() && n.termAt(index) == term {
		tail := n.entries[index-n.firstIndex()+1:]
		kept := make([]Entry, 0, len(tail)+1)
		kept = append(kept, Entry{Index: index, Term: term})
		n.entries = append(kept, tail...)
		return
	}
	n.entries = []Entry{{Index: index, Term: term}}
}

// Role transitions. All require mu to be held and do not persist; the
// caller persists once all changes for the event are made.

func (n *Node) becomeFollower(term uint64, leader transport.NodeID) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	if n.role != Follower {
		n.log.Info("became follower", "term", n.currentTerm, "leader", string(leader))
	}
	n.role = Follower
	n.leaderID = leader
}

func (n *Node) resetElectionTimer() {
	n.lastHeard = n.clk.Now()
	spread := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionTimeout = n.cfg.ElectionTimeoutMin
	if spread > 0 {
		n.electionTimeout += time.Duration(n.rng.Int63n(int64(spread)))
	}
}

// Persistence. Requires mu to be held.

func (n *Node) encode(what string, v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		n.fatal("encode "+what, err)
	}
	return buf.Bytes()
}

// markDirty notes that the log changed from index onwards and that the
// change is not yet on disk. It must be followed by persist before mu is
// released.
func (n *Node) markDirty(index uint64) {
	if n.dirtyFrom == 0 || index < n.dirtyFrom {
		n.dirtyFrom = index
	}
}

// persist appends one record with the current term and vote and whatever
// part of the log changed since the last call. It must be called before
// replying to any RPC, or sending one, that reflects a change to them.
// Its cost is proportional to the change, not to the size of the log.
func (n *Node) persist() {
	rec := walRecord{Term: n.currentTerm, VotedFor: n.votedFor}
	if n.dirtyFrom != 0 {
		rec.From = n.dirtyFrom
		rec.Entries = n.entries[n.dirtyFrom-n.firstIndex():]
		n.dirtyFrom = 0
	}
	if err := n.store.Append(n.encode("log record", &rec)); err != nil {
		n.fatal("persist", err)
	}
}

// persistWithSnapshot saves the snapshot and rewrites the write-ahead log to
// hold only what follows it. This is what keeps the log from growing forever.
func (n *Node) persistWithSnapshot() {
	snap := snapshotRecord{Index: n.firstIndex(), Term: n.entries[0].Term, Data: n.snapshot}
	base := walRecord{
		Term:     n.currentTerm,
		VotedFor: n.votedFor,
		Reset:    true,
		Base:     n.entries[0],
		Entries:  n.entries[1:],
	}
	err := n.store.Compact(n.encode("snapshot", &snap), [][]byte{n.encode("log record", &base)})
	if err != nil {
		n.fatal("persist snapshot", err)
	}
	n.dirtyFrom = 0
}

// fatal stops the process. Continuing after a failed write could let the
// node break a promise it already made, such as a vote, after a restart.
func (n *Node) fatal(what string, err error) {
	n.log.Error("fatal: "+what, "err", err)
	panic(fmt.Sprintf("raft %s: %s: %v", n.cfg.ID, what, err))
}

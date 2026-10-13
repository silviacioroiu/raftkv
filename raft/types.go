// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", extended version).
// Section references in comments point at that paper.
//
// A Node replicates an opaque log of commands across a cluster and feeds
// committed commands, in order, to a StateMachine. It depends only on the
// transport, clock and persistence interfaces, never on a concrete network.
package raft

import (
	"log/slog"
	"time"

	"github.com/silviacioroiu/raftkv/internal/clock"
	"github.com/silviacioroiu/raftkv/transport"
)

// Entry is one log entry. Indices start at 1; index 0 is reserved.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
	// NoOp marks the blank entry a new leader appends at the start of its
	// term (§8). No-ops are still passed to StateMachine.Apply, which must not
	// change state for them; it sees them so that it can tell a waiting
	// client that its index was taken by a different entry.
	NoOp bool
}

// RPC messages, as in Figure 2 of the paper.

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  transport.NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     transport.NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool

	// Hints for the leader when Success is false, so that it can skip a whole
	// term of conflicting entries per round trip instead of one entry (§5.3,
	// "the protocol can be optimized"). ConflictTerm is the follower's term at
	// PrevLogIndex and ConflictIndex the first index it holds for that term.
	// ConflictTerm is 0 when the follower's log is simply too short, in which
	// case ConflictIndex is one past its last entry.
	ConflictTerm  uint64
	ConflictIndex uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          transport.NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Method names registered on the transport mux.
const (
	MethodRequestVote     = "Raft.RequestVote"
	MethodAppendEntries   = "Raft.AppendEntries"
	MethodInstallSnapshot = "Raft.InstallSnapshot"
)

// StateMachine is the application that Raft replicates. All three methods
// are called from a single goroutine, never concurrently with each other.
type StateMachine interface {
	// Apply executes one committed command. Commands arrive in log order.
	// After a restart, commands since the last snapshot are applied again,
	// so Apply must be deterministic.
	Apply(e Entry)
	// Snapshot serialises the complete current state.
	Snapshot() []byte
	// Restore replaces the complete current state with a snapshot.
	Restore(data []byte)
}

// Persister stores Raft's durable state (Figure 2: currentTerm, votedFor,
// log[]) as a snapshot plus a write-ahead log of records, each describing
// one change made since that snapshot. The records are opaque to the store.
type Persister interface {
	// Append adds one record. It must be durable before Append returns: a
	// crash right after it must read the record back, and a crash during it
	// must read back either the whole record or none of it.
	Append(record []byte) error
	// Compact replaces the snapshot and all records. If a crash interrupts
	// it, Load may return the new snapshot with the old records, but never
	// new records with the old snapshot.
	Compact(snapshot []byte, records [][]byte) error
	// Load returns the snapshot and the records in the order they were
	// appended. Both are empty for a store that was never written.
	Load() (snapshot []byte, records [][]byte, err error)
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type Config struct {
	ID    transport.NodeID
	Peers []transport.NodeID // every member of the cluster, including ID

	// Election timeouts are drawn uniformly from [Min, Max] (§5.2). Both
	// must be well above the heartbeat interval and the round-trip time.
	ElectionTimeoutMin time.Duration // default 150ms
	ElectionTimeoutMax time.Duration // default 300ms
	HeartbeatInterval  time.Duration // default 50ms

	RPCTimeout         time.Duration // for RequestVote and AppendEntries, default 100ms
	SnapshotRPCTimeout time.Duration // for InstallSnapshot, default 5s

	MaxEntriesPerAppend int // default 256

	// SnapshotThreshold is how many entries may accumulate after the last
	// snapshot before the node asks the state machine for a new one (§7).
	// Zero disables compaction.
	SnapshotThreshold uint64

	// Seed for the election-timeout PRNG. Zero means seed from the clock.
	Seed int64
}

func (c Config) withDefaults() Config {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.ElectionTimeoutMin, 150*time.Millisecond)
	def(&c.ElectionTimeoutMax, 300*time.Millisecond)
	def(&c.HeartbeatInterval, 50*time.Millisecond)
	def(&c.RPCTimeout, 100*time.Millisecond)
	def(&c.SnapshotRPCTimeout, 5*time.Second)
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 256
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	return c
}

// Deps are the external pieces a Node is wired to.
type Deps struct {
	Transport transport.Transport
	Mux       *transport.Mux // the node registers its RPC handlers here
	Store     Persister
	FSM       StateMachine
	Clock     clock.Clock  // optional, default clock.Real{}
	Logger    *slog.Logger // optional, default discards
}

// Status is a consistent snapshot of a node's volatile state, for tests,
// monitoring and client leader hints.
type Status struct {
	ID            transport.NodeID
	Term          uint64
	Role          Role
	LeaderID      transport.NodeID
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	SnapshotIndex uint64
}

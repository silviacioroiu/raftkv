package raft

import (
	"context"
	"sort"

	"github.com/silviacioroiu/raftkv/transport"
)

// heartbeatLoop makes a leader contact every follower at least once per
// heartbeat interval, which both replicates entries and stops elections.
func (n *Node) heartbeatLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.clk.After(n.cfg.HeartbeatInterval):
		}
		n.mu.Lock()
		if n.role == Leader {
			for _, peer := range n.cfg.Peers {
				if peer != n.cfg.ID && n.inflight[peer] {
					n.wg.Add(1)
					go n.heartbeat(peer, n.currentTerm)
				}
			}
			n.broadcast()
		}
		n.mu.Unlock()
	}
}

// heartbeat sends an empty AppendEntries to a follower whose replication
// request is still outstanding. Without it, one lost message would silence
// the leader for a whole RPC timeout, long enough for the follower to start
// an election. Only the term in the reply is used: progress is tracked by
// the replicate goroutine alone.
func (n *Node) heartbeat(peer transport.NodeID, term uint64) {
	defer n.wg.Done()
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	// Any index of our log is a safe consistency point for an empty request:
	// if the follower matches there it truncates nothing, and if it does
	// not it rejects, having still reset its election timer.
	prev := n.matchIndex[peer]
	if prev < n.firstIndex() {
		prev = n.firstIndex()
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	reply, err := transport.Invoke[AppendEntriesArgs, AppendEntriesReply](ctx, n.tr, peer, MethodAppendEntries, args)
	cancel()
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, "")
		n.persist()
	}
}

// broadcast makes sure a replication goroutine is running for every
// follower. mu must be held.
func (n *Node) broadcast() {
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID || n.inflight[peer] {
			continue
		}
		n.inflight[peer] = true
		n.wg.Add(1)
		go n.replicate(peer, n.currentTerm)
	}
}

// replicate brings one follower up to date and then exits. At most one runs
// per follower, so replies are processed in the order requests were sent.
// It always sends at least one AppendEntries, which doubles as the heartbeat.
func (n *Node) replicate(peer transport.NodeID, term uint64) {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		if n.stopped || n.role != Leader || n.currentTerm != term {
			n.inflight[peer] = false
			n.mu.Unlock()
			return
		}

		next := n.nextIndex[peer]
		if next <= n.firstIndex() {
			// The entries this follower needs were compacted away (§7).
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderID:          n.cfg.ID,
				LastIncludedIndex: n.firstIndex(),
				LastIncludedTerm:  n.entries[0].Term,
				Data:              n.snapshot,
			}
			n.mu.Unlock()
			if !n.sendSnapshot(peer, term, args) {
				return
			}
			continue
		}

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     n.cfg.ID,
			PrevLogIndex: next - 1,
			PrevLogTerm:  n.termAt(next - 1),
			Entries:      n.entriesFrom(next, n.cfg.MaxEntriesPerAppend),
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
		reply, err := transport.Invoke[AppendEntriesArgs, AppendEntriesReply](ctx, n.tr, peer, MethodAppendEntries, args)
		cancel()

		n.mu.Lock()
		if err != nil {
			// Unreachable or slow; the next heartbeat tick retries.
			n.inflight[peer] = false
			n.mu.Unlock()
			return
		}
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term, "")
			n.persist()
		}
		if n.role != Leader || n.currentTerm != term {
			n.inflight[peer] = false
			n.mu.Unlock()
			return
		}

		if !reply.Success {
			n.nextIndex[peer] = n.nextIndexAfterReject(args, reply)
			n.mu.Unlock()
			continue
		}

		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndex()

		if n.nextIndex[peer] > n.lastIndex() {
			// Caught up. Clearing inflight in the same critical section as
			// this check means a concurrent Propose cannot be missed.
			n.inflight[peer] = false
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}

// nextIndexAfterReject picks where to retry after a follower rejected an
// AppendEntries, using its conflict hints to skip a whole term at a time
// instead of one entry (§5.3). The result is always lower than before, so
// the search terminates. mu must be held.
func (n *Node) nextIndexAfterReject(args *AppendEntriesArgs, reply *AppendEntriesReply) uint64 {
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		// If we also have entries from the follower's conflicting term, the
		// logs agree up to our last entry of that term at the earliest.
		i := args.PrevLogIndex
		if i > n.lastIndex() {
			i = n.lastIndex()
		}
		for ; i > n.firstIndex() && n.termAt(i) >= reply.ConflictTerm; i-- {
			if n.termAt(i) == reply.ConflictTerm {
				next = i + 1
				break
			}
		}
	}
	if next > args.PrevLogIndex {
		next = args.PrevLogIndex
	}
	if next < 1 {
		next = 1
	}
	return next
}

// advanceCommitIndex moves commitIndex to the highest index stored on a
// majority, provided that entry is from the current term (§5.3, §5.4.2).
// mu must be held.
func (n *Node) advanceCommitIndex() {
	if n.role != Leader {
		return
	}
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	majority := matches[n.quorum()-1]

	// Never commit an entry from an earlier term by counting replicas: it
	// could still be overwritten (Figure 8). It becomes committed indirectly
	// once an entry from this term is.
	if majority > n.commitIndex && n.termAt(majority) == n.currentTerm {
		n.commitIndex = majority
		n.applyCond.Broadcast()
	}
}

func (n *Node) handleAppendEntries(ctx context.Context, from transport.NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, errStopped
	}

	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	// A valid leader for this term or a later one: follow it. This also
	// makes a candidate in the same term stand down.
	dirty := args.Term > n.currentTerm
	n.becomeFollower(args.Term, args.LeaderID)
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	entries := args.Entries
	switch {
	case args.PrevLogIndex < n.firstIndex():
		// Everything through our snapshot is committed and so identical to
		// the leader's; skip the part of the request it already covers.
		skip := n.firstIndex() - args.PrevLogIndex
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}

	case args.PrevLogIndex > n.lastIndex():
		reply.ConflictIndex = n.lastIndex() + 1
		if dirty {
			n.persist()
		}
		return reply, nil

	case n.termAt(args.PrevLogIndex) != args.PrevLogTerm:
		// Consistency check failed (§5.3). Report the conflicting term and
		// the first index we hold for it.
		conflictTerm := n.termAt(args.PrevLogIndex)
		first := args.PrevLogIndex
		for first > n.firstIndex()+1 && n.termAt(first-1) == conflictTerm {
			first--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = first
		if dirty {
			n.persist()
		}
		return reply, nil
	}

	// Append anything new. Entries that already match are left alone: this
	// request may be a delayed duplicate, and truncating on it would delete
	// entries a newer request already added.
	for i, e := range entries {
		if e.Index <= n.lastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue
			}
			// Same index, different term: drop it and all that follow.
			n.entries = n.entries[:e.Index-n.firstIndex()]
		}
		n.entries = append(n.entries, entries[i:]...)
		n.markDirty(e.Index)
		dirty = true
		break
	}
	if dirty {
		n.persist()
	}

	// Only entries known to match the leader's log may be committed, hence
	// the bound by the last entry this request covered, not by our last entry.
	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if c := min(args.LeaderCommit, lastNew); c > n.commitIndex {
		n.commitIndex = c
		n.applyCond.Broadcast()
	}

	reply.Success = true
	return reply, nil
}

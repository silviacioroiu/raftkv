package raft

import (
	"context"

	"github.com/silviacioroiu/raftkv/transport"
)

const maxApplyBatch = 1024

// applyLoop is the only goroutine that touches the state machine. It feeds
// it committed entries in order, hands it snapshots received from the
// leader, and takes snapshots for log compaction.
func (n *Node) applyLoop() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && !n.restorePending && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}

		if n.restorePending {
			data, index := n.snapshot, n.firstIndex()
			n.restorePending = false
			n.mu.Unlock()
			n.fsm.Restore(data)
			n.mu.Lock()
			if n.lastApplied < index {
				n.lastApplied = index
			}
			continue
		}

		count := n.commitIndex - n.lastApplied
		if count > maxApplyBatch {
			count = maxApplyBatch
		}
		batch := n.entriesFrom(n.lastApplied+1, int(count))
		n.mu.Unlock()
		for _, e := range batch {
			n.fsm.Apply(e)
		}
		n.mu.Lock()
		n.lastApplied = batch[len(batch)-1].Index
		n.maybeCompact()
	}
}

// maybeCompact snapshots the state machine and discards the log prefix it
// covers, once enough entries have accumulated (§7). It must be called from
// the apply goroutine, so that the state machine is exactly at lastApplied.
// mu must be held; it is released while the snapshot is taken.
func (n *Node) maybeCompact() {
	if n.cfg.SnapshotThreshold == 0 || n.restorePending || n.lastApplied <= n.firstIndex() {
		return
	}
	if n.lastApplied-n.firstIndex() < n.cfg.SnapshotThreshold {
		return
	}
	index := n.lastApplied
	term := n.termAt(index)

	n.mu.Unlock()
	data := n.fsm.Snapshot()
	n.mu.Lock()

	if index <= n.firstIndex() {
		return // a snapshot from the leader overtook this one
	}
	n.truncateBefore(index, term)
	n.snapshot = data
	n.persistWithSnapshot()
	n.log.Info("compacted log", "through", index)
}

// sendSnapshot ships the leader's snapshot to a follower that is too far
// behind for log replication. It reports whether the replicate loop should
// continue; when it returns false it has already cleared inflight.
func (n *Node) sendSnapshot(peer transport.NodeID, term uint64, args *InstallSnapshotArgs) bool {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.SnapshotRPCTimeout)
	reply, err := transport.Invoke[InstallSnapshotArgs, InstallSnapshotReply](ctx, n.tr, peer, MethodInstallSnapshot, args)
	cancel()

	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil {
		n.inflight[peer] = false
		return false
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, "")
		n.persist()
	}
	if n.role != Leader || n.currentTerm != term {
		n.inflight[peer] = false
		return false
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1
	return true
}

func (n *Node) handleInstallSnapshot(ctx context.Context, from transport.NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, errStopped
	}

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	dirty := args.Term > n.currentTerm
	n.becomeFollower(args.Term, args.LeaderID)
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	if args.LastIncludedIndex <= n.commitIndex {
		// A delayed or duplicate snapshot: we already have everything in it,
		// and installing it would roll the state machine backwards.
		if dirty {
			n.persist()
		}
		return reply, nil
	}

	// Keeps any entries after the snapshot point that are consistent with
	// it, and discards the whole log otherwise (§7).
	n.truncateBefore(args.LastIncludedIndex, args.LastIncludedTerm)
	n.snapshot = args.Data
	n.commitIndex = args.LastIncludedIndex
	n.restorePending = true
	n.persistWithSnapshot()
	n.applyCond.Broadcast()
	return reply, nil
}

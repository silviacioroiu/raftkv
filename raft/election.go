package raft

import (
	"context"
	"errors"
	"time"

	"github.com/silviacioroiu/raftkv/transport"
)

var errStopped = errors.New("raft: node stopped")

// quorum is the smallest majority of the cluster.
func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

// electionLoop starts an election whenever the election timeout elapses
// without contact from a leader or a granted vote (§5.2).
func (n *Node) electionLoop() {
	defer n.wg.Done()
	tick := n.cfg.ElectionTimeoutMin / 10
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.clk.After(tick):
		}
		n.mu.Lock()
		if n.role != Leader && n.clk.Now().Sub(n.lastHeard) >= n.electionTimeout {
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// startElection makes this node a candidate for a new term and requests
// votes from every peer in parallel. mu must be held.
func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	// A fresh random timeout for every attempt is what resolves split votes.
	n.resetElectionTimer()
	n.persist()

	term := n.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.termAt(n.lastIndex()),
	}
	n.log.Info("starting election", "term", term)

	votes := 1 // our own; guarded by mu
	if votes >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.wg.Add(1)
		go func(peer transport.NodeID) {
			defer n.wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
			reply, err := transport.Invoke[RequestVoteArgs, RequestVoteReply](ctx, n.tr, peer, MethodRequestVote, args)
			cancel()
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term, "")
				n.persist()
				return
			}
			// Ignore replies to an election we are no longer running.
			if n.role != Candidate || n.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes == n.quorum() {
				n.becomeLeader()
			}
		}(peer)
	}
}

// becomeLeader takes over as leader for the current term. mu must be held.
func (n *Node) becomeLeader() {
	n.log.Info("became leader", "term", n.currentTerm)
	n.role = Leader
	n.leaderID = n.cfg.ID
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}

	// A leader may only commit entries from its own term by counting replicas
	// (§5.4.2, Figure 8). Appending a no-op right away gives it such an entry,
	// which in turn commits everything before it (§8).
	e := Entry{Index: n.lastIndex() + 1, Term: n.currentTerm, NoOp: true}
	n.entries = append(n.entries, e)
	n.matchIndex[n.cfg.ID] = e.Index
	n.markDirty(e.Index)
	n.persist()

	n.advanceCommitIndex()
	n.broadcast()
}

func (n *Node) handleRequestVote(ctx context.Context, from transport.NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, errStopped
	}

	dirty := false
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, "")
		dirty = true
	}
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}

	// Election restriction (§5.4.1): only vote for a candidate whose log is
	// at least as up to date as ours, so that a winner is guaranteed to hold
	// every committed entry.
	lastIndex := n.lastIndex()
	lastTerm := n.termAt(lastIndex)
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.resetElectionTimer()
		reply.VoteGranted = true
		dirty = true
	}
	if dirty {
		// The vote must be on disk before the candidate hears about it, or a
		// restart could let us vote twice in the same term.
		n.persist()
	}
	return reply, nil
}

# Design questions

Questions a reviewer might ask about each part of the system, with the place
in the code or the paper that answers each one.

## 1. Transport and simulated network

1. `Transport.Call` can return an error even though the remote handler ran. Why
   can't the transport hide that, and what does it force every caller to do?
   (`transport/transport.go`, `TestOneWayBlockLosesReply`)
2. The simulator takes a seed, yet a failing test is not exactly reproducible
   from it. What is reproducible, what is not, and what would full determinism
   cost? (`transport/simnet/simnet.go`, package comment and `plan`)
3. Why does the simulator copy every payload instead of passing the slice
   through, when everything runs in one process? (`deliver`)

## 2. Leader election

1. Why are election timeouts randomised, and why must each node reseed
   differently? What happens if all nodes pick the same timeout?
   (`resetElectionTimer`, and the seed comment in `raft/harness_test.go`)
2. A node votes, crashes before the vote reaches disk, restarts and gets
   another RequestVote for the same term. What goes wrong?
   (`handleRequestVote`, the `persist` before returning)
3. What is the election restriction, and which safety property depends on it?
   (`handleRequestVote`, `upToDate`; paper §5.4.1)

## 3. Log replication

1. Why may a leader not commit an entry from an earlier term just because it
   is stored on a majority? Walk through Figure 8. (`advanceCommitIndex`)
2. Why does a new leader append a no-op entry immediately? (`becomeLeader`)
3. A follower receives an AppendEntries whose entries it already has. Why must
   it not truncate its log, and how could that request arrive at all?
   (`handleAppendEntries`, the append loop)

## 4. Persistence

1. Which three pieces of state must be durable, and for each, what breaks if
   it is lost? Why are `commitIndex` and `lastApplied` not among them?
   (`walRecord`, paper Figure 2)
2. The first version rewrote one state file per write and was replaced with a
   write-ahead log. What was measured, and why was the old design slow?
   (`storage/storage.go`; README, "Persistence")
3. A crash cuts an append in half. How does the node notice on restart, and
   why is it safe to simply drop the partial record? (`parse`, `Load`)

## 5. Snapshots

1. Compaction writes the snapshot before rewriting the log. What state does a
   crash between the two leave, and how is it repaired? What would the
   opposite order risk? (`File.Compact`, `Node.load`)
2. Why does a follower ignore an InstallSnapshot whose index is at or below
   its commit index? (`handleInstallSnapshot`)
3. Only one goroutine ever touches the state machine. Which bugs does that
   rule out, and how does a snapshot from the leader get applied given that
   rule? (`applyLoop`, `restorePending`)

## 6. Key-value service

1. A client sends an Append, the leader commits it and crashes before
   replying. The client retries on the new leader. Why is the value not
   appended twice? (`Server.execute`, the session table)
2. Why is the session table part of the snapshot? (`Server.Snapshot`)
3. Reads go through the log like writes. What would break if the leader
   answered a Get from its local map, and what are the two standard ways to
   make reads cheaper without breaking it? (README, "Checking the checker";
   paper §8)

## 7. Chaos testing and linearizability

1. Define linearizability in one sentence, and say how it differs from
   serializability.
2. The checker was run against two deliberately broken builds. What were the
   bugs, and why did the stale-read bug go undetected until read-only clients
   were added? (`chaos/chaos_test.go`, `numReaders`)
3. Every client in the chaos tests retries until it gets an answer. Why does
   that matter for the recorded history, and what would you need to change to
   check a history containing operations with unknown outcomes?

## 8. Benchmarks

1. All nodes ran on one laptop. Which of the reported numbers would change
   most on three separate machines, and in which direction?
2. Throughput rises with the number of clients while single-client latency is
   dominated by one disk sync. Explain both with group commit.
   (`Server.batchLoop`, `Node.ProposeBatch`)
3. Failover takes roughly one election timeout. What is the trade-off in
   lowering the timeout, and what bounds it from below? (paper §5.6, timing
   requirement)

## 9. The whole system

1. During a network partition, which side keeps serving writes and why? Where
   does this system sit in CAP terms?
2. Name three things this implementation lacks that etcd has, and what each
   would take to add. (README, "Known limitations")
3. What changes if a node's disk silently loses acknowledged writes, and
   would Raft still be safe?

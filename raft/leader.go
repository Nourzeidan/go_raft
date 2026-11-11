package raft

import "time"

// caller holds rf.mu, so no Lock() here
func (rf *Raft) onBecomeLeaderLocked() {

	// if we are already leader, do nothing
	if rf.state == Leader {
		return
	}

	// after election
	rf.state = Leader // set state to leader
	lastLogIndex, _ := rf.lastLogIndexTerm() // get last log index and term
	rf.nextIndex = make([]int, len(rf.peers)) // initialize nextIndex for each peer
	rf.matchIndex = make([]int, len(rf.peers)) // initialize matchIndex for each peer

	// nextIndex for each peer is set to last log index + 1 - assuming everyone is up to date. 
	// If a peer is behind, it will be adjusted in replicateToPeer.
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex[rf.me] = lastLogIndex // matchIndex for self is set to last log index

	// send initial heartbeats to everyone, without holding the lock
	go rf.sendHeartbeats(rf.currentTerm)
}

const heartbeatInterval = 100 * time.Millisecond

// runs in its own goroutine for as long as this server is leader of term.
// every heartbeatInterval it sends AppendEntries to every other peer.
func (rf *Raft) sendHeartbeats(term int) {
	for !rf.killed() {
		rf.mu.Lock() // acquire lock to check state and term
		// stop if we lost leadership, or became leader again in a newer term
		// (that newer term has its own heartbeat loop)
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// RPCs must be sent without the lock, each in its own goroutine
		// so one slow or disconnected peer doesn't delay the others
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.replicateToPeer(peer, term)
			}
		}
		time.Sleep(heartbeatInterval)
	}
}

// sends one AppendEntries to peer and handles the reply.
func (rf *Raft) replicateToPeer(peer int, term int) {
	rf.mu.Lock()

	// stop if we lost leadership, or became leader again in a newer term
	// (that newer term has its own heartbeat loop)
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	// copy the entries so later changes to rf.log can't affect this message
	entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries, // empty when the follower is up to date = heartbeat
		LeaderCommit: rf.commitIndex,
	}
	DPrintf("[S%d T%d L] send AE to S%d prev=%d n=%d commit=%d",
		rf.me, term, peer, prevLogIndex, len(entries), args.LeaderCommit)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return // lost message or unreachable peer: the next heartbeat will retry
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()// re-acquire lock before returning

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if rf.state != Leader || term != rf.currentTerm {
		return // stale reply from an old term, ignore it
	}
	
	if reply.Success {
		// the follower accepted our entries, update nextIndex and matchIndex
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] { 
			rf.matchIndex[peer] = newMatch
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.advanceCommitIndexLocked()
		return
	}

	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	if reply.ConflictTerm == -1 {
		// follower's log is too short
		rf.nextIndex[peer] = reply.ConflictIndex
	} else {
		// find our last entry with ConflictTerm
		last := -1
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				last = i
				break
			}
		}
		if last != -1 {
			rf.nextIndex[peer] = last + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1 // index 0 is the dummy entry
	}
}
// finds the highest index stored on a majority of servers and commits it.
// only entries from the current term are committed by counting (Figure 8);
// older entries get committed along with them.
func (rf *Raft) advanceCommitIndexLocked() {
	lastIndex, _ := rf.lastLogIndexTerm()

	for n := lastIndex; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue // Figure 8: don't commit old-term entries by counting
		}

		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			DPrintf("[S%d T%d L] commit up to %d", rf.me, rf.currentTerm, n)
			rf.signalApplyLocked()
			return
		}
	}
}

// runs on every server for its whole life.
// sends committed entries to the tester on applyCh, in order.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// 1. sleep while there is nothing new to apply
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}

		// 2. copy what needs sending, while holding the lock
		start := rf.lastApplied + 1
		end := rf.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Command: rf.log[i].Command})
		}

		// 3. send WITHOUT the lock
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
		rf.mu.Lock()

		// 4. remember how far we got
		rf.lastApplied = end
	}
}

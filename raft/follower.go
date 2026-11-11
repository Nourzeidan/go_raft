package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// reject a leader if it has a smaller term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// if we continue then this is a valid leader
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	lastIndex, _ := rf.lastLogIndexTerm()
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		DPrintf("[%d] AE reject: log is too short (prev=%d last=%d)",
			rf.me, args.PrevLogIndex, lastIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == conflictTerm {
			i--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = i
		DPrintf("[%d] AE reject: term mismatch (prev=%d term=%d)", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// append new entries
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= lastIndex && rf.log[index].Term != entry.Term {
			rf.log = rf.log[:index]
			lastIndex = index - 1
		}
		if index > lastIndex {
			rf.log = append(rf.log, entry)
			lastIndex = index
		}
	}

	// commit entries
	newCommit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
		rf.signalApplyLocked()
	}

	reply.Success = true
	DPrintf("[%d] AE success: prev=%d last=%d commit=%d", rf.me, args.PrevLogIndex, lastIndex, rf.commitIndex)

}

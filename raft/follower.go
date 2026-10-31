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
		return;
	}

	// if we continue then this is a valid leader
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	lastIndex, - = rf.lastLogIndexTerm()
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		DPrintf("[%d] AE reject: log is too short (prev=%d last=%d)",
		 rf.me, args.PrevLogIndex, lastIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = args.PrevLogIndex
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
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastIndex)
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	DPrintf("[%d] AE success: prev=%d last=%d commit=%d", rf.me, args.PrevLogIndex, lastIndex, rf.commitIndex)

}
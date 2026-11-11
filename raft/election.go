package raft

import (
	"math/rand"
	"time"
)

// handleRequestVote is the body of the RequestVote RPC handler
// (called from RequestVote in raft.go). Runs on the server being asked for its vote.
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// 1. candidate is from an old term: reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// 2. candidate is in a newer term: catch up (clears votedFor)
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	// 3. one vote per term (voting again for the same candidate is fine)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// 4. election restriction: candidate's log must be at least as up to date as ours
	myLastIndex, myLastTerm := rf.lastLogIndexTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)
	if !upToDate {
		return
	}

	// 5. grant the vote; reset the timer only when granting
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	reply.VoteGranted = true
	DPrintf("[S%d T%d] vote for S%d", rf.me, rf.currentTerm, args.CandidateId)
}

// ticker runs for the server's whole life and starts an election
// when no leader has been heard from for a random 300-600 ms.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// fresh random timeout every round, so peers don't keep colliding
		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		time.Sleep(timeout) // never sleep while holding the lock

		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeard) >= timeout {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
}

// startElectionLocked becomes a candidate and asks every peer for a vote.
// Caller holds rf.mu.
func (rf *Raft) startElectionLocked() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()

	term := rf.currentTerm
	lastIndex, lastTerm := rf.lastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	DPrintf("[S%d T%d] start election", rf.me, term)

	votes := 1 // our own vote; protected by rf.mu

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			// send WITHOUT the lock
			if !rf.sendRequestVote(peer, &args, &reply) {
				return // lost message; the election either succeeds without it or times out
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// someone is in a newer term: step down
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
			// this election is over (won, lost, or a newer term started): ignore
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if !reply.VoteGranted {
				return
			}

			votes++
			if votes > len(rf.peers)/2 {
				DPrintf("[S%d T%d] won election with %d votes", rf.me, term, votes)
				// do NOT set state = Leader here; onBecomeLeaderLocked does it
				rf.onBecomeLeaderLocked()
			}
		}(peer)
	}
}

package raft

// Student 3's own tests for the leader side.
// They fake a leader by hand, because elections (Student 1) aren't ready yet.
// Run with: go test -race -run Leader ./raft

import (
	"testing"
	"time"
)

// Fakes a leader by setting its fields directly, then checks Start.
// Needs nothing except Start, so it works right now.
func TestLeaderStartFake(t *testing.T) {
	cfg := make_config(t, 3, false)
	defer cfg.cleanup()

	leader := cfg.rafts[0]

	// make server 0 a leader by hand (normally onBecomeLeaderLocked does this)
	leader.mu.Lock()
	leader.state = Leader
	leader.matchIndex = make([]int, len(leader.peers))
	leader.nextIndex = make([]int, len(leader.peers))
	leader.mu.Unlock()

	// the leader should accept commands at indexes 1, 2, 3
	for want := 1; want <= 3; want++ {
		index, _, isLeader := leader.Start(want * 100)
		if !isLeader {
			t.Fatalf("leader.Start returned isLeader=false")
		}
		if index != want {
			t.Fatalf("got index %v, expected %v", index, want)
		}
	}

	// a follower must refuse
	_, _, isLeader := cfg.rafts[1].Start(999)
	if isLeader {
		t.Fatalf("follower.Start returned isLeader=true")
	}

	// the follower's log must not have changed
	cfg.rafts[1].mu.Lock()
	n := len(cfg.rafts[1].log)
	cfg.rafts[1].mu.Unlock()
	if n != 1 {
		t.Fatalf("follower log has length %v, expected 1 (just the dummy)", n)
	}
}

// Same idea, but uses the real onBecomeLeaderLocked.
func TestLeaderBecomeLeader(t *testing.T) {
	cfg := make_config(t, 3, false)
	defer cfg.cleanup()

	leader := cfg.rafts[0]

	leader.mu.Lock()
	leader.onBecomeLeaderLocked()
	leader.mu.Unlock()

	index, _, isLeader := leader.Start(100)
	if !isLeader || index != 1 {
		t.Fatalf("got index=%v isLeader=%v, expected 1 true", index, isLeader)
	}

	// let the heartbeat loop run a few rounds (100 ms each),
	// so bugs in sendHeartbeats / replicateToPeer actually show up
	time.Sleep(350 * time.Millisecond)

	leader.mu.Lock()
	stillLeader := leader.state == Leader
	leader.mu.Unlock()
	if !stillLeader {
		t.Fatalf("leader stopped being leader during heartbeats")
	}
}

// Full chain with a fake leader: Start -> replicate -> commit -> apply.
// cfg.one only succeeds if all 3 servers apply the command on applyCh.
func TestLeaderOneFake(t *testing.T) {
	cfg := make_config(t, 3, false)
	defer cfg.cleanup()

	leader := cfg.rafts[0]
	leader.mu.Lock()
	leader.onBecomeLeaderLocked()
	leader.mu.Unlock()

	for want := 1; want <= 3; want++ {
		index := cfg.one(want*100, 3)
		if index != want {
			t.Fatalf("got index %v, expected %v", index, want)
		}
	}
}
package raft


type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int 
	Success bool 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// Reject heartbeats from a stale leader.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// args.Term >= currentTerm: this is a valid current leader. Any peer
	// (follower or candidate) must recognize it and reset its election timer.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = FOLLOWER
	rf.leaderHeartBeat = true
	reply.Term = rf.currentTerm
	reply.Success = true
}
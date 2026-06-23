package raft
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER 
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	

	term := args.Term

	
	if term < rf.currentTerm {
		reply.VoteGranted = false 
		reply.Term = rf.currentTerm
	} else {

		if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
			args.LastLogTerm >= rf.log[len(rf.log)-1].term && 
			args.LastLogIndex >= len(rf.log) - 1 {
			
			
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER
			rf.leaderHeartBeat = true 
			
			reply.VoteGranted = true
		} else {
			
			reply.VoteGranted = false 
		}
		
		reply.Term = rf.currentTerm

	}
}
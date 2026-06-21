package raft

// TODO: has the issue of not returning when collecting values for votes, can consider to use timeouts first

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const FOLLOWER = 0; const CANDIDATE = 1; const LEADER = 2
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// stable states
	currentTerm int
	// -1 if voted no one
	votedFor int
	log []*Log

	// volatile states on servers 
	commitIndex int 
	lastApplied int 

	

	// volatile states on leaders
	nextIndex []int
	matchIndex []int

	// leader state
	// 0 follower, 1 candidate, 2 leader
	state int 
	leaderHeartBeat bool
}

type Log struct {
	command string 
	term int 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


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
		
		reply.Term = term
		
	}
	if !reply.VoteGranted {
		println(rf.me, "not vote to", args.CandidateId, "because", "rf.votedFor", rf.votedFor, "args.Term", args.Term, "rf.currentTerm", rf.currentTerm, "args.LastLogIndex", args.LastLogIndex, "len(rf.log)-1", len(rf.log) - 1)
	} else {
		println(rf.me, "vote to", args.CandidateId, reply.VoteGranted)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
	if args.Term >= rf.currentTerm {
		rf.leaderHeartBeat = true 
	}
	
	if args.Term > rf.currentTerm { 
		
		rf.currentTerm = args.Term
		rf.state = FOLLOWER 
		rf.votedFor = -1
		return
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	sleepRandom(rand.Int63() % 300)
	
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == FOLLOWER && rf.leaderHeartBeat == false {
			rf.state = CANDIDATE
		}

		if rf.state == CANDIDATE {
			votes := electWithUnlock(rf)
			currTerm := rf.currentTerm
			
			if votes > len(rf.peers) / 2 {
				rf.mu.Lock()
				if rf.state == CANDIDATE && currTerm == rf.currentTerm{
    				rf.state = LEADER
					rf.leaderHeartBeat = false
				}
				rf.mu.Unlock()

				isLeader := rf.sendHeartbeat()
				
				if isLeader {

					go func(rf *Raft) {
						for !rf.killed() {
							
							
							isLeader := rf.sendHeartbeat()
							if !isLeader {
								break
							}
							
							sleepRandom(40 + (rand.Int63() % 30))
						}
					
					} (rf)
				}

				 
			
			}
		} else {
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		if rf.state == FOLLOWER {
			rf.leaderHeartBeat = false
		}
		
		rf.mu.Unlock()
		
		
		sleepRandom(300 + (rand.Int63() % 300))
	}
}

func (rf *Raft) sendHeartbeat() bool {
	doneChan := make(chan bool, len(rf.peers))
	isLeader := true
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.mu.Lock()
		if rf.state != LEADER {
			isLeader = false
		}
		currTerm := rf.currentTerm
		rf.mu.Unlock()
		go func(peer int, doneChan chan bool) {		
			reply := &AppendEntriesReply{}

			rf.mu.Lock()
			if rf.state != LEADER {
				isLeader = false
				doneChan <- true
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(peer, &AppendEntriesArgs{Term: currTerm}, reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm && ok {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.mu.Unlock()
				doneChan <- true
				return
			} 
			rf.mu.Unlock()
			doneChan <- true
		} (peer, doneChan)
	}
	for range (len(rf.peers) - 1) {
		<- doneChan
	}

	return isLeader
}

// around request vote locks may be needed to avoid split brain

func electWithUnlock(rf *Raft) int {
	votes := 1

	rf.currentTerm += 1
	rf.votedFor = rf.me
	currTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].term
	rf.mu.Unlock()
	// ask for votes

	voteChan := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := RequestVoteArgs{
			Term:         currTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}


		var reply RequestVoteReply

		go func (voteChan chan bool)  {
			ok := rf.sendRequestVote(i, &arg, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || currTerm != rf.currentTerm {
				voteChan <- false
			} else if reply.Term > currTerm {
				rf.state = FOLLOWER
				voteChan <- false
			} else if reply.VoteGranted {
				voteChan <- true
			} else {
				voteChan <- false 
			}
		}(voteChan)
	}
	for range (len(rf.peers) - 1) {
		if (<- voteChan) { votes++ }
		if votes > len(rf.peers) / 2 {break}
	}


	return votes
}

func sleepRandom(ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1 
	rf.log = make([]*Log, 0)
	rf.log = append(rf.log, &Log{"", 1})
	
	rf.commitIndex = 0 
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	lastLogIndex := len(rf.log) - 1

	for i := range peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "crypto/aes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	follower  = 0
	candidate = 1
	leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	state       int
	lastRecTime int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
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
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

type RequestAppendArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendReply struct {
	// Your data here (2A, 2B).
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("raft:[%v] term:%v accept vote", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	reply.VoteGranted = false
	rf.mu.Lock()
	rf.lastRecTime = time.Now().UnixNano() / 1e6
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DPrintf("raft:[%v] term:%v change state to follower when vote term > curterm,args term=%v", rf.me, rf.currentTerm, args.Term)
		rf.state = follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.state = follower
			DPrintf("raft:[%v] term:%v change state to followerwhen vote term == curterm", rf.me, rf.currentTerm)
			reply.VoteGranted = true
		}
	}
	reply.Term = rf.currentTerm
	defer rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	DPrintf("raft:[%v] term:%v accept append entries from raft%v term%v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if rf.currentTerm > args.Term {
		DPrintf("raft:[%v] term:%v find old entries, my term=%v, the old term=%v", rf.me, rf.currentTerm, rf.currentTerm, args.Term)
		reply.Success = false
	} else {
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		DPrintf("raft:[%v] term:%v change state to follower 199", rf.me, rf.currentTerm)
		rf.state = follower
		rf.lastRecTime = time.Now().UnixNano() / 1e6
		reply.Success = true
	}
	reply.Term = rf.currentTerm
	defer rf.mu.Unlock()
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

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAllEntries(curTerm int) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		// state := rf.state
		if rf.state != leader || curTerm < rf.currentTerm {
			DPrintf("rf %v is not leader, state=%v", rf.me, rf.state)
			rf.mu.Unlock()
			return
		}
		DPrintf("raft:[%v] term:%v is leader, send entries to all servers, curTerm=%v-------------------------", rf.me, rf.currentTerm, curTerm)
		rf.lastRecTime = time.Now().UnixNano() / 1e6
		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			args := RequestAppendArgs{rf.currentTerm, rf.me}
			reply := RequestAppendReply{}
			go func(idx int) {
				DPrintf("raft:[%v] term:%v send heartbeat to raft=%v", rf.me, rf.currentTerm, idx)
				rf.sendAppendEntries(idx, &args, &reply)
				rf.mu.Lock()
				if (!reply.Success) && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					DPrintf("raft:[%v] term:%v change state to follower when send the heartbeat find big term, send to raft=%v", rf.me, rf.currentTerm, idx)
					rf.state = follower
				}
				rf.mu.Unlock()
			}(idx)
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

func (rf *Raft) startElect() {
	rf.mu.Lock()
	rf.state = candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	DPrintf("kaft:[%v] add term, now term=%v", rf.me, rf.currentTerm)
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	var temMu sync.Mutex //互斥访问count
	count := 1
	DPrintf("raft:[%v] term:%v, start get other vote", rf.me, rf.currentTerm)
	for a, _ := range rf.peers {
		if a == rf.me {
			continue
		}
		args := RequestVoteArgs{curTerm, rf.me}
		reply := RequestVoteReply{}
		go func(idx int, args *RequestVoteArgs, reply *RequestVoteReply) {
			DPrintf("raft:[%v] term:%v, send vote to %v, args term=%v", rf.me, rf.currentTerm, idx, args.Term)
			rf.sendRequestVote(idx, args, reply)
			// DPrintf("raft:%v, vote raft:%v, VoteGranted:%v", rf.me, idx, reply.VoteGranted)
			if reply.VoteGranted {
				temMu.Lock()
				count++
				temMu.Unlock()
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					DPrintf("raft:[%v] term:%v change state to follower when elect", rf.me, rf.currentTerm)
					rf.state = follower
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
			temMu.Lock()
			nowCount := count
			temMu.Unlock()
			rf.mu.Lock()
			if nowCount > len(rf.peers)/2 && curTerm == rf.currentTerm {
				if rf.state == candidate {
					rf.state = leader
					DPrintf("raft:[%v] term:%v become leader!! time=%v", rf.me, rf.currentTerm, time.Now().UnixNano()/1e6)
					go rf.sendAllEntries(rf.currentTerm)
				}
			}
			rf.mu.Unlock()
		}(a, &args, &reply)
	}
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

	// Your code here (2B).

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
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rand.Seed(time.Now().UnixNano())
		ms := 0 + (rand.Int63() % 600)
		// DPrintf("raft:%v,sleep time=%v", rf.me, time.Duration(ms)*time.Millisecond)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// DPrintf("raft:%v,sleep time=%v", rf.me, time.Duration(ms)*time.Millisecond)

		rf.mu.Lock()
		lastTime := rf.lastRecTime
		rf.mu.Unlock()
		// DPrintf("lastTime:%v,now:%v,time distance:%v", lastTime, time.Now().UnixNano()/1e6, time.Now().UnixNano()/1e6-lastTime)
		if time.Now().UnixNano()/1e6-lastTime > 500 && rf.state != leader {
			DPrintf("raft:[%v] term:%v, time out, start a new election,time=%v", rf.me, rf.currentTerm, time.Now().UnixNano()/1e6)
			rf.startElect()
			// ms := 1000 + (rand.Int63() % 500)
			// time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = follower
	rf.lastRecTime = time.Now().UnixNano() / 1e6
	DPrintf("success create raft: [%v] term:%v", me, rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

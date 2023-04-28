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
	"bytes"
	// "crypto/aes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh *chan ApplyMsg

	//persistent state
	currentTerm int
	votedFor    int
	log         []Entry

	//Volatile state
	state        int
	lastRecTime  int64
	commitIndex  int //当前日志待提交的序号
	lastApplied  int //已提交日志的序号
	lastLogIndex int //最后一个日志序号

	//Reinitialized after election
	nextIndex  []int
	matchIndex []int
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.state)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	DPrintf("raft:[%v] store data to persist success", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry
	var state int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&state) != nil {
		DPrintf("raft:[%v] restore data from persist decode failed!", rf.me)
	} else {
		DPrintf("raft:[%v] restore data from persist success!", rf.me)
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.state = state
		rf.lastLogIndex = len(rf.log) - 1
		rf.mu.Unlock()
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type RequestAppendReply struct {
	// Your data here (2A, 2B).
	Term       int
	Success    bool
	FailReason string
	UpdateNext int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft:[%v] term:%v accept vote request from raft%v", rf.me, rf.currentTerm, args.CandidateId)
	reply.VoteGranted = false
	rf.lastRecTime = time.Now().UnixNano() / 1e6
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.state = follower
			rf.currentTerm = args.Term
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			curLogIdx := rf.lastLogIndex
			curLogTerm := rf.log[curLogIdx].Term

			if (args.LastLogTerm > curLogTerm) || (args.LastLogTerm == curLogTerm && args.LastLogIndex >= curLogIdx) {
				rf.votedFor = args.CandidateId
				rf.state = follower
				rf.currentTerm = args.Term
				reply.VoteGranted = true
				DPrintf("raft:[%v] term:%v agree the vote from raft%v", rf.me, rf.currentTerm, args.CandidateId)
			}
		}
		rf.currentTerm = args.Term
		rf.persist()
	}

	// if args.Term > rf.currentTerm {
	// 	rf.currentTerm = args.Term
	// 	DPrintf("raft:[%v] term:%v change state to follower when vote term > curterm,args term=%v", rf.me, rf.currentTerm, args.Term)
	// 	rf.state = follower
	// 	rf.votedFor = args.CandidateId
	// 	reply.VoteGranted = true
	// } else if args.Term == rf.currentTerm {
	// 	if rf.votedFor == -1 {
	// 		rf.votedFor = args.CandidateId
	// 		rf.state = follower
	// 		DPrintf("raft:[%v] term:%v change state to followerwhen vote term == curterm", rf.me, rf.currentTerm)
	// 		reply.VoteGranted = true
	// 	}
	// }
	reply.Term = rf.currentTerm
}

func (rf *Raft) commitEntry() {
	if rf.commitIndex > rf.lastApplied {
		DPrintf("raft:[%v] term:%v, commit entry from %v to %v", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			tmpMsg := ApplyMsg{true, rf.log[i].Command, i, false, nil, -1, -1}
			*rf.applyCh <- tmpMsg
			rf.lastApplied++
		}
	}

	// targetCommit := rf.lastApplied
	// DPrintf("raft:[%v] term:%v,rf.lastApplied=%v, leaderCommit=%v", rf.me, rf.currentTerm, rf.lastApplied, leaderCommit)
	// if targetCommit > leaderCommit {
	// 	targetCommit = leaderCommit
	// }
	// if rf.commitIndex+1 >= targetCommit {
	// 	DPrintf("raft:[%v] term:%v, commit entry from %v to %v", rf.me, rf.currentTerm, rf.commitIndex+1, targetCommit)
	// }
	// for i := rf.commitIndex + 1; i <= targetCommit; i++ {
	// 	tmpMsg := ApplyMsg{true, rf.log[i].Command, i, false, nil, -1, -1}
	// 	*rf.applyCh <- tmpMsg
	// 	rf.commitIndex++
	// }
}

func min(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("raft:[%v] term:%v accept append entries from raft%v term%v", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	//旧消息，拒绝
	if rf.currentTerm > args.Term {
		DPrintf("raft:[%v] term:%v find old entries, my term=%v, the old term=%v", rf.me, rf.currentTerm, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FailReason = "oldterm"
		return
	}
	//如果为心跳消息
	if args.Entries == nil {
		DPrintf("raft:[%v] term:%v, accept heart beat from leader raft %v", rf.me, rf.currentTerm, args.LeaderId)
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		// DPrintf("raft:[%v] term:%v change state to follower 199", rf.me, rf.currentTerm)
		rf.state = follower
		rf.lastRecTime = time.Now().UnixNano() / 1e6
		//为了防止收到心跳消息时的LeaderCommit比lastApplied小，从而导致commitIndex<lastApplied
		if args.LeaderCommit > rf.lastApplied {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
			rf.commitEntry() //心跳时 如果leadercommit有更新则需检测提交
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	//如果当前日志中不包含args中的上一个日志序号即term，返回false
	if args.PrevLogIndex >= 0 {
		//长度不够或term不匹配
		if rf.lastLogIndex < args.PrevLogIndex || (rf.lastLogIndex >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			if rf.lastLogIndex < args.PrevLogIndex {
				reply.UpdateNext = rf.lastLogIndex
			} else {
				reply.UpdateNext = args.PrevLogIndex - 1
			}
			reply.UpdateNext = 1
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.FailReason = "inconsistency"
			DPrintf("raft:[%v] term:%v, dont match the args PrevLogIndex or PrevLogTerm", rf.me, rf.currentTerm)
			return
		}
	}
	//如果新增日志序号存在但冲突则删除后面日志，序号不存在添加到末端
	ia := 0
	for i := args.PrevLogIndex + 1; i <= rf.lastLogIndex; i++ {
		if rf.log[i].Term != args.Entries[ia].Term {
			rf.log = rf.log[:i]
			break
		}
		ia++
	}
	if ia < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[ia:]...)
	}
	rf.lastLogIndex = len(rf.log) - 1
	DPrintf("raft:[%v] term:%v, add entries to %v success", rf.me, rf.currentTerm, rf.lastLogIndex)

	rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
	rf.commitEntry()
	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("raft:[%v] term:%v, apply entries to %v", rf.me, rf.currentTerm, rf.lastApplied)
	rf.persist()
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

func (rf *Raft) syncAllEntries() {
	DPrintf("raft:[%v] term:%v, start sync all entries to server ", rf.me, rf.currentTerm)
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			term := rf.currentTerm //防止重新被选为leader
			rf.mu.Unlock()
			for {
				rf.mu.Lock()
				if term != rf.currentTerm || rf.state != leader {
					rf.mu.Unlock()
					return
				}
				lastLogIndex := rf.lastLogIndex //防止被添加新的log
				rf.mu.Unlock()
				// DPrintf("raft:[%v] term:%v, lastLogIndex=%v rf.nextIndex[i]=%v ", rf.me, rf.currentTerm, lastLogIndex, rf.nextIndex[i])
				for lastLogIndex >= rf.nextIndex[i] && rf.nextIndex[i] > 0 {
					//传commitIndex还是rf.lastApplied还待测试
					rf.mu.Lock()
					args := RequestAppendArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, rf.log[rf.nextIndex[i]:], rf.commitIndex}
					reply := RequestAppendReply{} //{0, false, ""}
					lastLogIndex = rf.lastLogIndex
					DPrintf("raft:[%v] term:%v, sending sync entries%v~%v to raft%v !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", rf.me, rf.currentTerm, rf.nextIndex[i], rf.lastLogIndex, i)
					rf.mu.Unlock()

					rf.sendAppendEntries(i, &args, &reply)

					rf.mu.Lock()
					if rf.currentTerm != term || rf.state != leader { //当被新选举为leader时
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = lastLogIndex
						DPrintf("raft:[%v] term:%v, sync entries to raft%v success, nextIndex=%v", rf.me, rf.currentTerm, i, rf.nextIndex[i])
					} else {
						if reply.FailReason == "inconsistency" {
							rf.nextIndex[i]--
							rf.nextIndex[i] = reply.UpdateNext
							DPrintf("raft:[%v] term:%v, sync entries to raft%v fail, the reason is inconsistency, nextIndex=%v", rf.me, rf.currentTerm, i, rf.nextIndex[i])
						} else if reply.FailReason == "oldterm" {
							DPrintf("raft:[%v] term:%v, sync entries to raft%v fail, the reason is oldterm, nextIndex=%v", rf.me, rf.currentTerm, i, rf.nextIndex[i])
							rf.mu.Unlock()
							break
						} else {
							DPrintf("raft:[%v] term:%v, sync entries to raft%v fail, the reason is network , nextIndex=%v", rf.me, rf.currentTerm, i, rf.nextIndex[i])
						}
					}
					lastLogIndex = rf.lastLogIndex
					rf.mu.Unlock()
				}

				rf.mu.Lock()

				//排序取中值，按理说是可以的，但有些测试不通过
				// tmpMatchs := make([]int, len(rf.peers))
				// copy(tmpMatchs, rf.matchIndex)
				// sort.Ints(tmpMatchs)
				// N := tmpMatchs[len(rf.peers)/2]
				// if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
				// 	fmt.Printf("N=%v success\n", N)
				// 	rf.commitIndex = N
				// 	rf.commitEntry()
				// }

				//这种方法效率较低，后面还需用上面方法进行更改
				for N := rf.lastLogIndex; N > rf.commitIndex; N-- {
					count := 0
					for t := 0; t < len(rf.peers); t++ {
						if t == rf.me || rf.matchIndex[t] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						if rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							rf.commitEntry()
						}
						break
					}
				}
				rf.mu.Unlock()

				time.Sleep(time.Duration(100) * time.Microsecond)
			}

		}(idx)
	}
}

func (rf *Raft) startElect() {
	rf.mu.Lock()
	rf.state = candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	DPrintf("kaft:[%v] add term, now term=%v, start get other vote", rf.me, rf.currentTerm)
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	var temMu sync.Mutex //互斥访问count
	count := 1

	for a, _ := range rf.peers {
		if a == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			args := RequestVoteArgs{curTerm, rf.me, rf.lastLogIndex, rf.log[rf.lastLogIndex].Term}
			reply := RequestVoteReply{}
			DPrintf("raft:[%v] term:%v, send vote to %v, args term=%v", rf.me, rf.currentTerm, idx, args.Term)
			rf.mu.Unlock()
			rf.sendRequestVote(idx, &args, &reply)
			// DPrintf("raft:%v, vote raft:%v, VoteGranted:%v", rf.me, idx, reply.VoteGranted)
			if reply.VoteGranted {
				temMu.Lock()
				count++
				temMu.Unlock()
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					// DPrintf("raft:[%v] term:%v change state to follower when elect", rf.me, rf.currentTerm)
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
				if rf.state == candidate { //防止多个rpc返回后设置多次心跳
					rf.state = leader
					//每次选举成功后都对nextIndex和matchIndex数组初始化
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastLogIndex + 1
						rf.matchIndex[i] = 0
					}
					go rf.syncAllEntries()
					DPrintf("raft:[%v] term:%v become leader!!!!!!!!!!!!!!!!!!! ", rf.me, rf.currentTerm)
					go rf.sendAllHeartBeat(rf.currentTerm)
				}
			}
			rf.mu.Unlock()
		}(a)
	}
}

func (rf *Raft) sendAllHeartBeat(curTerm int) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		// state := rf.state
		if rf.state != leader || curTerm < rf.currentTerm {
			DPrintf("rf %v is not leader, state=, break heart beat%v", rf.me, rf.state)
			rf.mu.Unlock()
			return
		}
		DPrintf("raft:[%v] term:%v is leader, send heart beat to all servers-------------------------", rf.me, rf.currentTerm)
		rf.lastRecTime = time.Now().UnixNano() / 1e6
		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			//发送心跳时，如果当前日志还未同步完成，直接发送commitIndex会导致错误提交，因此sendCommitIndex=min(lastApplied,matchIndex[idx])
			go func(idx int) {
				rf.mu.Lock()
				sendCommitIndex := min(rf.lastApplied, rf.matchIndex[idx])
				args := RequestAppendArgs{rf.currentTerm, rf.me, -1, -1, nil, sendCommitIndex}
				reply := RequestAppendReply{}
				rf.mu.Unlock()

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

//被废弃，同步日志都使用syncAllEntries函数
// func (rf *Raft) sendAllEntries(logIdx int) {
// 	rf.mu.Lock()
// 	if rf.state != leader {
// 		rf.mu.Unlock()
// 		return
// 	}
// 	DPrintf("raft:[%v] term:%v, send entries to others service!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", rf.me, rf.currentTerm)

// 	rf.mu.Unlock()
// 	count := 1
// 	var tmpMu sync.Mutex
// 	tmpCond := sync.NewCond(&tmpMu)
// 	for idx := 0; idx < len(rf.peers); idx++ {
// 		if idx == rf.me {
// 			continue
// 		}
// 		args := RequestAppendArgs{rf.currentTerm, rf.me, rf.lastLogIndex - 1, rf.log[rf.lastLogIndex-1].Term, rf.log[logIdx : logIdx+1], rf.lastApplied}
// 		reply := RequestAppendReply{0, false, "",-1}
// 		go func(i int) {
// 			DPrintf("raft:[%v] term:%v send entries%v to raft=%v", rf.me, rf.currentTerm, logIdx, i)
// 			rf.sendAppendEntries(i, &args, &reply)
// 			if reply.Success {
// 				tmpMu.Lock()
// 				count++
// 				tmpMu.Unlock()
// 			}
// 			// else if (!reply.Success) && reply.Term > rf.currentTerm {
// 			// 	rf.currentTerm = reply.Term
// 			// 	DPrintf("raft:[%v] term:%v change state from leader to follower when send the entry find big term, send to raft=%v", rf.me, rf.currentTerm, idx)
// 			// 	rf.state = follower
// 			// }
// 			tmpCond.Broadcast()
// 		}(idx)
// 	}

// 	//当count数量超过一半时，提交当前log
// 	tmpMu.Lock()
// 	for count <= len(rf.peers)/2 && rf.state == leader {
// 		tmpCond.Wait()
// 	}
// 	rf.mu.Lock()
// 	// if rf.state == leader { //可能由于出现新leader导致提交失败
// 	DPrintf("raft:[%v] term:%v, as leader, the follower accept the entry more than half, so commit the entry%v, count=%v", rf.me, rf.currentTerm, logIdx, count)
// 	rf.commitIndex = logIdx
// 	rf.commitEntry()
// 	// }
// 	rf.mu.Unlock()
// }

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		rf.log = append(rf.log, Entry{command, rf.currentTerm})
		index = len(rf.log) - 1
		rf.lastLogIndex = index
		term = rf.currentTerm
		DPrintf("raft:[%v] term:%v, start a new command%v entry!!!!!!!!!!!!!!!!!!", rf.me, rf.currentTerm, command)
		rf.persist()
		// go rf.sendAllEntries(index)
		// go rf.syncAllEntries()
	} else {
		isLeader = false
	}
	DPrintf("raft:[%v] term:%v, start function: index=%v term=%v isLeader=%v", rf.me, rf.currentTerm, index, term, isLeader)

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

		rf.mu.Lock()
		lastTime := rf.lastRecTime
		// DPrintf("lastTime:%v,now:%v,time distance:%v", lastTime, time.Now().UnixNano()/1e6, time.Now().UnixNano()/1e6-lastTime)
		if time.Now().UnixNano()/1e6-lastTime > 500 && rf.state != leader {
			DPrintf("raft:[%v] term:%v, time out, start a new election,time=%v", rf.me, rf.currentTerm, time.Now().UnixNano()/1e6)
			go rf.startElect()
		}
		rf.mu.Unlock()
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

	rf.applyCh = &applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = follower
	rf.lastRecTime = time.Now().UnixNano() / 1e6
	rf.log = append(rf.log, Entry{"start log", 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	DPrintf("success create raft: [%v] term:%v", me, rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

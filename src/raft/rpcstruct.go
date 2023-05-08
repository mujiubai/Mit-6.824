package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //rpc发起者的term
	CandidateId  int //rpc发起者的序号
	LastLogIndex int //rpc发起者的最后一个日志的序号
	LastLogTerm  int //rpc发起者的最后一个日志的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //被调用者的当前term
	VoteGranted bool //被调用者是否同意投票给调用者
}

type RequestAppendArgs struct {
	// Your data here (2A, 2B).
	Term         int     //发起者当前term
	LeaderId     int     //发起者id
	PrevLogIndex int     //当前要添加日志的前一个日志位置
	PrevLogTerm  int     //当前要添加日志的前一个日志的term
	Entries      []Entry //要添加的日志
	LeaderCommit int     //发起者的commit大小，主要用于接受者提交日志
}

type RequestAppendReply struct {
	// Your data here (2A, 2B).
	Term       int    //接收方当前term
	Success    bool   //是否成功添加日志
	FailReason string //添加失败的原因，有点多余这参数，
	UpdateNext int    //添加失败，即日志冲突时，需要将leader的nextIndex更新为UpdateNext
}

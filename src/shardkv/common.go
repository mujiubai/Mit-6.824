package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrShutDown    = "ErrShutDown"
	ErrShardEmpty  = "ErrShardEmpty"
	ErrRepeatReq   = "ErrRepeatReq"
	ErrShardData   = "ErrShardData"
)

const (
	Put             = "Put"
	Append          = "Append"
	Get             = "Get"
	UpdateConfig    = "UpdateConfig"
	DeleteShardData = "DeleteShardData"
	UpShardData     = "UpShardData"
)

const (
	WaitChTimeout    = time.Duration(100) * time.Millisecond //ms
	UpConfigInterval = time.Duration(80) * time.Millisecond  //ms
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	RequestID int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct {
	ShardID       int
	DB            ShardDB
	ClientLastReq map[int64]LastReqRes
}

type SendShardReply struct {
	Err          Err
	ShardConfNum int
}

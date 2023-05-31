package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	requestId int
	mu        sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	ck.requestId++
	args.ID.RequestId = ck.requestId
	ck.mu.Unlock()
	args.ID.ClientId = ck.clientId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				raft.DPrintf("ck[%v] deal Query success, Num=%v", ck.clientId, args.Num)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	ck.requestId++
	args.ID.RequestId = ck.requestId
	ck.mu.Unlock()
	args.ID.ClientId = ck.clientId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				raft.DPrintf("ck[%v] deal Join success, servers=%v", ck.clientId, args.Servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	ck.requestId++
	args.ID.RequestId = ck.requestId
	ck.mu.Unlock()
	args.ID.ClientId = ck.clientId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				raft.DPrintf("ck[%v] deal Leave success, GIDs=%v", ck.clientId, args.GIDs)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	ck.requestId++
	args.ID.RequestId = ck.requestId
	ck.mu.Unlock()
	args.ID.ClientId = ck.clientId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

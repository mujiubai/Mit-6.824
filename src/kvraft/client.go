package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me        int64
	requestId int
	leaderId  int64
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.requestId = 0
	ck.me = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.requestId++
	ck.mu.Unlock()
	serverId := ck.leaderId
	args := GetArgs{key, ck.me, ck.requestId}
	DPrintf("ck[%v] send Get rpc..., args=%v", ck.me, args)
	for {
		// DPrintf("ck[%v] resend Get rpc..., args=%v", ck.me, args)
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		// DPrintf("ck[%v] resend Get rpc..., args=%v, reply=%v", ck.me, args, reply)
		if (!ok) || reply.Err == ErrWrongLeader {
			serverId = (serverId + 1) % int64(len(ck.servers))
			continue
		}
		if reply.Err == ErrNoKey {
			ck.mu.Lock()
			ck.leaderId = serverId
			ck.mu.Unlock()
			DPrintf("ck[%v] get no key", ck.me)
			break
		}
		if reply.Err == OK {
			ck.mu.Lock()
			ck.leaderId = serverId
			ck.mu.Unlock()
			DPrintf("ck[%v] success get value=%v, args=%v", ck.me, reply.Value, args)
			return reply.Value
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.requestId++
	ck.mu.Unlock()
	serverId := ck.leaderId
	args := PutAppendArgs{key, value, op, ck.me, ck.requestId}
	DPrintf("ck[%v] send %v rpc..., args=%v,value=%v", ck.me, op, args, value)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		// DPrintf("ck[%v] resend PutAppend rpc..., args=%v, reply=%v", ck.me, args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			serverId = (serverId + 1) % int64(len(ck.servers))
			continue
		}
		if reply.Err == OK {
			ck.mu.Lock()
			ck.leaderId = serverId
			ck.mu.Unlock()
			DPrintf("ck[%v] success PutAppend keyvalue, args=%v", ck.me, args)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

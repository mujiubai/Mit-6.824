package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {

	if Debug {
		log.Printf(format, a...)
	}
	return
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	requestID int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.requestID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	ck.requestID++
	args.RequestID = ck.requestID
	args.ClientID = ck.clientID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("ck[%v] send Get to %v, args=%v", ck.clientID, servers[si], args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("ck[%v] send Get, args=%v, get res=%v ok=%v", ck.clientID, args, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("ck[%v] Get request deal success, args=%v", ck.clientID, args)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("ck[%v] find ErrWrongGroup of Get, args=%v, curConf.Num=%v, curConf.Shards=%v", ck.clientID, args, ck.config.Num, ck.config.Shards)
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.clientID
	ck.requestID++
	args.RequestID = ck.requestID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		// DPrintf("ck[%v] cur config:%v", ck.clientID, ck.config)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("ck[%v] send PutAppend op to %v, args=%v", ck.clientID, servers[si], args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("ck[%v] send PutAppend op to %v, args=%v, get res:%v, ok=%v", ck.clientID, servers[si], args, reply, ok)
				if ok && reply.Err == OK {
					DPrintf("ck[%v] PutAppend request deal success, args=%v", ck.clientID, args)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("ck[%v] find ErrWrongGroup of PutAppend, args=%v, curConf.Num=%v, curConf.Shards=%v", ck.clientID, args, ck.config.Num, ck.config.Shards)
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

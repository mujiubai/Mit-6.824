package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const DealTimeOut = 100 //ms 不能设置过大 否则completion after heal (3A) 无法通过，因为没有超时返回ERRWrongLeader, 就不会重新发送请求将值设置为15，导致无法通过

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	_, isLeader := kv.rf.GetState()
	log.SetFlags(log.Lmicroseconds)
	isLeader = true
	if Debug && isLeader {
		log.Printf(format, a...)
	}
	return
}

type applyRes struct {
	Err       Err
	RequestId int
	Value     string
}

type commandEntry struct {
	op    Op
	curCh chan applyRes
}

const (
	PutOp     = "Put"
	GetOp     = "Get"
	AppendOp  = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB          map[string]string
	clientLastReq map[int64]applyRes   //每个客户端记录的最后一个请求
	applyWaitCh   map[int]commandEntry //raft返回的每个日志对应的通道，用于等待处理完成

	lastApply int
}

func (kv *KVServer) getCurSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvDB) != nil || e.Encode(kv.clientLastReq) != nil {
		return nil
	}
	kv.DPrintf("kv[%v] generate snapdata success, kvdb=%v", kv.me, kv.kvDB)
	return w.Bytes()
}

func (kv *KVServer) restoreFromSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	w := bytes.NewBuffer(data)
	e := labgob.NewDecoder(w)
	var db map[string]string
	var clientLastReq map[int64]applyRes
	if e.Decode(&db) != nil || e.Decode(&clientLastReq) != nil {
		return
	} else {
		kv.kvDB = db
		kv.clientLastReq = clientLastReq
		DPrintf("kv[%v] restore from snapdata success, \ndb=%v", kv.me, kv.kvDB)
	}
}

func (kv *KVServer) applyLoop() {
	for message := range kv.applyCh {
		kv.DPrintf("kv[%v] deal new OP=%v", kv.me, message)

		var result string
		if message.SnapshotValid {
			kv.mu.Lock()
			//考虑raft底层可能提交较老的快照（这份代码底层会这样）
			if message.SnapshotIndex > kv.lastApply {
				kv.lastApply = message.SnapshotIndex
				kv.restoreFromSnapshot(message.Snapshot)
				for _, command := range kv.applyWaitCh {
					command.curCh <- applyRes{ErrWrongLeader, 0, ""}
				}
				kv.applyWaitCh = make(map[int]commandEntry)
			}
			kv.DPrintf("kv[%v] install from snapData, ", kv.me)
			kv.DPrintf("kv[%v] ,\nkvDB=%v, \nclientLast=%v", kv.me, kv.kvDB, kv.clientLastReq)
			kv.mu.Unlock()
			continue
		}
		if !message.CommandValid {
			continue
		}
		if kv.maxraftstate-kv.rf.GetRaftStateSize() <= 2 && kv.maxraftstate > 0 {
			snapData := kv.getCurSnapshot()
			kv.DPrintf("kv[%v] prepare send a snapshot%v to raft, kv.rf.GetRaftStateSize()=%v", kv.me, kv.lastApply, kv.rf.GetRaftStateSize())
			kv.rf.Snapshot(kv.lastApply, snapData)
			kv.DPrintf("kv[%v] success save snapshot", kv.me)
		}

		op := message.Command.(Op)
		kv.mu.Lock()
		kv.lastApply = message.CommandIndex
		lastRes := kv.clientLastReq[op.ClientId]
		kv.DPrintf("kv[%v] lastRes.requestId=%v, op.RequestId=%v", kv.me, lastRes.RequestId, op.RequestId)
		if lastRes.RequestId >= op.RequestId {
			result = lastRes.Value
			kv.DPrintf("kv[%v] use old reques res, message=%v", kv.me, message)
			// DPrintf("kv[%v] use old reques res, message=%v", kv.me, message)
		} else {
			switch op.Operation {
			case PutOp:
				kv.kvDB[op.Key] = op.Value
				kv.DPrintf("kv[%v] deal PutOp success, res=%v", kv.me, kv.kvDB[op.Key])
				result = ""
			case AppendOp:
				kv.kvDB[op.Key] = kv.kvDB[op.Key] + op.Value
				kv.DPrintf("kv[%v] deal AppendOp success, res=%v", kv.me, kv.kvDB[op.Key])
				result = ""
			case GetOp:
				result = kv.kvDB[op.Key]
				kv.DPrintf("kv[%v] deal GetOp success, res=%v", kv.me, kv.kvDB[op.Key])
			}
			kv.DPrintf("kv[%v] after deal, the db=%v", kv.me, kv.kvDB[op.Key])
			kv.clientLastReq[op.ClientId] = applyRes{OK, op.RequestId, result}
		}
		command, ok := kv.applyWaitCh[message.CommandIndex]
		kv.lastApply = message.CommandIndex

		if ok {
			delete(kv.applyWaitCh, message.CommandIndex)
			kv.mu.Unlock()
			if command.op != op {
				command.curCh <- applyRes{ErrWrongLeader, op.RequestId, result}
			} else {
				kv.DPrintf("kv[%v] test1, op=%v", kv.me, op)
				command.curCh <- applyRes{OK, op.RequestId, result}
				kv.DPrintf("kv[%v] test2, op=%v", kv.me, op)
			}
		} else {
			kv.DPrintf("kv[%v] dont get chan from hashtable", kv.me)
			kv.mu.Unlock()
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, tmp := range kv.applyWaitCh {
		close(tmp.curCh)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		kv.DPrintf("kv[%v] is killed, return ErrWrongLeader", kv.me)
		return
	}
	op := Op{GetOp, args.Key, "", args.ClientId, args.RequestId}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrNoKey
		return
	}
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		// kv.DPrintf("kv[%v] is not a leader, return ErrWrongLeader", kv.me)
		return
	}

	kv.DPrintf("kv[%v] start deal get, op=%v", kv.me, op)

	curCh := make(chan applyRes)
	command := commandEntry{op, curCh}
	kv.applyWaitCh[index] = command
	kv.mu.Unlock()

	for !kv.killed() {
		select {
		case res := <-curCh:
			reply.Err = res.Err
			reply.Value = res.Value
			if res.Err == OK {
				if res.Value == "" {
					res.Err = ErrNoKey
				}
			}
			return
		case <-time.After(time.Duration(DealTimeOut) * time.Millisecond):
			go func() { <-curCh }() //需要接受处理结果，否则applyloop会死等
			reply.Err = ErrWrongLeader
			kv.DPrintf("kv[%v] timeout, return ErrWrongLeader", kv.me)
			return
		}
	}
	//当kv crash时
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		kv.DPrintf("kv[%v] is killed, return ErrWrongLeader", kv.me)
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.RequestId}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrNoKey
		return
	}
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		// kv.DPrintf("kv[%v] is not a leader, return ErrWrongLeader", kv.me)
		return
	}

	kv.DPrintf("kv[%v] start deal PutAppend, op=%v", kv.me, op)

	curCh := make(chan applyRes)
	command := commandEntry{op, curCh}
	kv.applyWaitCh[index] = command
	kv.mu.Unlock()

	for !kv.killed() {

		select {
		case res := <-curCh:
			reply.Err = res.Err
			return
		case <-time.After(time.Duration(DealTimeOut) * time.Millisecond):
			go func() { <-curCh }() //需要接受处理结果，否则applyloop会死等
			kv.DPrintf("kv[%v] timeout, return ErrWrongLeader", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
	}

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.kvDB = make(map[string]string)
	kv.applyWaitCh = make(map[int]commandEntry)
	kv.clientLastReq = make(map[int64]applyRes)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApply = kv.rf.GetRaftLastIncludedIndex()
	kv.restoreFromSnapshot(persister.ReadSnapshot())
	DPrintf("kv[%v] create success and restore from snapdata", kv.me)
	// You may need initialization code here.
	go kv.applyLoop()

	return kv
}

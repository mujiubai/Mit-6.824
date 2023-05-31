package shardkv

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = false

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	_, isLeader := kv.rf.GetState()
	log.SetFlags(log.Lmicroseconds)
	// isLeader = true
	if Debug && isLeader {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation     string
	ClientID      int64
	RequestID     int
	Key           string
	Value         string
	UpConfig      shardctrler.Config
	ConfigNum     int //
	ShardId       int //删除更新shard数据时用
	ShardData     ShardDB
	ClientLastReq map[int64]LastReqRes
}

type applyRes struct {
	Err   Err
	reqId int
	value string
}

type LastReqRes struct {
	ReqId int
	Value string
}

type ShardDB struct {
	DB        map[string]string
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApply int
	sc        *shardctrler.Clerk
	dead      int32

	curConf shardctrler.Config
	preConf shardctrler.Config

	lastClientReq []map[int64]LastReqRes //shardID -> clientID -> lastreq
	applyWaitCh   map[int]chan applyRes
	kvDB          []ShardDB
}

func (kv *ShardKV) applyLoop() {
	// DPrintf("DPrintf applyLoop\n")
	DPrintf("kv[%v %v] start applyLoop... ", kv.gid, kv.me)
	for message := range kv.applyCh {
		kv.DPrintf("kv[%v %v] start a new message..., message=%v\n rf-lastApply=%v, rf-logs=%v ", kv.gid, kv.me, message, kv.rf.LastApplied, kv.rf.Logs)
		if message.SnapshotValid {
			kv.mu.Lock()
			if kv.lastApply < message.SnapshotIndex {
				kv.lastApply = message.SnapshotIndex
				kv.restoreFromSnap(message.Snapshot)
				for _, waitCh := range kv.applyWaitCh {
					waitCh <- applyRes{Err: ErrWrongGroup}
				}
				kv.applyWaitCh = make(map[int]chan applyRes)
				kv.DPrintf("kv[%v %v] install snapshot success ", kv.gid, kv.me)
			}
			kv.mu.Unlock()
			continue
		}
		if !message.CommandValid {
			continue
		}

		if kv.maxraftstate-kv.rf.GetRaftStateSize() <= 2 && kv.maxraftstate > 0 {
			kv.DPrintf("kv[%v %v] prepare send snapshot to raft, maxraftstate=%v, GetRaftStateSize()=%v", kv.gid, kv.me, kv.maxraftstate, kv.rf.GetRaftStateSize())
			data := kv.genSnapData()
			kv.rf.Snapshot(kv.lastApply, data)
			kv.DPrintf("kv[%v %v] send snapshot to raft success, maxraftstate=%v, GetRaftStateSize()=%v", kv.gid, kv.me, kv.maxraftstate, kv.rf.GetRaftStateSize())
		}

		op := message.Command.(Op)

		res := applyRes{}
		kv.mu.Lock()
		if op.Operation == Put || op.Operation == Get || op.Operation == Append {
			shardID := key2shard(op.Key)
			lastReq, ok := kv.lastClientReq[shardID][op.ClientID]
			if ok && lastReq.ReqId >= op.RequestID {
				// res.value = lastReq.value
				res.Err = ErrRepeatReq
				res.value = lastReq.Value
				kv.DPrintf("kv[%v %v] find ErrRepeatReq failed, lastReq=%v, op.RequestID=%v", kv.gid, kv.me, lastReq, op.RequestID)
			} else {
				shardID := key2shard(op.Key)
				switch op.Operation {
				case Put:
					kv.DPrintf("kv[%v %v] exec a Put op...,op=%v", kv.gid, kv.me, op)
					kv.kvDB[shardID].DB[op.Key] = op.Value
				case Get:
					kv.DPrintf("kv[%v %v] exec a Get op...,op=%v", kv.gid, kv.me, op)
					res.value = kv.kvDB[shardID].DB[op.Key]
				case Append:
					kv.DPrintf("kv[%v %v] exec a Append op..., op=%v", kv.gid, kv.me, op)
					kv.DPrintf("kv[%v %v] before deal op, the shardID%v corrspond map:%v", kv.gid, kv.me, shardID, kv.kvDB[shardID])
					kv.kvDB[shardID].DB[op.Key] = kv.kvDB[shardID].DB[op.Key] + op.Value
				}
				res.Err = OK
				res.reqId = op.RequestID
				kv.lastClientReq[shardID][op.ClientID] = LastReqRes{op.RequestID, res.value}
				kv.DPrintf("kv[%v %v] after deal op, the shardID%v corrspond map:%v", kv.gid, kv.me, shardID, kv.kvDB[shardID])
			}
		} else {
			switch op.Operation {
			case UpdateConfig:
				kv.DPrintf("kv[%v %v] exec a UpdateConfig op...", kv.gid, kv.me)
				if op.UpConfig.Num > kv.curConf.Num {
					kv.preConf = kv.curConf
					kv.curConf = op.UpConfig
					kv.DPrintf("kv[%v %v] update config success,preconf=%v, curConf=%v", kv.gid, kv.me, kv.preConf, kv.curConf)
				} else {
					kv.DPrintf("kv[%v %v] update config failed, find old upconfig, curConf.Num=%v, op.upconfig.Num=%v", kv.gid, kv.me, kv.curConf.Num, op.UpConfig.Num)
				}

			case UpShardData:
				kv.upShardDataHandler(&op)
			case DeleteShardData:
				kv.deleteShardDataHandler(&op)
			}
			res.Err = OK
		}
		kv.lastApply = message.CommandIndex
		waitCh, ok := kv.applyWaitCh[message.CommandIndex]
		kv.mu.Unlock()
		if ok {
			kv.DPrintf("kv[%v %v] waitch...", kv.gid, kv.me)
			waitCh <- res
			kv.DPrintf("kv[%v %v] waitch success", kv.gid, kv.me)
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, tmp := range kv.applyWaitCh {
		close(tmp)
	}
}

func (kv *ShardKV) detecConfLoop() {
	for !kv.killed() {
		kv.DPrintf("kv[%v %v] detecConfLoop test , curConfig=%v", kv.gid, kv.me, kv.curConf)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(UpConfigInterval)
			continue
		}
		kv.mu.Lock()
		if !kv.sendFinish() {
			kv.DPrintf("kv[%v %v] sendFinish failed, start sync shard to other server", kv.gid, kv.me)
			kv.sendShard2Server()
			kv.mu.Unlock()
			time.Sleep(UpConfigInterval)
			continue
		} else {
			kv.DPrintf("kv[%v %v] sendFinish finish", kv.gid, kv.me)
		}
		if !kv.recFinish() {
			kv.DPrintf("kv[%v %v] recFinish failed", kv.gid, kv.me)
			kv.mu.Unlock()
			time.Sleep(UpConfigInterval)
			continue
		} else {
			kv.DPrintf("kv[%v %v] recFinish finish", kv.gid, kv.me)
		}
		configNum := kv.curConf.Num + 1
		newConfig := kv.sc.Query(configNum)
		if newConfig.Num == configNum {
			kv.DPrintf("kv[%v %v] find new config , newConfig=%v", kv.gid, kv.me, newConfig)
			op := Op{}
			op.Operation = UpdateConfig
			op.UpConfig = newConfig
			op.UpConfig = newConfig
			index, term, isLeader := kv.rf.Start(op)
			if term > 0 && isLeader {
				kv.DPrintf("kv[%v %v] submit config update, newConfig=%v", kv.gid, kv.me, newConfig)
				waitCh := make(chan applyRes)
				kv.applyWaitCh[index] = waitCh
				kv.mu.Unlock()
				res := <-waitCh
				if res.Err == OK {
					kv.DPrintf("kv[%v %v] get update config res, curConf=%v", kv.gid, kv.me, kv.curConf)
				} else {
					kv.DPrintf("kv[%v %v] update config failed, upConf=%v", kv.gid, kv.me, newConfig)
				}
				// select {
				// case res := <-waitCh:
				// 	if res.Err == OK {
				// 		kv.DPrintf("kv[%v %v] update config success, curConf=%v", kv.gid, kv.me, kv.curConf)
				// 	} else {
				// 		kv.DPrintf("kv[%v %v] update config failed, upConf=%v", kv.gid, kv.me, newConfig)
				// 	}
				// case <-time.After(WaitChTimeout):
				// 	go func() { <-waitCh }()
				// 	kv.DPrintf("kv[%v %v] update config timeout", kv.gid, kv.me)
				// }
			} else {
				kv.mu.Unlock()
			}
		} else {
			kv.DPrintf("kv[%v %v] dont find new config", kv.gid, kv.me)
			kv.mu.Unlock()
		}

		time.Sleep(UpConfigInterval)
	}
}

func (kv *ShardKV) upShardDataHandler(op *Op) {
	shardID := op.ShardId
	if shardID < 0 || shardID >= shardctrler.NShards {
		return
	}
	kv.kvDB[shardID].DB = make(map[string]string)
	for k, v := range op.ShardData.DB {
		kv.kvDB[shardID].DB[k] = v
	}
	kv.kvDB[shardID].ConfigNum = op.ConfigNum
	for k, v := range op.ClientLastReq {
		kv.lastClientReq[shardID][k] = v
	}
	kv.DPrintf("kv[%v %v] update shard data success, now kvdb=%v", kv.gid, kv.me, kv.kvDB)
}

func (kv *ShardKV) deleteShardDataHandler(op *Op) {
	shardID := op.ShardId
	if shardID < 0 || shardID >= shardctrler.NShards {
		return
	}
	if kv.preConf.Num > 0 {
		kv.kvDB[shardID].DB = nil
	}
	kv.kvDB[shardID].ConfigNum = op.ConfigNum
	kv.lastClientReq[shardID] = make(map[int64]LastReqRes)
	kv.DPrintf("kv[%v %v] delete shard data success, now kvdb=%v", kv.gid, kv.me, kv.kvDB)
}

func (kv *ShardKV) sendFinish() bool {
	kv.DPrintf("kv[%v %v] preconf:%v %v ,curConf:%v %v", kv.gid, kv.me, kv.preConf.Num, kv.preConf.Shards, kv.curConf.Num, kv.curConf.Shards)
	for shardID, gid := range kv.preConf.Shards {
		// kv.DPrintf("kv[%v %v] gid=%v kv.gid=%v kv.curConf.Shards[shardID]=%v kv.kvDB[shardID].configNum=%v", kv.gid, kv.me, gid, kv.gid, kv.curConf.Shards[shardID], kv.kvDB[shardID].configNum)
		if gid == kv.gid && kv.curConf.Shards[shardID] != kv.gid && kv.kvDB[shardID].ConfigNum < kv.curConf.Num {
			return false
		}
	}
	if kv.preConf.Num == 0 {
		for shardID, gid := range kv.curConf.Shards {
			if gid == kv.gid && kv.kvDB[shardID].ConfigNum == 0 {
				return false
			}
		}
	}
	return true
}

func (kv *ShardKV) recFinish() bool {
	for shardID, gid := range kv.curConf.Shards {
		if gid == kv.gid && kv.preConf.Shards[shardID] != kv.gid && kv.kvDB[shardID].ConfigNum < kv.curConf.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) sendShard2Server() {
	for shardID, gid := range kv.curConf.Shards {
		if (gid == kv.gid && kv.kvDB[shardID].ConfigNum == 0 && kv.preConf.Num == 0) || (gid != kv.gid && kv.preConf.Shards[shardID] == kv.gid && kv.kvDB[shardID].ConfigNum < kv.curConf.Num) {
			// if kv.kvDB[shardID].ConfigNum > 0 && kv.kvDB[shardID].DB == nil {
			// 	DPrintf("kv[%v %v] try send shard data to %v, but the db is nil, failed. SharID=%v", kv.gid, kv.me, gid, shardID)
			// 	continue
			// }
			db := ShardDB{}
			db.ConfigNum = kv.curConf.Num
			db.DB = make(map[string]string)
			for k, v := range kv.kvDB[shardID].DB {
				db.DB[k] = v
			}
			lastReq := make(map[int64]LastReqRes)
			for k, v := range kv.lastClientReq[shardID] {
				lastReq[k] = v
			}
			args := SendShardArgs{}
			args.DB = db
			args.ClientLastReq = lastReq
			args.ShardID = shardID
			sendID := gid
			if kv.kvDB[shardID].ConfigNum == 0 {
				sendID = kv.gid
				args.DB.ConfigNum = 1
			}
			curConf := kv.curConf
			if gid == kv.gid && kv.kvDB[shardID].ConfigNum == 0 && kv.preConf.Num == 0 {
				DPrintf("kv[%v %v] decide1 send shard data to %v, data=%v", kv.gid, kv.me, sendID, args)
			} else {
				DPrintf("kv[%v %v] decide2 send shard data to %v, data=%v", kv.gid, kv.me, sendID, args)
			}
			go func(gid int) {
				if servers, ok := curConf.Groups[gid]; ok {
					offset := rand.Intn(len(servers))
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[(si+offset)%len(servers)])
						var reply SendShardReply
						DPrintf("kv[%v %v] send shard data to %v, args=%v", kv.gid, kv.me, servers[si], args)
						ok := srv.Call("ShardKV.UpdateShardData", &args, &reply)
						// done := make(chan bool, 1)
						// go func() {
						// 	ok := srv.Call("ShardKV.UpdateShardData", &args, &reply)
						// 	done <- ok
						// }()
						// select {
						// case <-time.After(time.Duration(300 * time.Microsecond)):
						// 	DPrintf("kv[%v %v] send shard data to %v, args=%v, timeout!!", kv.gid, kv.me, servers[si], args)
						// 	ok = false
						// case res := <-done:
						// 	ok = res
						// }
						DPrintf("kv[%v %v] send shard data to %v, args=%v, get reply=%v", kv.gid, kv.me, servers[si], args, reply)
						if !ok {
							continue
						}
						if ok && reply.Err == ErrShardData {
							return
						}
						if ok && reply.Err == OK {
							DPrintf("kv[%v %v] send shard success, args=%v,reply=%v", kv.gid, kv.me, args, reply)
							kv.mu.Lock()
							if reply.ShardConfNum == kv.curConf.Num {
								op := Op{}
								op.Operation = DeleteShardData
								op.ShardId = args.ShardID
								op.ConfigNum = kv.curConf.Num
								index, term, isLeader := kv.rf.Start(op)
								if term > 0 && isLeader {
									DPrintf("kv[%v %v] submit delete shard data op, args=%v", kv.gid, kv.me, args)
									waitCh := make(chan applyRes)
									kv.applyWaitCh[index] = waitCh
									go func() { <-waitCh }()
								}
							}
							kv.mu.Unlock()
							return
						}
						if ok && reply.Err == ErrWrongGroup {
							DPrintf("kv[%v %v] update shard failed, find ErrWrongGroup, args=%v", kv.gid, kv.me, args)
							return
						}
						if ok && reply.Err == ErrWrongLeader {
							DPrintf("kv[%v %v] update shard failed, find ErrWrongLeader, args=%v", kv.gid, kv.me, args)
							continue
						}
						// ... not ok, or ErrWrongLeader
					}
				}
			}(sendID)

		}
	}
}

//--------------------------------------RPC--------------------------------------
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.DPrintf("kv[%v %v] find Get op, args=%v, raft:LastApplied=%v,CommitIndex=%v,Lastindex=%v,logs=%v", kv.gid, kv.me, args, kv.rf.LastApplied, kv.rf.CommitIndex, kv.rf.Lastindex, kv.rf.Logs)
	kv.DPrintf("kv[%v %v] find Get op, args=%v", kv.gid, kv.me, args)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardID := key2shard(args.Key)
	kv.mu.Lock()
	if kv.curConf.Shards[shardID] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if kv.kvDB[shardID].DB == nil {
		kv.mu.Unlock()
		reply.Err = ErrShardEmpty
		kv.DPrintf("kv[%v %v] PutAppend ErrShardEmpty, shardID=%v db=%v\n preConf=%v %v, curConf=%v %v", kv.gid, kv.me, shardID, kv.kvDB, kv.preConf.Num, kv.preConf.Shards, kv.curConf.Num, kv.curConf.Shards)
		return
	}
	mop := Op{}
	mop.Operation = Get
	mop.ClientID = args.ClientID
	mop.RequestID = args.RequestID
	mop.Key = args.Key
	index, term, isLeader := kv.rf.Start(mop)
	if !isLeader || term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	kv.DPrintf("kv[%v %v] start Get op, args=%v, op=%v", kv.gid, kv.me, args, mop)
	waitCh := make(chan applyRes)
	kv.applyWaitCh[index] = waitCh
	kv.mu.Unlock()

	for {
		select {
		case res := <-waitCh:
			reply.Err = res.Err
			if res.value == "" && res.Err == OK {
				reply.Err = ErrNoKey
			}
			if reply.Err == ErrRepeatReq {
				reply.Err = OK
			}
			reply.Value = res.value
			return
		case <-time.After(WaitChTimeout):
			go func() { <-waitCh }()
			reply.Err = ErrWrongLeader
			return
		}
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.DPrintf("kv[%v %v] get PutAppend op, args=%v", kv.gid, kv.me, args)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardID := key2shard(args.Key)
	kv.mu.Lock()
	if kv.curConf.Shards[shardID] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		kv.DPrintf("kv[%v %v] PutAppend ErrWrongGroup, shardID=%v,curConf.Num=%v, curConf=%v, preconf=%v", kv.gid, kv.me, shardID, kv.curConf.Num, kv.curConf.Shards, kv.preConf.Shards)
		return
	}
	if kv.kvDB[shardID].DB == nil {
		kv.mu.Unlock()
		reply.Err = ErrShardEmpty
		kv.DPrintf("kv[%v %v] PutAppend ErrShardEmpty, shardID=%v db=%v", kv.gid, kv.me, shardID, kv.kvDB)
		return
	}
	mop := Op{}
	mop.Operation = args.Op
	mop.ClientID = args.ClientID
	mop.RequestID = args.RequestID
	mop.Key = args.Key
	mop.Value = args.Value
	index, term, isLeader := kv.rf.Start(mop)
	DPrintf("2\n")
	if !isLeader || term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	kv.DPrintf("kv[%v %v] start PutAppend op, args=%v", kv.gid, kv.me, args)
	waitCh := make(chan applyRes)
	kv.applyWaitCh[index] = waitCh
	kv.mu.Unlock()

	for {
		kv.DPrintf("kv[%v %v] args=%v, start loop...", kv.gid, kv.me, args)
		select {
		case res := <-waitCh:
			reply.Err = res.Err
			if reply.Err == ErrRepeatReq {
				reply.Err = OK
			}
			kv.DPrintf("kv[%v %v] args=%v, wait deal success", kv.gid, kv.me, args)
			return
		case <-time.After(WaitChTimeout):
			go func() { <-waitCh }()
			reply.Err = ErrWrongLeader
			kv.DPrintf("kv[%v %v] args=%v, wait deal timeout", kv.gid, kv.me, args)
			return
		}
	}
}

func (kv *ShardKV) UpdateShardData(args *SendShardArgs, reply *SendShardReply) {
	// kv.DPrintf("kv[%v %v] get UpdateShardData rpc", kv.gid, kv.me)
	kv.mu.Lock()
	kv.DPrintf("kv[%v %v] get UpdateShardData rpc, dealing. args=%v", kv.gid, kv.me, args)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//判断是否为旧分片数据
	if kv.kvDB[args.ShardID].ConfigNum >= args.DB.ConfigNum && args.DB.ConfigNum > 0 {
		kv.DPrintf("kv[%v %v] update shard, find old data, kv.kvDB[args.ShardID].configNum=%v, args.DB.configNum =%v", kv.gid, kv.me, kv.kvDB[args.ShardID].ConfigNum, args.DB.ConfigNum)
		reply.Err = OK
		reply.ShardConfNum = kv.kvDB[args.ShardID].ConfigNum
		kv.mu.Unlock()
		return
	}
	//判断当前配置是否允许接受改分片
	if kv.curConf.Shards[args.ShardID] != kv.gid {
		reply.Err = ErrShardData
		kv.mu.Unlock()
		kv.DPrintf("kv[%v %v] find ErrShardData, args=%v.\n preConf:%v %v,curConf=%v %v", kv.gid, kv.me, args, kv.preConf.Num, kv.preConf.Shards, kv.curConf.Num, kv.curConf.Shards)
		return
	}
	op := Op{}
	op.Operation = UpShardData
	op.ShardId = args.ShardID
	op.ShardData = args.DB
	op.ClientLastReq = args.ClientLastReq
	op.ConfigNum = args.DB.ConfigNum
	index, term, isLeader := kv.rf.Start(op)
	kv.DPrintf("kv[%v %v] submit a UpShardData op, op=%v", kv.gid, kv.me, op)
	if term > 0 && isLeader {
		waitCh := make(chan applyRes)
		kv.applyWaitCh[index] = waitCh
		go func() { <-waitCh }()
		reply.Err = OK
		reply.ShardConfNum = args.DB.ConfigNum
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()

}

//-------------------------------持久化和恢复---------------------------------
func (kv *ShardKV) genSnapData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvDB) != nil ||
		e.Encode(kv.lastClientReq) != nil ||
		e.Encode(kv.curConf) != nil ||
		e.Encode(kv.preConf) != nil {
		return nil
	}
	return w.Bytes()
}

func (kv *ShardKV) restoreFromSnap(data []byte) {
	if len(data) == 0 {
		kv.DPrintf("kv[%v %v] restore data from snapdata failed, the data is nil", kv.gid, kv.me)
		return
	}
	w := bytes.NewBuffer(data)
	e := labgob.NewDecoder(w)
	var db []ShardDB
	var lastClientReq []map[int64]LastReqRes
	var curConf shardctrler.Config
	var preConf shardctrler.Config
	if e.Decode(&db) != nil ||
		e.Decode(&lastClientReq) != nil ||
		e.Decode(&curConf) != nil ||
		e.Decode(&preConf) != nil {
		return
	} else {
		kv.kvDB = db
		kv.lastClientReq = lastClientReq
		kv.curConf = curConf
		kv.preConf = preConf
		kv.DPrintf("kv[%v %v] restore data from snapdata, now db=%v", kv.gid, kv.me, kv.kvDB)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.lastApply = 0
	kv.lastClientReq = make([]map[int64]LastReqRes, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.lastClientReq[i] = make(map[int64]LastReqRes)
	}
	kv.applyWaitCh = make(map[int]chan applyRes)
	kv.kvDB = make([]ShardDB, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.kvDB[i] = ShardDB{nil, 0}
	}
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.curConf = kv.sc.Query(0)
	kv.preConf = kv.curConf

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApply = kv.rf.Lastindex
	kv.restoreFromSnap(persister.ReadSnapshot())
	go kv.detecConfLoop()
	go kv.applyLoop()

	DPrintf("kv[%v %v] create success!curConf:%v,server=%v, maxraftstate=%v", kv.gid, kv.me, kv.curConf, ctrlers, kv.maxraftstate)

	//需要提交一条空日志，否则底层raft不重放日志
	go func() {
		time.Sleep(time.Duration(600) * time.Millisecond)
		op := Op{}
		kv.mu.Lock()
		index, term, isLeader := kv.rf.Start(op)
		DPrintf("kv[%v %v] submit a NULL op, op=%v", kv.gid, kv.me, op)
		kv.mu.Unlock()
		if term > 0 && isLeader {
			DPrintf("kv[%v %v] submit  NULL op success, op=%v", kv.gid, kv.me, op)
			waitCh := make(chan applyRes)
			kv.applyWaitCh[index] = waitCh
			go func() { <-waitCh }()
		}
	}()

	return kv
}

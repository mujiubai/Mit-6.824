package shardctrler

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clientLastReq map[int64]applyRes
	applyWaitCh   map[int]chan applyRes

	lastApply int

	configs   []Config // indexed by config num
	curConfId int
}

type applyRes struct {
	Err   Err
	reqId int
	Value Config
}

type Op struct {
	// Your data here.
	Operation string
	ClientID  int64
	RequestId int
	Servers   map[int][]string //JoinOp
	GIDs      []int            //LeaveOp
	Shard     int              //MoveOp
	GID       int              //MoveOp
	Num       int              //QueryOp

}

func (sc *ShardCtrler) rebalanceLoad(shards [NShards]int) [NShards]int {
	raft.DPrintf("sc[%v] rebalanceLoad dealing, res=%v", sc.me, shards)
	gids := make(map[int][]int)
	for i := 0; i < NShards; i++ {
		gids[shards[i]] = append(gids[shards[i]], i)
	}
	for {
		minN, maxN := 1, 1
		for key, _ := range gids {
			minN = key
			maxN = key
		}
		for key, value := range gids {
			if len(value) < len(gids[minN]) {
				minN = key
			}
			if len(value) > len(gids[maxN]) {
				maxN = key
			}
		}
		// raft.DPrintf("max=%v ,min=%v", len(gids[maxN]), len(gids[minN]))
		if len(gids[maxN])-len(gids[minN]) <= 1 {
			break
		}
		length := len(gids[maxN])
		gids[minN] = append(gids[minN], gids[maxN][length-1])
		gids[maxN] = gids[maxN][:length-1]
	}
	for key, value := range gids {
		for i := 0; i < len(value); i++ {
			shards[value[i]] = key
		}
	}
	raft.DPrintf("sc[%v] rebalanceLoad success, res=%v", sc.me, shards)
	return shards
}

func (sc *ShardCtrler) applyLoop() {
	for message := range sc.applyCh {
		_, isLeader := sc.rf.GetState()
		isLeader = true
		if message.SnapshotValid {
			// sc.mu.Lock()
			// if message.SnapshotIndex > sc.lastApply {
			// 	sc.lastApply = message.SnapshotIndex
			// 	sc.restoreFromSnapshot(message.Snapshot)
			// 	for _, ch := range sc.applyWaitCh {
			// 		tmp := applyRes{}
			// 		tmp.Err = TimeOut
			// 		ch <- tmp
			// 	}
			// 	sc.applyWaitCh = make(map[int]chan applyRes)
			// 	raft.DPrintf("sc[%v] install snapshot success!", sc.me)
			// }
			// sc.mu.Unlock()
			continue
		}
		if !message.CommandValid {
			continue
		}
		op := message.Command.(Op)
		sc.mu.Lock()
		lastReq := sc.clientLastReq[op.ClientID]
		res := applyRes{}
		res.Err = OK
		res.reqId = op.RequestId
		if lastReq.reqId >= op.RequestId {
			res.Value = lastReq.Value
			if isLeader {
				raft.DPrintf("sc[%v] use old value, last reqID=%v, cur reqID=%v", sc.me, lastReq.reqId, op.RequestId)
			}
		} else {
			if op.Operation != QueryOp {
				//创建新配置
				newConf := Config{}
				newConf.Groups = make(map[int][]string)
				newConf.Shards = sc.configs[sc.curConfId].Shards
				for key, value := range sc.configs[sc.curConfId].Groups {
					newConf.Groups[key] = value
				}

				switch op.Operation {
				case JoinOp:
					if isLeader {
						raft.DPrintf("sc[%v] get a Join, dealing", sc.me)
					}
					for key, value := range op.Servers {
						newConf.Groups[key] = append(newConf.Groups[key], value...)
					}
				case LeaveOp:
					if isLeader {
						raft.DPrintf("sc[%v] get a Leave, dealing", sc.me)
					}
					for i := 0; i < len(op.GIDs); i++ {
						delete(newConf.Groups, op.GIDs[i])
					}
				case MoveOp:
					if isLeader {
						raft.DPrintf("sc[%v] get a Move, dealing", sc.me)
					}
					newConf.Shards[op.Shard] = op.GID
					if isLeader {
						raft.DPrintf("sc[%v] after deal Move, shards=%v", sc.me, newConf.Shards)
					}
				}
				if op.Operation == JoinOp || op.Operation == LeaveOp {
					var tmpKeys []int
					for key := range newConf.Groups {
						tmpKeys = append(tmpKeys, key)
					}
					sort.Ints(tmpKeys)
					i := 0
					for key := range tmpKeys {
						newConf.Shards[i] = tmpKeys[key]
						// raft.DPrintf("sc[%v] key:%v", sc.me, key)
						i++
						if i >= 10 {
							break
						}
					}
					for ; i < 10 && i > 0; i++ {
						newConf.Shards[i] = newConf.Shards[i-1]
					}
				}
				//平衡负载并保存配置
				newConf.Shards = sc.rebalanceLoad(newConf.Shards) //有个缺陷：万一把Move改了
				sc.curConfId++
				newConf.Num = sc.curConfId
				sc.configs = append(sc.configs, newConf)
				if isLeader {
					raft.DPrintf("sc[%v] new config config:%v ", sc.me, newConf)
				}
			} else {
				if isLeader {
					raft.DPrintf("sc[%v] get a Query, dealing, Num=%v", sc.me, op.Num)
				}
				if op.Num == -1 || op.Num > sc.curConfId {
					res.Value = sc.configs[sc.curConfId]
				} else {
					res.Value = sc.configs[op.Num]
				}
				if isLeader {
					raft.DPrintf("sc[%v] return config res:%v !! configs=%v", sc.me, res.Value, sc.configs)
				}
			}
			sc.clientLastReq[op.ClientID] = res
		}
		curCh, ok := sc.applyWaitCh[message.CommandIndex]
		if ok {
			raft.DPrintf("sc[%v] return res ing...", sc.me)
			curCh <- res
			raft.DPrintf("sc[%v] return res success", sc.me)
		}
		sc.mu.Unlock()
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, ch := range sc.applyWaitCh {
		close(ch)
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	mop := Op{}
	mop.Operation = JoinOp
	mop.Servers = args.Servers
	mop.ClientID = args.ID.ClientId
	mop.RequestId = args.ID.RequestId
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(mop)
	if !isLeader || term == 0 {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("sc[%v] start Join Operation, servers=%v", sc.me, mop.Servers)
	curCh := make(chan applyRes)
	sc.applyWaitCh[index] = curCh
	sc.mu.Unlock()
	for {
		select {
		case res := <-curCh:
			reply.WrongLeader = false
			reply.Err = res.Err
			return
		case <-time.After(time.Duration(100) * time.Millisecond):
			reply.WrongLeader = true
			reply.Err = TimeOut
			go func() { <-curCh }()
			return
		}
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	mop := Op{}
	mop.Operation = LeaveOp
	mop.GIDs = args.GIDs
	mop.ClientID = args.ID.ClientId
	mop.RequestId = args.ID.RequestId
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(mop)
	if !isLeader || term == 0 {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("sc[%v] start Leave Operation, GIDs=%v", sc.me, mop.GIDs)
	curCh := make(chan applyRes)
	sc.applyWaitCh[index] = curCh
	sc.mu.Unlock()

	for {
		select {
		case res := <-curCh:
			reply.WrongLeader = false
			reply.Err = res.Err
			if res.Err == NoLeaveGid {
				reply.WrongLeader = true
			}
			return
		case <-time.After(time.Duration(100) * time.Millisecond):
			reply.WrongLeader = true
			reply.Err = TimeOut
			go func() { <-curCh }()
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	mop := Op{}
	mop.Operation = MoveOp
	mop.ClientID = args.ID.ClientId
	mop.RequestId = args.ID.RequestId
	mop.Shard = args.Shard
	mop.GID = args.GID
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(mop)
	if !isLeader || term == 0 {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("sc[%v] start Move Operation, shards=%v, gid=%v", sc.me, mop.Shard, mop.GID)
	curCh := make(chan applyRes)
	sc.applyWaitCh[index] = curCh
	sc.mu.Unlock()
	for {
		select {
		case res := <-curCh:
			reply.WrongLeader = false
			reply.Err = res.Err
			return
		case <-time.After(time.Duration(100) * time.Millisecond):
			reply.WrongLeader = true
			reply.Err = TimeOut
			go func() { <-curCh }()
			return
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	mop := Op{}
	mop.Operation = QueryOp
	mop.Num = args.Num
	mop.ClientID = args.ID.ClientId
	mop.RequestId = args.ID.RequestId
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(mop)
	sc.mu.Unlock()
	if !isLeader || term == 0 {
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("sc[%v] start Query Operation, Num=%v", sc.me, mop.Num)
	curCh := make(chan applyRes)
	sc.applyWaitCh[index] = curCh

	for {
		select {
		case res := <-curCh:
			reply.WrongLeader = false
			reply.Err = res.Err
			reply.Config = res.Value
			return
		case <-time.After(time.Duration(100) * time.Millisecond):
			reply.WrongLeader = true
			reply.Err = TimeOut
			go func() { <-curCh }()
			return
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) getCurSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sc.configs) != nil || e.Encode(sc.clientLastReq) != nil {
		return nil
	}
	return w.Bytes()
}

func (sc *ShardCtrler) restoreFromSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	w := bytes.NewBuffer(data)
	e := labgob.NewDecoder(w)
	var configs []Config
	var clientLastReq map[int64]applyRes
	if e.Decode(&configs) != nil || e.Decode(&clientLastReq) != nil {
		return
	} else {
		sc.configs = configs
		sc.clientLastReq = clientLastReq
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.curConfId = 0

	// Your code here.

	sc.applyWaitCh = make(map[int]chan applyRes)
	sc.clientLastReq = make(map[int64]applyRes)
	sc.lastApply = sc.rf.Lastindex

	sc.restoreFromSnapshot(persister.ReadSnapshot())
	go sc.applyLoop()

	return sc
}

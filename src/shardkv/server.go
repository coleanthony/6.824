package shardkv

import (
	"bytes"
	"lab/src/labgob"
	"lab/src/labrpc"
	"lab/src/raft"
	"lab/src/shardmaster"
	"sync"
	"time"
)

//实现ShardKVServer服务，ShardKVServer则需要实现所有分片的读写任务，
//相比于基础的读写服务，还需要功能和难点为配置更新，分片数据迁移，分片数据清理，空日志检测

const TimeoutApply = 240 * time.Millisecond
const TimeoutConfigUpdate = 100 * time.Millisecond

const (
	CommandPut          = "Put"
	CommandAppend       = "Append"
	CommandGet          = "Get"
	CommandUpdateConfig = "UpdateConfig"
	CommandGC           = "GC"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	CommandId int64
	ClientId  int64
	//get put append
	Key   string
	Value string
	//update config

	//clean up shards
}

type Res struct {
	Value       string
	Err         Err
	ClientId    int64
	CommandId   int64
	OK          bool
	WrongLeader bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	statemachine [shardmaster.NShards]KVmemory //存储
	resultCh     map[int]chan Res              // logindex对应位置的结果
	lastopack    map[int64]int64               // 记录一个 client 已经处理过的最大 requestId
	config       shardmaster.Config            //配置
	mck          *shardmaster.Clerk            //clerk
}

func (kv *ShardKV) SubmitCommand(op Op) Res {
	//submit commands to the Raft log using Start()
	//ApplyEntries() to rf.applych->KVSerer.applych
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Res{OK: false}
	}

	kv.mu.Lock()
	_, ok := kv.resultCh[index]
	if !ok {
		kv.resultCh[index] = make(chan Res, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultCh[index]:
		//fmt.Printf("client[%d] command[%d] get data\n", op.ClientId, op.CommandId)
		if op.Command == CommandAppend || op.Command == CommandGet || op.Command == CommandPut {
			if op.ClientId == result.ClientId && op.CommandId == result.CommandId {
				return result
			}
		} else if op.Command == CommandGC {

		} else {
			//Command update config

		}

		return Res{OK: false}
	case <-time.After(TimeoutApply):
		//fmt.Printf("client[%d] command[%d] timeout\n", op.ClientId, op.CommandId)
		return Res{OK: false}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Command:   CommandGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	res := kv.SubmitCommand(op)
	//fmt.Println("get")
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	res := kv.SubmitCommand(op)
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	//fmt.Println("append put")
	reply.WrongLeader = false
	reply.Err = res.Err
}

func (kv *ShardKV) IsValidKey(key string) bool {
	//which shard it belongs to?
	return key2shard(key) == kv.gid
}

func (kv *ShardKV) ApplyGet(op Op, res *Res) {
	if !kv.IsValidKey(op.Key) {
		res.Err = ErrWrongGroup
		return
	}

	kv.lastopack[op.ClientId] = op.CommandId
	shardId := key2shard(op.Key)
	res.Value, res.Err = kv.statemachine[shardId].Get(op.Key)
}

func (kv *ShardKV) ApplyPut(op Op, res *Res) {
	if !kv.IsValidKey(op.Key) {
		res.Err = ErrWrongGroup
		return
	}
	shardId := key2shard(op.Key)

	if _, ok := kv.lastopack[op.ClientId]; !ok {
		res.Err = kv.statemachine[shardId].Put(op.Key, op.Value)
		kv.lastopack[op.ClientId] = op.CommandId
	} else {
		if kv.lastopack[op.ClientId] >= op.CommandId {
			res.Err = OK
		} else {
			kv.lastopack[op.ClientId] = op.CommandId
			res.Err = kv.statemachine[shardId].Put(op.Key, op.Value)
		}
	}
}

func (kv *ShardKV) ApplyAppend(op Op, res *Res) {
	if !kv.IsValidKey(op.Key) {
		res.Err = ErrWrongGroup
		return
	}
	shardId := key2shard(op.Key)

	if _, ok := kv.lastopack[op.ClientId]; !ok {
		kv.lastopack[op.ClientId] = op.CommandId
		res.Err = kv.statemachine[shardId].Append(op.Key, op.Value)
	} else {
		if kv.lastopack[op.ClientId] >= op.CommandId {
			res.Err = OK
		} else {
			kv.lastopack[op.ClientId] = op.CommandId
			res.Err = kv.statemachine[shardId].Append(op.Key, op.Value)
		}
	}
}

func (kv *ShardKV) ApplyUpdateConfig(op Op, res *Res) {

}

func (kv *ShardKV) ApplyGC(op Op, res *Res) {

}

func (kv *ShardKV) Applier() {
	// keep reading applyCh while PutAppend() and Get() handlers submit commands to the Raft log using Start()
	for {
		applymsg := <-kv.applyCh
		kv.mu.Lock()
		if applymsg.UseSnapshot {
			//fmt.Println("use snapshot")
			r := bytes.NewBuffer(applymsg.Snapshot)
			d := labgob.NewDecoder(r)
			var lastIncludedIndex, lastIncludedTerm int
			//d.Decode(&lastIncludedIndex)
			//d.Decode(&lastIncludedTerm)
			//d.Decode(&kv.statemachine)
			//d.Decode(&kv.lastopack)

			if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&kv.statemachine) != nil || d.Decode(&kv.lastopack) != nil {
				//fmt.Println("applier decode snapshot error")
				panic("applier decode snapshot error")
			}
		} else {
			//do not use snapshot
			op := applymsg.Command.(Op)
			res := Res{
				ClientId:    op.ClientId,
				Err:         OK,
				CommandId:   op.CommandId,
				OK:          true,
				WrongLeader: false,
			}

			switch op.Command {
			case CommandGet:
				kv.ApplyGet(op, &res)
			case CommandPut:
				kv.ApplyPut(op, &res)
			case CommandAppend:
				kv.ApplyAppend(op, &res)
			case CommandUpdateConfig:
				kv.ApplyUpdateConfig(op, &res)
			case CommandGC:
				kv.ApplyGC(op, &res)
			}

			if ch, ok := kv.resultCh[applymsg.CommandIndex]; ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				kv.resultCh[applymsg.CommandIndex] = make(chan Res, 1)
			}
			kv.resultCh[applymsg.CommandIndex] <- res

			//the Raft state size is approaching maxraftsize, it should save a snapshot,
			//and tell the Raft library that it has snapshotted, so that Raft can discard old log entries.
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				//fmt.Println("reach maxraftstate")
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.statemachine)
				e.Encode(kv.lastopack)
				snapshot := w.Bytes()
				go kv.rf.CreateSnapshot(applymsg.CommandIndex, snapshot)
			}
		}

		kv.mu.Unlock()
	}
}

//have your server detect when a configuration happens and start accepting requests whose keys match shards that it now owns.
func (kv *ShardKV) UpdateConfig() {
	for {
		if _, isleader := kv.rf.GetState(); isleader {
			//I am the leader,update the config
			// get the latestconfig
			lastconfig := kv.mck.Query(-1)

		}
		time.Sleep(TimeoutConfigUpdate)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastopack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Res)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.statemachine[i] = KVmemory{
			Store: make(map[string]string),
		}
	}

	go kv.Applier()
	go kv.UpdateConfig()

	return kv
}

package kvraft

import (
	"lab/src/labgob"
	"lab/src/labrpc"
	"lab/src/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

const TimeoutApply = 240 * time.Millisecond

const (
	CommandPut    = "Put"
	CommandAppend = "Append"
	CommandGet    = "Get"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // "Put" or "Append" or "Get"
	Key       string
	Value     string
	CommandId int64
	ClientId  int64
}

type Res struct {
	Value       string
	Err         Err
	ClientId    int64
	CommandId   int64
	OK          bool
	WrongLeader bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	statemachine StateMachine
	resultCh     map[int]chan Res // logindex对应位置的结果
	lastopack    map[int64]int64  // 记录一个 client 已经处理过的最大 requestId
}

func (kv *KVServer) SubmitCommand(op Op) Res {
	//submit commands to the Raft log using Start()
	//ApplyEntries() to rf.applych->KVSerer.applych
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Res{OK: false}
	}

	kv.mu.Lock()
	_, ok := kv.resultCh[index]
	if !ok {
		kv.resultCh[index] = make(chan Res)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultCh[index]:
		//fmt.Printf("client[%d] command[%d] get data\n", op.ClientId, op.CommandId)
		if op.ClientId == result.ClientId && op.CommandId == result.CommandId {
			return result
		}
		return Res{OK: false}
	case <-time.After(TimeoutApply):
		//fmt.Printf("client[%d] command[%d] timeout\n", op.ClientId, op.CommandId)
		return Res{OK: false}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Command:   CommandGet,
		Key:       args.Key,
		Value:     "",
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

//apply the command sent by client
func (kv *KVServer) Applier() {
	// keep reading applyCh while PutAppend() and Get() handlers submit commands to the Raft log using Start()
	for !kv.killed() {
		applymsg := <-kv.applyCh
		kv.mu.Lock()
		op := applymsg.Command.(Op)
		res := Res{
			ClientId:    op.ClientId,
			Err:         OK,
			CommandId:   op.CommandId,
			OK:          true,
			WrongLeader: false,
		}
		if op.Command == CommandGet {
			res.Value, res.Err = kv.statemachine.Get(op.Key)
		} else if op.Command == CommandPut {
			if _, ok := kv.lastopack[op.ClientId]; !ok {
				res.Err = kv.statemachine.Put(op.Key, op.Value)
			} else {
				if kv.lastopack[op.ClientId] >= op.CommandId {
					res.Err = OK
				} else {
					res.Err = kv.statemachine.Put(op.Key, op.Value)
				}
			}
		} else {
			if _, ok := kv.lastopack[op.ClientId]; !ok {
				res.Err = kv.statemachine.Append(op.Key, op.Value)
			} else {
				if kv.lastopack[op.ClientId] >= op.CommandId {
					res.Err = OK
				} else {
					res.Err = kv.statemachine.Append(op.Key, op.Value)
				}
			}
		}
		kv.lastopack[op.ClientId] = op.CommandId

		if ch, ok := kv.resultCh[applymsg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			kv.resultCh[applymsg.CommandIndex] = make(chan Res, 1)
		}
		kv.resultCh[applymsg.CommandIndex] <- res

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	return
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Res{})

	// You may need initialization code here.
	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		statemachine: &KVmemory{
			store: make(map[string]string),
		},
		resultCh:  make(map[int]chan Res),
		lastopack: make(map[int64]int64),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Applier()

	return kv
}

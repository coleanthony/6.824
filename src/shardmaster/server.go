package shardmaster

import (
	"lab/src/labgob"
	"lab/src/labrpc"
	"lab/src/raft"
	"sync"
	"time"
)

//作用：提供高可用的集群配置管理服务，实现分片的负载均衡，并尽可能少地移动分片。
//记录了每组（Group）ShardKVServer的集群信息和每个分片（shard）服务于哪组（Group）ShardKVServer。
//具体实现通过Raft维护 一个Configs数组。

const (
	CommandJoin  = "Join"
	CommandLeave = "Leave"
	CommandMove  = "Move"
	CommandQuery = "Query"
)

const TimeoutApply = 240 * time.Millisecond

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs   []Config         // indexed by config num
	resultCh  map[int]chan Res // logindex对应位置的结果
	lastopack map[int64]int64  // 记录一个 client 已经处理过的最大 requestId
}

type Op struct {
	Command string // "Join" "Leave" "Move" "Query"
	//query
	Num int // desired config number
	//move
	Shard int //将数据库子集Shard分配给GID的Group
	GID   int
	//leave
	GIDs []int
	//join
	Servers map[int][]string // new GID -> servers mappings

	CommandId int64
	ClientId  int64
}

type Res struct {
	Config      Config //query result
	Err         Err
	ClientId    int64
	CommandId   int64
	OK          bool
	WrongLeader bool
}

func (sm *ShardMaster) SubmitCommand(op Op) Res {
	//submit commands to the Raft log using Start()
	//ApplyEntries() to rf.applych->KVSerer.applych
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return Res{OK: false}
	}

	sm.mu.Lock()
	_, ok := sm.resultCh[index]
	if !ok {
		sm.resultCh[index] = make(chan Res, 1)
	}
	sm.mu.Unlock()

	select {
	case result := <-sm.resultCh[index]:
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

//Join： 新加入的Group信息，要求在每一个group平衡分布shard，
//即任意两个group之间的shard数目相差不能为1，具体实现每一次找出含有shard数目最多的和最少的，
//最多的给最少的一个，循环直到满足条件为止。坑为：GID = 0 是无效配置，一开始所有分片分配给GID=0，需要优先分配；
//map的迭代时无序的，不确定顺序的话，同一个命令在不同节点上计算出来的新配置不一致，按sort排序之后遍历即可。且 map 是引用对象，需要用深拷贝做复制。
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Command:   CommandJoin,
		Servers:   args.Servers,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)
	//fmt.Println("join")
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Err
}

//Leave： 移除Group，同样别忘记实现均衡，将移除的Group的shard每一次分配给数目最小的Group就行，如果全部删除，别忘记将shard置为无效的0。
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Command:   CommandLeave,
		GIDs:      args.GIDs,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)
	//fmt.Println("leave")
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Err
}

//Move 将数据库子集Shard分配给GID的Group。
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Command:   CommandMove,
		Shard:     args.Shard,
		GID:       args.GID,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)
	//fmt.Println("leave")
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Err
}

//Query： 查询最新的Config信息
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Command:   CommandQuery,
		Num:       args.Num,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)
	//fmt.Println("leave")
	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.Config = res.Config
	reply.Err = res.Err
	reply.WrongLeader = false
}

//apply the command sent by client
func (sm *ShardMaster) Applier() {
	for {
		applymsg := <-sm.applyCh
		sm.mu.Lock()
		op := applymsg.Command.(Op)
		res := Res{
			ClientId:    op.ClientId,
			Err:         OK,
			CommandId:   op.CommandId,
			OK:          true,
			WrongLeader: false,
		}
		if op.Command == CommandJoin {

		} else if op.Command == CommandLeave {

		} else if op.Command == CommandMove {

		} else {
			//query

		}

		if ch, ok := sm.resultCh[applymsg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			sm.resultCh[applymsg.CommandIndex] = make(chan Res, 1)
		}
		sm.resultCh[applymsg.CommandIndex] <- res

		sm.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})
	labgob.Register(Res{})

	sm := &ShardMaster{
		me:        me,
		configs:   make([]Config, 1),
		applyCh:   make(chan raft.ApplyMsg, 100),
		resultCh:  make(map[int]chan Res),
		lastopack: make(map[int64]int64),
	}
	sm.configs[0].Groups = map[int][]string{}
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.Applier()

	return sm
}

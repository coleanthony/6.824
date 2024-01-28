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
const INT_MAX = int(^uint(0) >> 1)

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
	//fmt.Println("ApplyEntries() to rf.applych->KVSerer.applych")
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
	//fmt.Println("send join")
	op := Op{
		Command:   CommandJoin,
		Servers:   args.Servers,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)

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
	//fmt.Println("send leave")
	op := Op{
		Command:   CommandLeave,
		GIDs:      args.GIDs,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)

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
	//fmt.Println("send move")
	op := Op{
		Command:   CommandMove,
		Shard:     args.Shard,
		GID:       args.GID,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)

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
	//fmt.Println("send query")
	op := Op{
		Command:   CommandQuery,
		Num:       args.Num,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	res := sm.SubmitCommand(op)

	if res.OK == false {
		reply.WrongLeader = true
		return
	}
	reply.Config = res.Config
	reply.Err = res.Err
	reply.WrongLeader = false
}

func (sm *ShardMaster) MakeNextConfig() Config {
	//更新config配置
	//println("Make Next Config")
	curconf := sm.configs[len(sm.configs)-1]
	newconf := Config{
		Num:    curconf.Num + 1,
		Shards: curconf.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range curconf.Groups {
		newconf.Groups[k] = v
	}
	return newconf
}

func (sm *ShardMaster) QueryExec(op Op) (Config, Err) {
	//fmt.Println("Query Executing...")
	if op.Num < 0 || op.Num >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1], OK
	}
	return sm.configs[op.Num], OK
}

func (sm *ShardMaster) MoveExec(op Op) Err {
	//fmt.Println("Move Executing...")
	newconf := sm.MakeNextConfig()
	newconf.Shards[op.Shard] = op.GID
	sm.configs = append(sm.configs, newconf)
	return OK
}

func (sm *ShardMaster) LeaveExec(op Op) Err {
	//fmt.Println("Leave Executing...")
	newconf := sm.MakeNextConfig()
	//find the remaining group
	staygid := 0
	for kgid, _ := range newconf.Groups {
		canstay := true
		for _, gid := range op.GIDs {
			if gid == kgid {
				canstay = false
				break
			}
		}
		if canstay {
			staygid = kgid
			break
		}
	}
	//移除Group
	for _, gid := range op.GIDs {
		for i := 0; i < len(newconf.Shards); i++ {
			if newconf.Shards[i] == gid {
				newconf.Shards[i] = staygid
			}
		}
		delete(newconf.Groups, gid)

	}
	sm.ReBalanceGroupShards(&newconf)
	sm.configs = append(sm.configs, newconf)

	return OK
}

func (sm *ShardMaster) JoinExec(op Op) Err {
	//fmt.Println("Join Executing...")
	newconf := sm.MakeNextConfig()
	for gid, servers := range op.Servers {
		newconf.Groups[gid] = servers
		for i := 0; i < len(newconf.Shards); i++ {
			if newconf.Shards[i] == 0 {
				newconf.Shards[i] = gid
			}
		}
	}
	sm.ReBalanceGroupShards(&newconf)
	sm.configs = append(sm.configs, newconf)

	return OK
}

func (sm *ShardMaster) ReBalanceGroupShards(conf *Config) {
	//should move as few shards as possible
	//fmt.Println("Do rebalance...")
	serverconf := make(map[int]int)
	serverconfmap := make(map[int][]int)
	for k, _ := range conf.Groups {
		serverconf[k] = 0
	}
	for i := 0; i < len(conf.Shards); i++ {
		gid := conf.Shards[i]
		serverconf[gid]++
		if _, ok := serverconfmap[gid]; !ok {
			serverconfmap[gid] = make([]int, 0)
		}
		serverconfmap[gid] = append(serverconfmap[gid], i)
	}
	for {
		//将最多的移向最少的，直到每个之间相差不到1
		maxnum, maxgid := 0, 0
		minnum, mingid := INT_MAX, 0
		for gid, shardnums := range serverconf {
			if maxnum < shardnums {
				maxnum = shardnums
				maxgid = gid
			}
			if minnum > shardnums {
				minnum = shardnums
				mingid = gid
			}
		}
		if maxnum-minnum <= 1 {
			break
		}
		serverconf[maxgid]--
		serverconf[mingid]++
		serverconfmap[mingid] = append(serverconfmap[mingid], serverconfmap[maxgid][len(serverconfmap[maxgid])-1])
		serverconfmap[maxgid] = serverconfmap[maxgid][:len(serverconfmap[maxgid])-1]
	}
	//for gid, shardnum := range serverconf {
	//	fmt.Printf("gid:%d  shardnum:%d\n", gid, shardnum)
	//}
	//fmt.Println()
	//for k, v := range serverconfmap {
	//	fmt.Printf("gid %d ", k)
	//	fmt.Println(v)
	//}
	//fmt.Println()
	//update conf.Shards
	for gid, shards := range serverconfmap {
		for _, shard := range shards {
			conf.Shards[shard] = gid
		}
	}
	//for _, gid := range conf.Shards {
	//	fmt.Printf("%d ", gid)
	//}
	//fmt.Println()
}

//apply the command sent by client
func (sm *ShardMaster) Applier() {
	for {
		//fmt.Println("Do apply...")
		applymsg := <-sm.applyCh
		sm.mu.Lock()
		//fmt.Println(applymsg.CommandValid)
		op := applymsg.Command.(Op)
		res := Res{
			ClientId:    op.ClientId,
			Err:         OK,
			CommandId:   op.CommandId,
			OK:          true,
			WrongLeader: false,
		}
		if op.Command == CommandJoin {
			if _, ok := sm.lastopack[op.ClientId]; !ok {
				res.Err = sm.JoinExec(op)
				sm.lastopack[op.ClientId] = op.CommandId
			} else {
				if sm.lastopack[op.ClientId] >= op.CommandId {
					res.Err = OK
				} else {
					sm.lastopack[op.ClientId] = op.CommandId
					res.Err = sm.JoinExec(op)
				}
			}
		} else if op.Command == CommandLeave {
			if _, ok := sm.lastopack[op.ClientId]; !ok {
				res.Err = sm.LeaveExec(op)
				sm.lastopack[op.ClientId] = op.CommandId
			} else {
				if sm.lastopack[op.ClientId] >= op.CommandId {
					res.Err = OK
				} else {
					sm.lastopack[op.ClientId] = op.CommandId
					res.Err = sm.LeaveExec(op)
				}
			}
		} else if op.Command == CommandMove {
			if _, ok := sm.lastopack[op.ClientId]; !ok {
				res.Err = sm.MoveExec(op)
				sm.lastopack[op.ClientId] = op.CommandId
			} else {
				if sm.lastopack[op.ClientId] >= op.CommandId {
					res.Err = OK
				} else {
					sm.lastopack[op.ClientId] = op.CommandId
					res.Err = sm.MoveExec(op)
				}
			}

		} else if op.Command == CommandQuery {
			//query
			sm.lastopack[op.ClientId] = op.CommandId
			res.Config, res.Err = sm.QueryExec(op)
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
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Res{})

	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	labgob.Register(Op{})
	labgob.Register(Res{})

	sm.resultCh = make(map[int]chan Res)
	sm.lastopack = make(map[int64]int64)

	go sm.Applier()
	//fmt.Printf("start shardmaster %d\n", me)
	return sm
}

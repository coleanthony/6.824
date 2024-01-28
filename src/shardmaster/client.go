package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"lab/src/labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mu        sync.Mutex
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		clientId:  nrand(),
		commandId: 0,
	}
	return ck
}

//Query： 查询最新的Config信息
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
	}

	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.commandId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//Join： 新加入的Group信息，要求在每一个group平衡分布shard，
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers

	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.commandId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//Leave： 移除Group，同样别忘记实现均衡，将移除的Group的shard每一次分配给数目最小的Group就行，如果全部删除，别忘记将shard置为无效的0。
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids

	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.commandId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//Move 将数据库子集Shard分配给GID的Group。
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid

	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.commandId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package shardkv

import "lab/src/shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNoyReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int64
	ClientId  int64
}

type PutAppendReply struct {
	Err         Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId int64
	ClientId  int64
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}

type TransferShardArgs struct {
	ShardIds []int
	Num      int
}

type TransferShardReply struct {
	Err       Err
	Data      [shardmaster.NShards]KVmemory
	LastOpAck map[int64]int64
}

type GarbageCollectionArgs struct {
	Num     int
	ShardId int
}

type GarbageCollectionReply struct {
	Err Err
}

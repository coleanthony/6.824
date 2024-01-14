package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"lab/src/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mu        sync.Mutex
	clientId  int64
	leaderId  int64
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
		commandId: 0,
		leaderId:  0,
		clientId:  nrand(),
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
	}
	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.mu.Unlock()
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrNoKey || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.mu.Lock()
		ck.commandId++
		ck.mu.Unlock()
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	ck.mu.Lock()
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.mu.Unlock()
	reply := &PutAppendReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrNoKey || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.mu.Lock()
		ck.commandId++
		ck.mu.Unlock()
		return

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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
	Key       string
	CommandId int64
	ClientId  int64
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}

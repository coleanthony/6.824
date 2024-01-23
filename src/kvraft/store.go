package kvraft

//type StateMachine interface {
//	Get(key string) (string, Err)
//	Put(key string, val string) Err
//	Append(key string, val string) Err
//}

//最简单的内存版本的 KV 状态机
type KVmemory struct {
	Store map[string]string
}

func (kv *KVmemory) Get(key string) (string, Err) {
	res, ok := kv.Store[key]
	if ok {
		return res, OK
	}
	return "", ErrNoKey
}

func (kv *KVmemory) Put(key string, val string) Err {
	kv.Store[key] = val
	return OK
}

func (kv *KVmemory) Append(key string, val string) Err {
	kv.Store[key] += val
	return OK
}

package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yunuskilicdev/distributedsystems/src/labgob"
	"github.com/yunuskilicdev/distributedsystems/src/labrpc"
	"github.com/yunuskilicdev/distributedsystems/src/raft"
)

const Debug = 0

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
	Command   string // "get" | "put" | "append"
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type OpResponse struct {
	Op        Op
	ClientId  int64
	RequestId int64
	Err       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	ack  map[int64]int64
	ch   map[int]chan OpResponse
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "Get"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.ch[index]; !ok {
		kv.ch[index] = make(chan OpResponse, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.ch[index]:
		if isMatch(entry, result) {
			reply.Value = kv.data[result.Op.Key]
			reply.Err = OK
			return
		}
	case <-time.After(250 * time.Millisecond):
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{}
	entry.Command = args.Op
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key
	entry.Value = args.Value

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.ch[index]; !ok {
		kv.ch[index] = make(chan OpResponse, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.ch[index]:
		if isMatch(entry, result) {
			reply.Err = OK
			return
		}
	case <-time.After(250 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) maintanance() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		response := kv.applyOp(op, msg.CommandIndex)
		if ch, ok := kv.ch[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			kv.ch[msg.CommandIndex] = make(chan OpResponse, 1)
		}
		kv.ch[msg.CommandIndex] <- response
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOp(op Op, commandIndex int) OpResponse {

	result := OpResponse{}
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId
	result.Op = op
	switch op.Command {
	case "Get":
		if _, ok := kv.data[op.Key]; ok {
			result.Err = OK
		} else {
			result.Err = ErrNoKey
		}
	case "Put":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] = op.Value
		}
		result.Err = OK
	case "Append":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] += op.Value
		}
		result.Err = OK
	}
	kv.ack[op.ClientId] = op.RequestId
	return result
}

func isMatch(entry Op, response OpResponse) bool {
	return entry.ClientId == response.ClientId && entry.RequestId == response.RequestId
}

func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestId, ok := kv.ack[op.ClientId]
	if ok {
		return lastRequestId >= op.RequestId
	}
	return false
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.ch = make(map[int]chan OpResponse)
	go kv.maintanance()
	return kv
}

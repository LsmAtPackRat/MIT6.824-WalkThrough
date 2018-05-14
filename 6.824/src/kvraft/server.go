package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type CommandType int

const (
    CmdPut CommandType = iota
    CmdAppend
    CmdGet
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Type CommandType    // actually int type.
    Key   string
    Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    // lsm : K/V mapping
    kvmappings map[string]string
    // lsm :
    getCh chan GetReply
}

// Get RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    key := args.Key
    kv.mu.Lock()
    // construct an Op to passed to kv.rf.Start()
    var op Op
    op.Type = CmdGet
    op.Key = key
    index, term, isLeader := kv.rf.Start(op)   // start an agreement.

    if isLeader {
        reply.WrongLeader = false
    } else {
        reply.WrongLeader = true
        // reply.Err & reply.Value will not need to be filled.
        return
    }

    // we need to update some data structures so that apply knows to poke us later.
    kv.getCh = make(chan GetReply)
    kv.mu.Unlock()

    // wait for apply() to poke us.
    reply <- getCh
    return
}


// PutAppend RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)  // this kv.rf will be associated with other kv.rf peers.

	// You may need initialization code here.
    // start a long-running goroutine to continously read from applyCh.
    go func() {
        // read command from the kv.applyCh indefinately.
        for m := range kv.applyCh {
            // m is an ApplyMsg
            err_msg := ""
            if m.CommandValid == false {
                // ignore other types of ApplyMsg
            } else {
                // cmd is the type interface{}
                command := m.Command
                command_index := m.CommandIndex
                // apply this cmd.
                kv.apply(command_index, command)
            }
        }
    }()

	return kv
}

// apply the cmd to the service.
func (kv *KVServer) apply(command_index int, command interface{}) {
    kv.mu.Lock()

    // cmd contains all the data to execute a command.
    op := (command).(Op)
    cmd_type := op.Type

    switch cmd_type {
    case CmdGet:
        // do the get.
        value, ok := kv.kvmappings[op.Key]
        // see who was listening for this index.
        var reply GetReply
        reply.Value = 
        reply.Err = 
        reply.WrongLeader = 
        // poke them all with the result of the operation. 
        kv.getCh <- reply
    case CmdPut:
    case CmdAppend:
    }

    kv.mu.Unlock()
}






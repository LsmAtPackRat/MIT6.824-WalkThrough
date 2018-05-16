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
    Id    int     // uniquely identify client operations.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kvmappings map[string]string
    getCh chan GetReply
    putAppendCh chan PutAppendReply
    opId   int
}

// Get RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server.go - Get(" + args.Key + ")")
    // Your code here.
    key := args.Key
    kv.mu.Lock()
    // construct an Op to passed to kv.rf.Start()
    var op Op
    op.Type = CmdGet
    op.Key = key
    op.Id = kv.opId + 1    // identify this op
    //index, term, isLeader := kv.rf.Start(op)   // start an agreement.
    _, _, isLeader := kv.rf.Start(op)   // start an agreement.
    // if op is committed, it should appear at index in kv.rf.log.
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    }

    // we need to update some data structures so that apply knows to poke us later.
    kv.mu.Unlock()

    // wait for apply() to poke us.
    var temp_reply GetReply
    temp_reply = <-kv.getCh
    reply.WrongLeader = false
    reply.Err = temp_reply.Err
    reply.Value = temp_reply.Value
    return
}


// PutAppend RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    DPrintf("server.go - server-%d PutAppend(" + args.Key + ", " + args.Value + ", " + args.Op + ")", kv.me)
	// Your code here.
    if args.Op == "Put" || args.Op == "Append" {
        kv.mu.Lock()
        key := args.Key
        value := args.Value
        var op Op
        op.Key = key
        op.Value = value
        if args.Op == "Put" {
            op.Type = CmdPut
        } else {
            op.Type = CmdAppend
        }
        _, _, isLeader := kv.rf.Start(op)
        if !isLeader {
            reply.WrongLeader = true
            kv.mu.Unlock()
            return
        }
        // we need to update some data structures so that apply knows to poke us later.
        kv.mu.Unlock()

        // wait for apply() to poke us.
        var temp_reply PutAppendReply
        temp_reply = <-kv.putAppendCh
        reply.WrongLeader = false
        reply.Err = temp_reply.Err
    } else {
        log.Fatal("PutAppend() get a wrong args!")
    }
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
    DPrintf("server.go - StartKVServer()!")
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
    kv.opId = 0
    kv.kvmappings = make(map[string]string)
    kv.getCh = make(chan GetReply)
    kv.putAppendCh = make(chan PutAppendReply)

	kv.applyCh = make(chan raft.ApplyMsg)
    // a kvserver contains a raft.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)  // this kv.rf will be associated with other kv.rf peers.

	// You may need initialization code here.
    // start a long-running goroutine to continously read from applyCh.
    go func() {
        // read command from the kv.applyCh indefinately.
        for msg := range kv.applyCh {
            // m is an ApplyMsg
            if msg.CommandValid == false {
                // ignore other types of ApplyMsg
                DPrintf("server.go - ignore other types of ApplyMsg!")
            } else {
                // cmd is the type interface{}
                command := msg.Command
                command_index := msg.CommandIndex
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
    op, ok := (command).(Op)
    if !ok {
        log.Fatal("server.go - apply() cannot convert command interface{} to Op struct")
    }

    cmd_type := op.Type
    DPrintf("server.go - server-%d apply command %d", kv.me, cmd_type)

    switch cmd_type {
    case CmdGet:
        // do the get.
        value, ok := kv.kvmappings[op.Key]
        // see who was listening for this index.
        var reply GetReply
        if ok {
            reply.Value = value
            reply.Err = OK
        } else {
            // the key is not exist!
            reply.Err = ErrNoKey
        }
        // poke them all with the result of the operation. 
        // NOTE: if we don't unlock() before write to the channel, it'll cause deadlock. 
        // if other kvserver call kv.rf.Start(), this kvserver will call  apply() too.
        // then it could block here with a lock held in hand. But Get() should get the lock and then unlock and then read the channel, so, deadlock!
        kv.mu.Unlock()
        kv.getCh <- reply
    case CmdPut:
        // replace the value for a particular key
        var reply PutAppendReply
        kv.kvmappings[op.Key] = op.Value
        reply.Err = OK
        kv.mu.Unlock()
        kv.putAppendCh <- reply
    case CmdAppend:
        value, ok := kv.kvmappings[op.Key]
        var reply PutAppendReply
        if ok {
            kv.kvmappings[op.Key] = value + op.Value
        } else {
            kv.kvmappings[op.Key] = op.Value
        }
        reply.Err = OK
        kv.mu.Unlock()
        kv.putAppendCh <- reply
    default:
        kv.mu.Unlock()
    }

}






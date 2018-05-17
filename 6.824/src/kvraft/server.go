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
    SerialNumber int64
}

type channels []chan interface{}

type KVServer struct {
	mu      sync.Mutex
    muservice   sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kvmappings map[string]string
    waitMap map[int]chan interface{}   // index -> channel
    waitCommandMap map[int]int64       // command index to serial number.
    servedRequest map[int64]interface{}    // record which command is served before, and the value is the reply.
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
    op.SerialNumber = args.SerialNumber
    //index, term, isLeader := kv.rf.Start(op)   // start an agreement.
    index, _, isLeader := kv.rf.Start(op)   // start an agreement.

    // if op is committed, it should appear at index in kv.rf.log.
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    }
    // we need to update some data structures so that apply knows to poke us later.
    kv.waitMap[index] = make(chan interface{})
    kv.waitCommandMap[index] = op.SerialNumber
    kv.mu.Unlock()

    // wait for apply() to poke us.
    temp_reply := (<-kv.waitMap[index]).(GetReply)
    reply.WrongLeader = temp_reply.WrongLeader
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
        op.SerialNumber = args.SerialNumber
        if args.Op == "Put" {
            op.Type = CmdPut
        } else {
            op.Type = CmdAppend
        }
        index, _, isLeader := kv.rf.Start(op)
        if !isLeader {
            reply.WrongLeader = true
            kv.mu.Unlock()
            return
        }

        // we need to update some data structures so that apply knows to poke us later.
        kv.waitMap[index] = make(chan interface{})
        kv.waitCommandMap[index] = op.SerialNumber
        kv.mu.Unlock()

        // wait for apply() to poke us.
        temp_reply := (<-kv.waitMap[index]).(PutAppendReply)
        reply.WrongLeader = temp_reply.WrongLeader
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
    kv.kvmappings = make(map[string]string)
    kv.waitMap = make(map[int]chan interface{})
    kv.waitCommandMap = make(map[int]int64)
    kv.servedRequest = make(map[int64]interface{})

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
    switch cmd_type {
    case CmdGet:
        // NOTE: if we don't unlock() before write to the channel, it'll cause deadlock. 
        // if other kvserver call kv.rf.Start(), this kvserver will call  apply() too.
        // then it could block here with a lock held in hand. But Get() should get the lock and then unlock and then read the channel, so, deadlock!
        kv.mu.Unlock()
        // check whether to poke the Get().
        if channel, ok := kv.waitMap[command_index]; ok {
            // someone is waiting for the result!
            var reply GetReply
            if kv.waitCommandMap[command_index] == op.SerialNumber {
                value, ok := kv.kvmappings[op.Key]
                if ok {
                    reply.Value = value
                    reply.Err = OK
                    reply.WrongLeader = false
                } else {
                    reply.Value = ""
                    reply.Err = ErrNoKey
                    reply.WrongLeader = false
                }
                if _, ok := kv.servedRequest[op.SerialNumber]; !ok {
                    // haven't served before.
                    kv.servedRequest[op.SerialNumber] = reply
                } else {
                    // served before.
                    // FIXME: why g
                    //reply = kv.servedRequest[op.SerialNumber].(GetReply)
                }
            } else {
                // hehe , send an error.
                reply.Value = ""
                reply.Err = ErrNoKey
                reply.WrongLeader = true
            }
            channel<-reply
        }
    case CmdPut:
        //
        if _, ok := kv.servedRequest[op.SerialNumber]; !ok {
            // we haven't serve for this command. apply this command.
            // replace the value for a particular key
            kv.kvmappings[op.Key] = op.Value
            kv.servedRequest[op.SerialNumber] = true
        }
        kv.mu.Unlock()
        // check whether to poke the PutAppend().
        if channel, ok := kv.waitMap[command_index]; ok {
            // someone is waiting for this index.
            var reply PutAppendReply
            if kv.waitCommandMap[command_index] == op.SerialNumber {
                reply.Err = OK
                reply.WrongLeader = false
            } else {
                // hehe, send an error to waiter.
                reply.Err = ErrNoKey
                reply.WrongLeader = true
            }
            channel<-reply
        }
    case CmdAppend:
        if _, ok := kv.servedRequest[op.SerialNumber]; !ok {
            // we haven't serve for this command. apply this command.
            // replace the value for a particular key
            value, ok := kv.kvmappings[op.Key]
            if ok {
                kv.kvmappings[op.Key] = value + op.Value
            } else {
                kv.kvmappings[op.Key] = op.Value
            }
            kv.servedRequest[op.SerialNumber] = true
        }
        kv.mu.Unlock()
        // check whether to poke the PutAppend().
        if channel, ok := kv.waitMap[command_index]; ok {
            // someone is waiting for this index.
            var reply PutAppendReply
            if kv.waitCommandMap[command_index] == op.SerialNumber {
                reply.Err = OK
                reply.WrongLeader = false
            } else {
                // hehe, send an error to waiter.
                reply.Err = ErrNoKey
                reply.WrongLeader = true
            }
            channel<-reply
        }
    default:
        kv.mu.Unlock()
    }
}






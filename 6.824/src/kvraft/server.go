package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
    "bytes"
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
	Type         CommandType // actually int type.
	Key          string
	Value        string
	SerialNumber int64
}

type ResultItem struct {
	reply         interface{} // maybe GetReply or PutAppendReply
	term          int         // the term in which the leader Start() this Op.
	serial_number int64       // indicate which Op is the owner of this result.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmappings    map[string]string       // the store is based on this map.
	waitCh        map[int]chan ResultItem // index -> channel
	servedRequest map[int64]interface{}   // record which command is served before, and the value is the reply.
}

// Get RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server.go - server-%d Get("+args.Key+")", kv.me)
	// Your code here.
	key := args.Key
	kv.mu.Lock()

	// construct an Op to passed to kv.rf.Start()
	var op Op
	op.Type = CmdGet
	op.Key = key
	op.SerialNumber = args.SerialNumber
	index, term, isLeader := kv.rf.Start(op) // start an agreement.

	// if op is committed, it should appear at index in kv.rf.log.
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	// we need to update some data structures so that apply knows to poke us later.
	wait_ch := make(chan ResultItem, 10)
	kv.waitCh[index] = wait_ch
	kv.mu.Unlock()

	// start a goroutine to check whether the term is changed?
	done_check_leadership := int32(0)
	go kv.checkLeadership(index, term, &done_check_leadership)

	// wait for apply() to poke us.
	result_item := (<-wait_ch)
	if result_item.serial_number != op.SerialNumber || result_item.term != term {
		reply.WrongLeader = true
	} else {
		temp_reply := (result_item.reply).(GetReply)
		reply.WrongLeader = false
		reply.Err = temp_reply.Err
		reply.Value = temp_reply.Value
	}
	atomic.StoreInt32(&done_check_leadership, 1) // I'm not sure, whether a w/r tear could happen without atomic. It's up to Go's memory model.
	return
}

// PutAppend RPC handler. You should enter an Op in the Raft log using Start().
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server.go - server-%d PutAppend("+args.Key+", "+args.Value+", "+args.Op+")", kv.me)
	// Your code here.
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
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	// we need to update some data structures so that apply knows to poke us later.
	wait_ch := make(chan ResultItem, 10)
	kv.waitCh[index] = wait_ch // cover the previous mapping if exists.
	kv.mu.Unlock()

	// start a goroutine to check whether the term is changed?
	done_check_leadership := int32(0)
	go kv.checkLeadership(index, term, &done_check_leadership)
	// the leader could not be the leader any more.
	// wait for apply() to poke us.
	result_item := (<-wait_ch)
	if result_item.serial_number != op.SerialNumber || result_item.term != term {
		reply.WrongLeader = true
	} else {
		temp_reply := (result_item.reply).(PutAppendReply)
		reply.Err = temp_reply.Err
		reply.WrongLeader = false
	}
	atomic.StoreInt32(&done_check_leadership, 1)
}

// check whether the server lose its leadership in an infinate loop.
func (kv *KVServer) checkLeadership(index int, term int, done *int32) {
	for atomic.LoadInt32(done) == 0 {
		kv.mu.Lock()
		curr_term, curr_isleader := kv.rf.GetState()
		if curr_term != term || !curr_isleader {
			// notify the server to return.
			// construct an error result and send to waitMap channel.
			var result_item ResultItem
			result_item.term = curr_term
			if channel, ok := kv.waitCh[index]; ok {
				// apply() haven't write to the channel.
				delete(kv.waitCh, index) // apply will not write to this channel.
				channel <- result_item
			}
			kv.mu.Unlock()
			return
		} else {
			if _, ok := kv.waitCh[index]; !ok {
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(50))
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
	kv.waitCh = make(map[int]chan ResultItem)
	kv.servedRequest = make(map[int64]interface{})

	kv.applyCh = make(chan raft.ApplyMsg)
	// a kvserver contains a raft.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // this kv.rf will be associated with other kv.rf peers.

	// You may need initialization code here.
	// start a long-running goroutine to continously read from applyCh.
	go func() {
		// read command from the kv.applyCh indefinately.
		for msg := range kv.applyCh {
			// m is an object of type ApplyMsg.
			if msg.CommandValid == false {
				// ignore other types of ApplyMsg
				DPrintf("server.go - ignore other types of ApplyMsg!")
			} else {
				// cmd is the type interface{}
				command := msg.Command
				command_index := msg.CommandIndex
				command_term := msg.CommandTerm
				// apply this cmd.
				kv.apply(command_index, command_term, command)

                // check whether we need to produce a snapshot.
                if kv.rf.StateOversize(maxraftstate) {
                    w := new(bytes.Buffer)
                    e := labgob.NewEncoder(w)
                    e.Encode(kv.kvmappings)
                    snapshot := w.Bytes()
                    kv.rf.SaveSnapshotAndTrimLog(snapshot)
                }
			}
		}
	}()

	return kv
}

// apply the cmd to the service.
func (kv *KVServer) apply(command_index int, command_term int, command interface{}) {
	DPrintf("apply()!")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// cmd contains all the data to execute a command.
	op, ok := (command).(Op)
	if !ok {
		log.Fatal("server.go - apply() cannot convert command interface{} to Op struct")
	}

	cmd_type := op.Type
	var result_item ResultItem
	switch cmd_type {
	case CmdGet:
		// NOTE: if we don't unlock() before write to the channel, it'll cause deadlock.
		// if other kvserver call kv.rf.Start(), this kvserver will call  apply() too.
		// then it could block here with a lock held in hand. But Get() should get the lock and then unlock and then read the channel, so, deadlock!
		// check whether to poke the Get().
		var reply GetReply
		if prev_reply, ok := kv.servedRequest[op.SerialNumber]; ok {
			result_item.reply = prev_reply
		} else {
			value, ok := kv.kvmappings[op.Key]
			if ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			kv.servedRequest[op.SerialNumber] = reply
			result_item.reply = reply
		}
	case CmdPut:
		if _, ok := kv.servedRequest[op.SerialNumber]; !ok {
			// we haven't serve for this command. apply this command.
			// replace the value for a particular key
			kv.kvmappings[op.Key] = op.Value
			kv.servedRequest[op.SerialNumber] = true
		}
		var reply PutAppendReply
		reply.Err = OK
		result_item.reply = reply
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
		var reply PutAppendReply
		reply.Err = OK
		result_item.reply = reply
	}

	result_item.term = command_term
	result_item.serial_number = op.SerialNumber
	if channel, ok := kv.waitCh[command_index]; ok {
		channel <- result_item
		delete(kv.waitCh, command_index)
	}
}

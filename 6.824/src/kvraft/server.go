package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
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
	SerialNumber int64 // identifier
}

type ResultItem struct {
	Reply         interface{} // could be GetReply or PutAppendReply
	Term          int         // the term in which the leader Start() this Op.
	Serial_number int64       // indicate which Op is the owner of this result.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Kvmappings    map[string]string       // the store is based on this map.
    GetServedResults map[int64]string     // serialnumber : value
    PutAppendServedResults map[int64]bool // bool just be true, isn't used.
	waitCh        map[int]chan ResultItem // index -> channel
	maxIndex      int
}

// all of the Get/PutAppend requests are routed to the leader. If the raft peer associated with the server is not the leader,
// the Start() will return and the requester will find that.
// It will continuously request another server for the same request until it is the leader.

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

	DPrintf("server.go - Get()-server-%d thinks he is the leader now!", kv.me)

	// we need to update some data structures so that apply knows to poke us later.
	wait_ch := make(chan ResultItem, 10)
	kv.waitCh[index] = wait_ch
	kv.mu.Unlock()

	// start a goroutine to check whether the term is changed?
	done_check_leadership := int32(0)
	go kv.checkLeadership(index, term, &done_check_leadership)

	// wait for apply() to poke us.
	result_item := (<-wait_ch)
	if result_item.Serial_number != op.SerialNumber || result_item.Term != term {
		reply.WrongLeader = true
	} else {
		temp_reply := (result_item.Reply).(GetReply)
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

	DPrintf("server.go - PutAppend()-server-%d thinks he is the leader now!", kv.me)

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
	if result_item.Serial_number != op.SerialNumber || result_item.Term != term {
		reply.WrongLeader = true
	} else {
		temp_reply := (result_item.Reply).(PutAppendReply)
		reply.Err = temp_reply.Err
		reply.WrongLeader = false
	}
	atomic.StoreInt32(&done_check_leadership, 1)
}

// check whether the server lost its leadership in an infinate loop.
func (kv *KVServer) checkLeadership(index int, term int, done *int32) {
	for atomic.LoadInt32(done) == 0 {
		kv.mu.Lock()
		curr_term, curr_isleader := kv.rf.GetState()
		if curr_term != term || !curr_isleader {
			// notify the server to return.
			// construct an error result and send to waitMap channel.
			var result_item ResultItem
			result_item.Term = curr_term // this will help the Get/PutAppend to find that the leadership is lost.
			if channel, ok := kv.waitCh[index]; ok {
				// apply() haven't write to the channel.
				delete(kv.waitCh, index) // apply will not write to this channel.
				channel <- result_item   // wake up the requester waiting at the index.
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

// unserialize the data(byte array) and cover the KVServer's state with it.
func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvmappings map[string]string
    //var getServedResults map[int64]string
    var putAppendServedResults map[int64]bool
	var snapshot_info raft.Snapshot

	if d.Decode(&snapshot_info) != nil {
		DPrintf("server.go-readSnapshot()-Decode snapshot_info error!")
	} else if d.Decode(&kvmappings) != nil {
		DPrintf("server.go-readSnapshot()-Decode kvmappings error!")
		//log.Fatal(err)
	//} else if err := d.Decode(&getServedResults); err != nil {
	//	DPrintf("server.go-readSnapshot()-Decode getServedResults error!")
	} else if err := d.Decode(&putAppendServedResults); err != nil {
		DPrintf("server.go-readSnapshot()-Decode putAppendServedResults error!")
	} else {
	    DPrintf("kv-%d readSnapshot() LastIncludedIndex = %d", kv.me, snapshot_info.LastIncludedIndex)
	    if kv.maxIndex >= snapshot_info.LastIncludedIndex {
		    return
	    }
	    kv.Kvmappings = kvmappings
        //kv.GetServedResults = getServedResults
        kv.PutAppendServedResults = putAppendServedResults
	}
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
	labgob.Register(ResultItem{})
	labgob.Register(map[string]string{})
	labgob.Register(map[int64]ResultItem{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.Kvmappings = make(map[string]string)
	kv.waitCh = make(map[int]chan ResultItem)
    kv.GetServedResults = make(map[int64]string)
    kv.PutAppendServedResults = make(map[int64]bool)
	kv.maxIndex = 0
	// read snapshot from the persisted state.
	kv.readSnapshot(persister.ReadSnapshot())

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
				// get a snapshot from Raft. If receive a snapshot, the raft peer must not be the leader.
				DPrintf("kv-%d get a snapshot from kv.applyCh!", kv.me)
				kv.applySnapshot(msg.Snapshot) // leader will not receive a snapshot, right?
			} else {
				// cmd is the type interface{}
				command := msg.Command
				command_index := msg.CommandIndex
				if command_index > kv.maxIndex {
					kv.maxIndex = command_index
				}
				command_term := msg.CommandTerm
				// apply this cmd.
				kv.apply(command_index, command_term, command)

				// snapshot switch.  maxraftstate == -1: off, otherwise: on.
				if maxraftstate != -1 {
					// check whether we need to produce a snapshot.
					if kv.rf.StateOversize(maxraftstate) {
						DPrintf("server.go - kv-%d's Raft state is too big, make a snapshot, command_index = %d. state's size = %d", kv.me, command_index, persister.RaftStateSize())
						kv.mu.Lock()
						var snapshot_info raft.Snapshot
						snapshot_info.LastIncludedIndex = command_index
						snapshot_info.LastIncludedTerm = command_term
						w := new(bytes.Buffer)
						e := labgob.NewEncoder(w)
						e.Encode(snapshot_info)
						e.Encode(kv.Kvmappings)
                        //e.Encode(kv.GetServedResults)
                        e.Encode(kv.PutAppendServedResults)
						snapshot := w.Bytes()
						kv.mu.Unlock()
						DPrintf("kv-%d will call SaveSnapshotAndTrimLog()", kv.me)
						kv.rf.SaveSnapshotAndTrimLog(snapshot, command_index)
					}
				}
			}
		}
	}()

	return kv
}

// apply a snapshot.
func (kv *KVServer) applySnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kv-%d applySnapshot()!", kv.me)
	kv.readSnapshot(snapshot)
}

// apply the cmd to the service.
func (kv *KVServer) apply(command_index int, command_term int, command interface{}) {
	kv.mu.Lock()
	DPrintf("kv-%d apply()!", kv.me)
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
		//if prev_result, ok := kv.GetServedResults[op.SerialNumber]; ok {
		/*if _, ok := kv.GetServedResults[op.SerialNumber]; ok {
			DPrintf("kv-%d CmdGet served request!", kv.me)
            //reply.Value = prev_result
            reply.Value = kv.Kvmappings[op.Key]
            reply.Err = OK
			result_item.Reply = reply
		} else {*/
			value, ok := kv.Kvmappings[op.Key]
			if ok {
				reply.Value = value
				reply.Err = OK
			} else {
				DPrintf("kv-%d CmdGet ErrNoKey!", kv.me)
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			result_item.Reply = reply
		//	kv.GetServedResults[op.SerialNumber] = value
		//}
	case CmdPut:
		var reply PutAppendReply
		if _, ok := kv.PutAppendServedResults[op.SerialNumber]; !ok {
			// we haven't serve for this command. apply this command.
			// replace the value for a particular key
			kv.Kvmappings[op.Key] = op.Value
			kv.PutAppendServedResults[op.SerialNumber] = true
		}
		reply.Err = OK
		result_item.Reply = reply
	case CmdAppend:
		var reply PutAppendReply
		DPrintf("PutAppend() Append : " + op.Value)
		if _, ok := kv.PutAppendServedResults[op.SerialNumber]; !ok {
			// we haven't serve for this command. apply this command.
			// replace the value for a particular key
			value, ok := kv.Kvmappings[op.Key]
			if ok {
				kv.Kvmappings[op.Key] = value + op.Value
			} else {
				kv.Kvmappings[op.Key] = op.Value
			}
			kv.PutAppendServedResults[op.SerialNumber] = true
		}
		reply.Err = OK
		result_item.Reply = reply
	}

	result_item.Term = command_term
	result_item.Serial_number = op.SerialNumber
	if channel, ok := kv.waitCh[command_index]; ok {
		channel <- result_item
		delete(kv.waitCh, command_index)
	}
}

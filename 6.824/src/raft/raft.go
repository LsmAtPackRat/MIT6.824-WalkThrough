package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

//import "fmt"
import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
// the Snapshot stucture

type Snapshot struct {
     LastIncludedIndex int
     LastIncludedTerm int
 }


type ApplyMsg struct {
	CommandValid bool
	Command      interface{} // Command contains all the things to execute the command. not just a int.
	CommandIndex int
	CommandTerm  int

	// if CommandValid == false, means that this ApplyMsg is a snapshot.
	Snapshot []byte
}

// A Log Entry
type LogEntry struct {
	Term    int
	Command interface{}
}

// use RaftState to indicate the state of a raft peer
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm   int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int        // candidateId that received vote in current term (or null if none)
	log           []LogEntry // log entries, each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	firstLogIndex int        // Lab3B, the index of the first element in log.

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int       // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int       // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	repCount   map[int]int // repCount[i] is the count of command-i replicated at currentTerm.

	state                    RaftState // Follower/Candidate/Leader
	voteCount                int
	resetElectionCh          chan int      // used to notify the peer to reset its election timeout.
	electionTimeoutStartTime time.Time     // nanosecond precision. used for election timeout.
	electionTimeoutInterval  time.Duration // nanosecond count.
	nonleaderCh              chan bool     // block/unblock nonleader's election timeout long-running goroutine.
	leaderCh                 chan bool     // block/unblock leader's heartbeat long-running goroutine.
	applyCh                  chan ApplyMsg
	canApplyCh               chan bool // if can apply command, write to this channel to notify the goroutine.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstLogIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var firstLogIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&firstLogIndex) != nil { //Decode return value is err, err == nil means okay!
		DPrintf("raft.go-readPersist()-Decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
        if firstLogIndex > 0 {
		    rf.firstLogIndex = firstLogIndex
        } else {
            rf.firstLogIndex = 1
        }
	}
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstLogIndex)
	raft_state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raft_state, snapshot)
}

func readSnapshot(data []byte) (snapshot Snapshot) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

    if d.Decode(&snapshot) != nil {
        DPrintf("raft.go-readSnapshot()-Decode error!")
    }
    return
}

func (rf *Raft) StateOversize(threshold int) (result bool) {
	if rf.persister.RaftStateSize() >= threshold {
		return true
	} else {
		return false
	}
}

// called by the associated KVServer, Raft must store each snapshot in the persister object using SaveStateAndSnapshot().
// arguments:
//    snapshot contains snapshot_info/kv.kvmappings/kv.servedRequest. The Raft is only interested for the first two items.
//    last_included_index is the last index included in the snapshot.
func (rf *Raft) SaveSnapshotAndTrimLog(snapshot []byte, last_included_index int) {
	SPrintf("peer-%d SaveSnapshotAndTrimLog(snapshot, index = %d)", rf.me, last_included_index)
	rf.mu.Lock()
	defer SPrintf("peer-%d SaveSnapshotAndTrimLog return!", rf.me)
	defer rf.mu.Unlock()

	// do a check, to confirm whether the snapshot is needed now?
	if last_included_index < rf.firstLogIndex {
		SPrintf("peer-%d SaveSnapshotAndTrimLog got a out-of-date index-%d, just ignore it.", rf.me, last_included_index)
		// this snapshot in argument is out of date, we have moved to a up-to-date snapshot. So ignore it.
		return
	}

	// now last_included_index >= rf.firstLogIndex
	rf.truncateLog(last_included_index + 1, rf.getLogLastIndex() + 1) // index is included in the snapshot.
	rf.firstLogIndex = last_included_index + 1                    // rf.commitIndex and rf.lastApplied must be bigger than rf.firstLogIndex at this time.

	// then save the raft state and snapshot in an atomic step.
	rf.persistWithSnapshot(snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// refer to TA's guide.
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset int    // you do not have to implement the offset mechanism.
	Data []byte // snapshot.
	//Done bool     // used by the offset mechanism.
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	SPrintf("peer-%d InstallSnapshot()! args.LastIncludedIndex = %d.", rf.me, args.LastIncludedIndex)
	rf.mu.Lock()
	defer SPrintf("peer-%d InstallSnapshot() return!", rf.me)
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// snapshot is out-of-date, ignore it.
	if args.LastIncludedIndex < rf.firstLogIndex {
		return
	}

	// NOTE: don't forget update rf.lastApplied!
	// snapshot contains new information not already in the recipient's log.(including conflict case)
	discard_all_logentries := false
	if rf.getLogLastIndex() <= args.LastIncludedIndex {
		discard_all_logentries = true
	} else {
		// check whether conflict.
		if rf.indexIsInLog(args.LastIncludedIndex) &&
            rf.getLogEntry(args.LastIncludedIndex).Term != args.LastIncludedTerm {
            // conflict with the existing log!
			discard_all_logentries = true
		}
	}

	if discard_all_logentries {
		SPrintf("peer-%d InstallSnapshot() branch1:snapshot contains new information not already in the recipient's log.", rf.me)
		// now discard the entire log.
		rf.log = make([]LogEntry, 0)
		// accept the snapshot.
		rf.firstLogIndex = args.LastIncludedIndex + 1
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		snapshot := args.Data
		// persist!
		rf.persistWithSnapshot(snapshot)
		var msg ApplyMsg
		msg.CommandValid = false // indicates that this ApplyMsg is a snapshot.
		msg.Snapshot = snapshot
		rf.applyCh <- msg // deadlock?
	} else {
		// instead the follower receives a snapshot that describes a prefix of its log.
		// discard log entries covered by the snapshot.
		SPrintf("peer-%d InstallSnapshot() branch2:snapshot descibes a prefix of recipient's log.", rf.me)
		rf.truncateLog(args.LastIncludedIndex+1, rf.getLogLastIndex()+1)
		snapshot := args.Data
		rf.firstLogIndex = args.LastIncludedIndex + 1 // no problem!
		// persist!
		rf.persistWithSnapshot(snapshot)
        // Take care!
		if args.LastIncludedIndex > rf.lastApplied {
			rf.lastApplied = args.LastIncludedIndex
            // invariant: rf.commitIndex >= rf.lastApplied
		    if args.LastIncludedIndex > rf.commitIndex {
			    rf.commitIndex = args.LastIncludedIndex
            }
            // need to notify the KVServer to update its state use the snapshot.
		    var msg ApplyMsg
		    msg.CommandValid = false
		    msg.Snapshot = snapshot
		    rf.applyCh <- msg // deadlock?
		}
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("peer-%d gets a RequestVote RPC.", rf.me)
	// Your code here (2A, 2B).
	// First, we need to detect obsolete information
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	stepdown := false // Does this raft peer need to step down to a follower? It could be Follower/Candidate/Leader at first.
	// step down and convert to follower, adopt the args.Term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		old_state := rf.state
		rf.state = Follower
		if old_state == Leader {
			rf.nonleaderCh <- true
		}
		rf.votedFor = -1
		rf.persist()
		stepdown = true
	}

	// 5.4.1 Election restriction : if the requester's log isn't more up-to-date than this peer's, don't vote for it.
	// check whether the requester's log is more up-to-date.(5.4.1 last paragraph)
	//if len(rf.log) > 0 { // At first, there's no log entry in rf.log
	if rf.getLogLen() > 0 { // At first, there's no log entry in rf.log
		//if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		/*last_term := 0
		  if len(rf.log) == 0 {
		      last_term = rf.lastIncludedTerm
		  } else {
		      last_term = rf.getLogEntry(rf.getLogLastIndex()).Term
		  }*/
		last_term := rf.getLogEntry(rf.getLogLastIndex()).Term
		if last_term > args.LastLogTerm {
			// this peer's log is more up-to-date than requester's.
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
			//} else if rf.log[len(rf.log)-1].Term == args.LastLogTerm {
		} else if last_term == args.LastLogTerm {
			if rf.getLogLastIndex() > args.LastLogIndex {
				// this peer's log is more up-to-date than requester's.
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}
		}
	}

	// requester's log is more up-to-date than requester's.
	// Then, we should check whether this server has voted for another server in the same term
	if stepdown {
		rf.resetElectionTimeout()
		// now we need to reset the election timer.
		rf.votedFor = args.CandidateId // First-come-first-served
		rf.persist()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	/* Section 5.5 :
	 * The server may crash after it completing an RPC but before responsing, then it will receive the same RPC again after it restarts.
	 * Raft RPCs are idempotent, so this causes no harm.
	 */
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false // First-come-first-served, this server has voted for another server before.
	}
	reply.Term = rf.currentTerm
	return
}

// AppendEntries RPC handler
// reset the election timeout! send a value to related channel
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) > 0 {
		DPrintf("peer-%d gets an AppendEntries RPC(args.LeaderId = %d, args.PrevLogIndex = %d, args.LeaderCommit = %d, args.Term = %d, rf.currentTerm = %d).", rf.me, args.LeaderId, args.PrevLogIndex, args.LeaderCommit, args.Term, rf.currentTerm)
	} else {
		//DPrintf("peer-%d gets an heartbeat(args.LeaderId = %d, args.Term = %d, args.PrevLogIndex = %d, args.LeaderCommit = %d).", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.LeaderCommit)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// initialize the reply.
	reply.ConflictIndex = 1
	reply.ConflictTerm = 0

	// detect obsolete information, this can filter out old leader's heartbeat.
	if args.Term < rf.currentTerm {
		DPrintf("peer-%d got an obsolete AppendEntries RPC..., ignore it.(args.Term = %d, rf.currentTerm = %d.)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	/* Can the old Leader receive an AppendEntries RPC from the new leader?
	 * I think the answer is yes.
	 * The old leader's heartbeat packets lost all the time,
	 * and others will be elected as the new leader(may do not need this peer's vote, consider a 3 peers cluster),
	 * then the new leader will heartbeat the old leader. So the old leader will learn this situation and convert to a Follower.
	 */

	// reset the election timeout as soon as possible to prevent an unneeded election!
	rf.resetElectionTimeout()
	rf.currentTerm = args.Term
	rf.persist()
	reply.Term = args.Term

	if rf.state == Candidate {
		DPrintf("peer-%d calm down from a Candidate to a Follower!!!", rf.me)
		rf.state = Follower
	} else if rf.state == Leader {
		DPrintf("peer-%d degenerate from a Leader to a Follower!!!", rf.me)
		rf.state = Follower
		rf.nonleaderCh <- true
	}

	// consistent check
	// Reply false(refuse the new entries) if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm($5.3)
	if rf.getLogLastIndex() < args.PrevLogIndex {
		// Then the leader will learn this situation and adjust this follower's matchIndex/nextIndex in its state, and AppendEntries RPC again.
		reply.Success = false
		return
	}

	// now rf.log's last index >= args.PrevLogIndex
	if args.PrevLogIndex < rf.firstLogIndex {
        // match! And truncate the args.Entries.
        if rf.firstLogIndex > args.PrevLogIndex + len(args.Entries) {
            // there's no need to do anything, But I don't think this will happen.
            reply.Success = false  // if true, then will cause deplicated count.
            return
        }
		args.Entries = args.Entries[rf.firstLogIndex - args.PrevLogIndex - 1 : ]
		args.PrevLogIndex = rf.firstLogIndex - 1
	} else {
        // now rf.firstLogIndex <= args.PrevLogIndex <= rf.getLogLastIndex(). So rf.getLogLastIndex() >= rf.firstLogIndex, so rf.log is not empty.
		SPrintf("peer-%d args.PrevLogIndex = %d, rf.firstLogIndex = %d.", rf.me, args.PrevLogIndex, rf.firstLogIndex)
		if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
			// If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it.
			// delete the log entries from PrevLogIndex to end(including PrevLogIndex).
			DPrintf("peer-%d fail to pass the consistency check, truncate the log", rf.me)
			// truncate log to args.PrevLogIndex-1
			rf.truncateLog(rf.firstLogIndex, args.PrevLogIndex) // log[i:j] contains i~j-1, and we don't want to reserve log entry at PrevLogIndex. So...
			rf.persist()
			reply.Success = false
            // search for the ConflictIndex and ConflictTerm.
            if args.PrevLogIndex - 1 >= rf.firstLogIndex {
			    reply.ConflictTerm = rf.getLogEntry(args.PrevLogIndex - 1).Term
			    var i int
			    for i = args.PrevLogIndex - 1; i >= rf.firstLogIndex; i-- { // log before rf.firstLogIndex is committed, will not conflict.
				    if rf.getLogEntry(i).Term == reply.ConflictTerm {
					    continue
				    } else {
					    break
				    }
			    }
			    reply.ConflictIndex = i + 1
            } else {
                // conflict at the rf.log's first element.
                reply.ConflictIndex = args.PrevLogIndex
                reply.ConflictTerm = args.PrevLogTerm
            }
			return
		}
	}

	// Now this peer's log matches the leader's log at PrevLogIndex. Append any new entries not already in the log
	DPrintf("peer-%d AppendEntries RPC pass the consistent check at PrevLogIndex = %d!", rf.me, args.PrevLogIndex)
	// now logs match at PrevLogIndex
	// NOTE: only if the logs don't match at PrevLogIndex, truncate the rf.log.
	log_index := args.PrevLogIndex + 1 // pos is the index of the slice just after the element at PrevLogIndex.
	entries_index := 0
	mismatch := false
	for log_index <= rf.getLogLen() && entries_index < len(args.Entries) {
		//if rf.log[pos].Term == args.Entries[i].Term {
		if rf.getLogEntry(log_index).Term == args.Entries[entries_index].Term {
			entries_index++
			log_index++
		} else {
			// conflict!
			mismatch = true
			break
		}
	}

	if mismatch {
		// need adjustment. rf.log[pos].Term != args.Entries[i].Term
		// truncate the rf.log and append entries.
		//rf.log = rf.log[:pos]
		rf.truncateLog(rf.firstLogIndex, log_index)
		rf.log = append(rf.log, args.Entries[entries_index:]...)
		rf.persist()
	} else {
		// there are some elements in entries but not in rf.log
		if entries_index < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[entries_index:]...)
			rf.persist()
		}
	}
	// now the log is consistent with the leader's. from 0 ~ PrevLogIndex + len(Entries). but whether the post-sequences are consistent is unknown.
	reply.Success = true

	// update the rf.commitIndex. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	last_index := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		old_commit_index := rf.commitIndex
		if args.LeaderCommit <= last_index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = last_index
		}
		DPrintf("peer-%d Nonleader update its commitIndex from %d to %d. And it's len(rf.log) = %d.", rf.me, old_commit_index, rf.commitIndex, len(rf.log))

		// apply. Now all the commands before rf.commitIndex will not be changed, and could be applied.
		go func() {
			rf.canApplyCh <- true
		}()
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	RV_RPCS++
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	AE_RPCS++
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// only the leader could send AppendEntries RPC. And it should handle the situations reflected in reply
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	// only the leader could send InstallSnapshot RPC.
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// NOTE: command is an object of type Op struct in kvraft/server.go
	DPrintf("peer-%d ----------------------Start()-----------------------", rf.me)
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
	}
	if isLeader {
		// Append the command into its own rf.log
		var newlog LogEntry
		newlog.Term = rf.currentTerm
		newlog.Command = command
		rf.log = append(rf.log, newlog)
		// now the log entry is appended into leader's log.
		rf.persist()
		index = rf.getLogLastIndex() // the log entry are expected to appear at index.
		rf.repCount[index] = 1
		rf.mu.Unlock()

		// start agreement and return immediately.
		for peer_index, _ := range rf.peers {
			if peer_index == rf.me {
				continue
			}
			// send AppendEntries RPC to each peer. And decide when it is safe to apply a log entry to the state machine.
			go func(i int) {
				rf.mu.Lock()
				nextIndex_copy := make([]int, len(rf.peers))
				copy(nextIndex_copy, rf.nextIndex)
				rf.mu.Unlock()
				for {
					// make a copy of current leader's state.
					rf.mu.Lock()
					// we should not send RPC if rf.currentTerm != term, the log entry will be sent in later AE-RPCs in args.Entries.
					if rf.state != Leader || rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					// make a copy of leader's raft state.
					commitIndex_copy := rf.commitIndex        // during the agreement, commitIndex may increase.
					log_copy := make([]LogEntry, len(rf.log)) // during the agreement, log could grow.
					copy(log_copy, rf.log)
					firstLogIndex_copy := rf.firstLogIndex
					//snapshot_copy := make([]byte, len(rf.persister.ReadSnapshot()))
					//copy(snapshot_copy, rf.persister.ReadSnapshot())
                    snapshot_copy := rf.persister.ReadSnapshot()
					rf.mu.Unlock()
			        snapshot := readSnapshot(snapshot_copy) // snapshot is type Snapshot, snapshot_copy is []byte.

					// the log entry that should be sent to the follower is snapshotted.
					if nextIndex_copy[i] < firstLogIndex_copy {
						SPrintf("leader-%d need to send an InstallSnapshot RPC to follower-%d.", rf.me, i)
						// send an InstallSnapshot RPC to the peer.
						var args InstallSnapshotArgs
						var reply InstallSnapshotReply
						args.Term = term
						args.LeaderId = rf.me
						// extract from snapshot.
						args.LastIncludedIndex = snapshot.LastIncludedIndex
						args.LastIncludedTerm = snapshot.LastIncludedTerm
						args.Data = snapshot_copy
						ok := rf.sendInstallSnapshot(i, &args, &reply)
						if ok {
							rf.mu.Lock()
							// re-establish the assumption.
							if rf.state != Leader || rf.currentTerm != term {
								rf.mu.Unlock()
								return
							}
							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = reply.Term
								rf.persist()
								rf.resetElectionTimeout()
								DPrintf("peer-%d degenerate from Leader into Follower!!!", rf.me)
								rf.mu.Unlock()
								rf.nonleaderCh <- true
								// don't try to send AppendEntries RPC to others then, rf is not the leader.
								return
							}
							// now the follower accept the InstallSnapshot RPC.
							if nextIndex_copy[i] < firstLogIndex_copy {
								nextIndex_copy[i] = firstLogIndex_copy
							}
                            // It's obvious that rf.nextIndex[i] must be larger than firstLogIndex_copy.
                            // because peer-i accepted the snapshot up to firstLogIndex_copy.
                            if rf.nextIndex[i] < firstLogIndex_copy {
                                rf.nextIndex[i] = firstLogIndex_copy
                            }
							rf.mu.Unlock()
						} else {
							time.Sleep(time.Millisecond * time.Duration(100))
						}
					} else { // assumption: nextIndex_copy[i] >= firstLogIndex_copy
						// send an AppendEntries RPC to the peer.
						var args AppendEntriesArgs
						var reply AppendEntriesReply
						args.Term = term
						args.LeaderId = rf.me
						args.LeaderCommit = commitIndex_copy
						// If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
						// NOTE: nextIndex is just a predication. not a precise value.
						args.PrevLogIndex = nextIndex_copy[i] - 1 // invariant: args.PrevLogIndex >= firstLogIndex_copy - 1

                        // prepare the args.Entries.
                        // log_copy(args.prevLogIndex, ....) will be included in args.Entries.
                        if args.PrevLogIndex == firstLogIndex_copy - 1 {
                            args.PrevLogTerm = snapshot.LastIncludedTerm
                            args.Entries = log_copy
                        } else if args.PrevLogIndex >= firstLogIndex_copy + len(log_copy) {//out of range
                            DPrintf("??????????????????????????")
                            // make an adjustment
                            args.PrevLogIndex = firstLogIndex_copy - 1
                            args.PrevLogTerm = snapshot.LastIncludedTerm
                            args.Entries = log_copy
                        } else {
                            // args.PrevLogIndex is in the log.
                            args.PrevLogTerm = log_copy[args.PrevLogIndex - firstLogIndex_copy].Term
                            if args.PrevLogIndex == firstLogIndex_copy + len(log_copy) - 1 {
                                args.Entries = make([]LogEntry, 0)
                            } else {
                                args.Entries = log_copy[args.PrevLogIndex - firstLogIndex_copy + 1 : ]
                            }
                        }

						SPrintf("leader-%d needs to send an AppendEntries RPC to follower-%d, PrevLogIndex = %d.", rf.me, i, args.PrevLogIndex)
						ok := rf.sendAppendEntries(i, &args, &reply)
						// handle RPC reply in the same goroutine.
						if ok == true {
							if reply.Success == true {
								// this case means that the log entry is replicated successfully.
								DPrintf("peer-%d AppendEntries success!", rf.me)
								// re-establish the assumption.
								rf.mu.Lock()
								if rf.state != Leader || rf.currentTerm != term {
									//Figure-8 and p-8~9: never commits log entries from previous terms by counting replicas!
									rf.mu.Unlock()
									return
								}
								// NOTE: TA's QA: nextIndex[i] should not decrease, so check and set.
								if index >= rf.nextIndex[i] {
									rf.nextIndex[i] = index + 1
									// TA's QA
									rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries) // matchIndex is not used in my implementation.
								}
								// test whether we can update the leader's commitIndex.
								rf.repCount[index]++
								// update leader's commitIndex! We can determine that Figure-8's case will not occur now,
								// because we have test rf.currentTerm == term_copy before, so we will never commit log entries from previous terms.
								if rf.commitIndex < index && rf.repCount[index] > len(rf.peers)/2 {
									// apply the command.
									DPrintf("peer-%d Leader moves its commitIndex from %d to %d.", rf.me, rf.commitIndex, index)
									// NOTE: the Leader should commit one by one.
									rf.commitIndex = index
									rf.mu.Unlock()
									// now the command at commitIndex is committed.
									go func() {
										rf.canApplyCh <- true
									}()
								} else {
									rf.mu.Unlock()
								}
								return // jump out of the loop.
							} else {
								// AppendEntries RPC fails because of log inconsistency: Decrement nextIndex and retry
								rf.mu.Lock()
								// re-establish the assumption.
								if rf.state != Leader || rf.currentTerm != term {
									rf.mu.Unlock()
									return
								}
								if reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.currentTerm = reply.Term
									rf.persist()
									rf.resetElectionTimeout()
									DPrintf("peer-%d degenerate from Leader into Follower!!!", rf.me)
									rf.mu.Unlock()
									rf.nonleaderCh <- true
									// don't try to send AppendEntries RPC to others then, rf is not the leader.
									return
								} else {
									// NOTE: the nextIndex[i] should never < 1
									conflict_term := reply.ConflictTerm
									conflict_index := reply.ConflictIndex
									// refer to TA's guide blog.
									// first, try to find the first index of conflict_term in leader's log.
									found := false
									new_next_index := conflict_index // at least 1
									for j := 0; j < len(rf.log); j++ {
										if rf.log[j].Term == conflict_term {
											found = true
										} else if rf.log[j].Term > conflict_term {
											if found {
												//new_next_index = j + 1
												new_next_index = j + rf.firstLogIndex
												break
											} else {
												break
											}
										}
									}
									nextIndex_copy[i] = new_next_index
									rf.mu.Unlock()
									// now retry to send AppendEntries RPC to peer-i.
								}
							}
						} else {
							// RPC fails. Retry!
							// when network partition
							time.Sleep(time.Millisecond * time.Duration(100))
						}
					}
				}
			}(peer_index)
		}
	} else {
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//PrintStatistics()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// create a channel in Raft
	rf.applyCh = applyCh
	rf.state = Follower
	rf.nonleaderCh = make(chan bool, 20)
	rf.leaderCh = make(chan bool, 20)
	rf.canApplyCh = make(chan bool, 20)
	// set election timeout
	rf.voteCount = 0
	rf.resetElectionTimeout()

	// Initialize volatile state on all servers.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.firstLogIndex = 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// seperate goroutine to apply command to statemachine.
	go func() {
		for {
			<-rf.canApplyCh
			rf.mu.Lock()
            if rf.commitIndex < rf.firstLogIndex - 1 {
                rf.commitIndex = rf.firstLogIndex - 1
            }
            if rf.lastApplied < rf.firstLogIndex - 1 {
                rf.lastApplied = rf.firstLogIndex - 1
            }
			commitIndex_copy := rf.commitIndex
			lastApplied_copy := rf.lastApplied
            firstLogIndex_copy := rf.firstLogIndex
			term_copy := rf.currentTerm
			log_copy := make([]LogEntry, len(rf.log))
			copy(log_copy, rf.log)
			rf.mu.Unlock()
            curr_index := -1
			for curr_index = lastApplied_copy + 1; curr_index <= commitIndex_copy; curr_index++ {
				// the log may be snapshotted at any time.
				DPrintf("peer-%d apply command-xx at index-%d.", rf.me, curr_index)
				var curr_command ApplyMsg
				curr_command.CommandValid = true
                if curr_index - firstLogIndex_copy >= len(log_copy) || curr_index - firstLogIndex_copy < 0 {
                    DPrintf("!!!!!!!!!!!!!!!index out of range, curr_index = %d, firstLogIndex_copy = %d, len(log_copy) = %d.", curr_index, firstLogIndex_copy, len(log_copy))
                    break
                }
				curr_command.Command = log_copy[curr_index-firstLogIndex_copy].Command
				curr_command.CommandIndex = curr_index
				curr_command.CommandTerm = term_copy
				rf.applyCh <- curr_command
			//	rf.lastApplied = curr_index
			}
            rf.mu.Lock()
            if curr_index - 1 > rf.lastApplied {
                rf.lastApplied = curr_index - 1
            }
			rf.mu.Unlock()  // if Unlock() here will cause 3A deadlock.
		}
	}()

	// Leader's heartbeat long-running goroutine.
	go func() {
		for {
			if rf.state == Leader {
				// send heartbeats
				rf.broadcastHeartbeats()
				time.Sleep(time.Millisecond * time.Duration(100)) // 100ms per heartbeat. (heartbeat time interval << election timeout)
			} else {
				// block until be elected as the new leader.
				DPrintf("peer-%d leader's heartbeat long-running goroutine. block.", rf.me)
				<-rf.leaderCh
				DPrintf("peer-%d leader's heartbeat long-running goroutine. get up.", rf.me)
			}
		}
	}()

	// Nonleader's election timeout long-running goroutine.
	go func() {
		for {
			// check rf.state == Follower
			if rf.state != Leader {
				// begin tic-toc
				time.Sleep(time.Millisecond * time.Duration(10))
				if rf.electionTimeout() {
					DPrintf("peer-%d kicks off an election!\n", rf.me)
					// election timeout! kick off an election.
					// convertion to a Candidate.
					rf.mu.Lock()
					DPrintf("peer-%d becomes a Candidate!!!\n", rf.me)
					rf.state = Candidate
					rf.currentTerm += 1
					rf.persist()
					term_copy := rf.currentTerm // create a copy of the term and it'll be used in RequestVote RPC.
					// vote for itself.
					rf.voteCount = 1
					rf.resetElectionTimeout()
					// send RequestVote RPCs to all other peers in seperate goroutines.
					last_log_index_copy := rf.getLogLastIndex()
					last_log_term_copy := -1
                    if len(rf.log) > 0 {
                        last_log_term_copy = rf.getLogEntry(last_log_index_copy).Term
                    } else {
                        snapshot := readSnapshot(rf.persister.ReadSnapshot())
                        last_log_term_copy = snapshot.LastIncludedTerm
                    }
					rf.mu.Unlock()
					for peer_index, _ := range rf.peers {
						if peer_index == rf.me {
							continue
						}
						// create goroutine.
						go func(i int) {
							// use a copy of the state of the rf peer
							var args RequestVoteArgs
							args.Term = term_copy
							args.CandidateId = rf.me
							args.LastLogIndex = last_log_index_copy
							args.LastLogTerm = last_log_term_copy
							var reply RequestVoteReply
							DPrintf("peer-%d send a sendRequestVote RPC to peer-%d", rf.me, i)
							// reduce RPCs....
							rf.mu.Lock()
							if rf.state != Candidate || rf.currentTerm != term_copy {
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
							ok := rf.sendRequestVote(i, &args, &reply)
							// handle the RPC reply in the same goroutine.
							if ok == true {
								if reply.VoteGranted == true {
									// whether the peer is still a Candidate and the previous term? if yes, increase rf.voteCount; if no, ignore.
									rf.mu.Lock()
									// re-establish the assumption.
									if rf.state == Candidate && term_copy == rf.currentTerm {
										rf.voteCount += 1
										DPrintf("peer-%d gets a vote!", rf.me)
										if rf.voteCount > len(rf.peers)/2 {
											rf.convertToLeader()
										}
									}
									rf.mu.Unlock()
								} else {
									rf.mu.Lock()
									// re-establish the assumption.
									if rf.state == Candidate && term_copy == rf.currentTerm {
										if reply.Term > rf.currentTerm {
											rf.state = Follower
											rf.currentTerm = reply.Term
											rf.persist()
											rf.voteCount = 0
											DPrintf("peer-%d calm down from a Candidate to a Follower!!!", rf.me)
											rf.resetElectionTimeout()
										}
									}
									rf.mu.Unlock()
								}
							}
						}(peer_index)
					}
				}
			} else {
				// block until become a Follower or Candidate.
				DPrintf("peer-%d non-leader's election timeout long-running goroutine. block.", rf.me)
				<-rf.nonleaderCh
				DPrintf("peer-%d non-leader's election timeout long-running goroutine. get up.", rf.me)
				rf.resetElectionTimeout()
			}
		}
	}()

	return rf
}

// you should call this method with rf.mu.lock held.
func (rf *Raft) convertToLeader() {
	rf.state = Leader
	DPrintf("peer-%d becomes the new leader!!!", rf.me)
	// when a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log. (Section 5.3)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	next_index_initval := rf.getLogLastIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = next_index_initval
		rf.matchIndex[i] = 0 // increases monotonically.
	}
	rf.repCount = make(map[int]int)
	DPrintf("peer-%d Leader's log array's length = %d.", rf.me, len(rf.log))
	rf.leaderCh <- true
}

// set the electionTimeoutStartTime to now.
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutStartTime = time.Now()
	// randomize election timeout, 300~400ms
	rf.electionTimeoutInterval = time.Duration(time.Millisecond * time.Duration(500+rand.Intn(300)))
}

func (rf *Raft) electionTimeout() bool {
	if time.Now().Sub(rf.electionTimeoutStartTime) >= rf.electionTimeoutInterval {
		DPrintf("peer-%d election timeout!", rf.me)
		return true
	} else {
		return false
	}
}

func (rf *Raft) broadcastHeartbeats() {
	DPrintf("peer-%d broadcast heartbeats.", rf.me)

	for peer_index, _ := range rf.peers {
		if peer_index == rf.me {
			continue
		}
		// create a goroutine to send heartbeat for each peer.
		go func(i int) {
			// reduce RPCs....
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			// the commitIndex could increase and the log could grow
			commitIndex_copy := rf.commitIndex
			log_copy := make([]LogEntry, len(rf.log))
			copy(log_copy, rf.log)
			firstLogIndex_copy := rf.firstLogIndex
			term_copy := rf.currentTerm
			rf.mu.Unlock()

			var args AppendEntriesArgs
			args.Term = term_copy
			args.LeaderId = rf.me
			// NOTE: This is a key point.
			args.PrevLogIndex = firstLogIndex_copy + len(log_copy) - 1
			if args.PrevLogIndex > 0 && len(log_copy) > 0 {
				args.PrevLogTerm = log_copy[args.PrevLogIndex-firstLogIndex_copy].Term
			}
			args.Entries = make([]LogEntry, 0) // heartbeat has an empty Entries.
			args.LeaderCommit = commitIndex_copy
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, &args, &reply)
			// handle the PRC reply in the same goroutine.
			if ok == true {
				if reply.Success {
					return
				} else {
					rf.mu.Lock()
					// re-establish the assumption.
					if rf.state != Leader || rf.currentTerm != term_copy {
						rf.mu.Unlock()
						return
					}
					// without re-establish above, the next branch could dead-lock. A lot of responses come and fill up the nonleaderCh with true.
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.persist()
						rf.state = Follower
						DPrintf("peer-%d degenerate from a Leader into a Follower!!!", rf.me)
						rf.mu.Unlock()
						rf.nonleaderCh <- true
					} else {
						// fail to pass the consistency check.
						rf.mu.Unlock()
					}
					return
				}
			} else {
				// heartbeat RPC failed!
				return
			}
		}(peer_index)
	}
}

// don't repeat yourself.
// call these functions with rf.mu held!
func (rf *Raft) getLogEntry(index int) (entry LogEntry) {
	if !rf.indexIsInLog(index) {
		DPrintf("getLogEntry() wrong index! index = %d, rf.firstLogIndex = %d, len(rf.log) = %d.", index, rf.firstLogIndex, len(rf.log))
		return rf.log[index-rf.firstLogIndex]  // this will crash.
	}
	return rf.log[index-rf.firstLogIndex]
}

func (rf *Raft) getLogLen() (length int) {
	return rf.firstLogIndex + len(rf.log) - 1
}

func (rf *Raft) getLogLastIndex() (index int) {
	if len(rf.log) == 0 {
		return rf.firstLogIndex - 1
	}
	return rf.firstLogIndex + len(rf.log) - 1
}

func (rf *Raft) indexIsInLog(index int) (result bool) {
    if index < rf.firstLogIndex || index >= rf.firstLogIndex + len(rf.log) {
        return false
    }
    return true
}

// log will be adjust to [first_index, last_index)
func (rf *Raft) truncateLog(first_index int, last_index int) {
    SPrintf("truncateLog() : before invocation, len(rf.log) = %d, first_index = %d, last_index = %d.", len(rf.log), first_index, last_index)

	if first_index < rf.firstLogIndex {
		first_index = rf.firstLogIndex
	}
	if last_index > rf.getLogLastIndex()+1 {
		last_index = rf.getLogLastIndex() + 1
	}
	if last_index == first_index {
		rf.log = make([]LogEntry, 0)
	} else {
        log_copy := make([]LogEntry, last_index-first_index)
        copy(log_copy, rf.log[first_index-rf.firstLogIndex : last_index-rf.firstLogIndex])
        rf.log = log_copy
	}
    SPrintf("truncateLog() : after invocation, len(rf.log) = %d.", len(rf.log))
}

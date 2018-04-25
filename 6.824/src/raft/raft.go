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
// import "bytes"
// import "labgob"



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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


// A Log Entry
type LogEntry struct {
    Term  int
    Index int   // need?
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
    currentTerm   int    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    votedFor      int    // candidateId that received vote in current term (or null if none)
    log           []LogEntry  // log entries, each entry contains command for state machine, and term when entry was received by leader (first index is 1)

    // Volatile state on all servers:
    commitIndex   int    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    lastApplied   int    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

    // Volatile state on leaders:
    nextIndex     []int  // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    matchIndex    []int  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    state  RaftState  //Follower/Candidate/Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    if rf.state == Leader {
        isLeader = true
    } else {
        isLeader = false
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int            // candidate's term
    CandidateId int     // candidate requesting vote
    LastLogIndex int    // index of candidate's last log entry
    LastLogTerm int     // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int            // currentTerm, for candidate to update itself
    VoteGranted bool    // true means candidate received vote
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term        int
    Success     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    // First, we need to detect obsolete information
    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term = currentTerm
        returni false
    }

    // Then, we should check whether this server has voted for another server in the same term
    if args.Term > rf.currentTerm {
        rf.votedFor = args.CandidateId  // First-come-first-served
        rf.currentTerm = args.Term
        reply.VoteGranted = true
        reply.Term = currentTerm
        return true
    }

    /* Section 5.5 : 
     * The server may crash after it completing an RPC but before responsing, then it will receive the same RPC again after it restarts.
     * Raft RPCs are idempotent, so this causes no harm.
     */
    if args.Term == rf.currentTerm {
        if rf.votedFor == args.CandidateId {
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false    // First-come-first-served, this server has voted for another server before.
        }
        reply.Term = rf.currentTerm
        return reply.VoteGranted
    }
}


// AppendEntries RPC handler
// FIXME:reset the election timeout! send a value to related channel
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // 1. detect obsolete information 
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return false
    }
    // consistent check
    // 2. Reply false(refuse the new entries) if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm($5.3)
    if len(rf.log)-1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.Term {
        // Then the leader will learn this situation and adjust this follower's matchIndex/nextIndex in its state, and AppendEntries RPC again.
        // 3. delete the log entries from PrevLogIndex to end
        rf.log = rf.log[:args.PrevLogIndex] // log[i:j] contains i~j-1, and we don't want to reserve log entry at PrevLogIndex. So...
        reply.Term = rf.currentTerm
        reply.Success = false
        return false
    }

    // 4. Now this peer's log matches the leader's log at PrevLogIndex. Append any new entries not already in the log
    rf.log = rf.log[:args.PrevLogIndex+1]
    rf.log = append(rf.log, args.Entries[PrevLogIndex+1:]...)

    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if rf.commitIndex < args.LeaderCommit {
        // we need to update commitIndex locally. Explictly update the old entries. See my note upon Figure8.
        // This step will exclude some candidates to be elected as the new leader!
        if leaderCommit > len(rf.log)-1 {
            rf.commitIndex = len(rf.log)-1
        } else {
            rf.commitIndex = leaderCommit
        }
    }
    reply.Term = rf.currentTerm
    reply.Success = true
    return true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    // only the leader coule send AppendEntries RPC. And it should handle the situations reflected in reply 
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

    // lsm: use GetState() to check whether this server is the leader

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
    // Code for 2A : fill the raft structure
    //rf.currentTerm = 
    //rf.votedFor = 
    // Initialize volatile state on all servers.
    rf.commitIndex = 0
    rf.lastApplied = 0
    // Create a goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while
    go func() {
        // goroutine which is responsible for leader election
        election_timeout := time.NewTimer(time.Second * 10)  // the time is a dummy.
        for {
            // then reset the timer to the expected value, now 300 is the base, and 100 is the interval.
            select {
            case <-reset_election_ch:    // reset the timer.
                election_timeout_interval :=  300 + rand.Intn(100)  // randomized timeouts to ensure that split votes are rare.
                resetTimer(&election_timeout, election_time_interval)
            case <-election_timeout.C:
                // election timeout!
                // now this peer should converse to a candidate, and kick off a leader election!
                rf.currentTerm += 1
                vote_counter := 1        // This peer has voted for itself
                // reset election timer
                election_timeout_interval :=  300 + rand.Intn(100)  // randomized timeouts to ensure that split votes are rare.
                resetTimer(&election_timeout, election_time_interval)
                // send RequestVote RPCs to each peer. 
                for i := 0; i < len(rf.peers); i++ {
                    if i == me {
                        continue
                    }
                    var args RequestVoteArgs
                    // fill the args
                    args.Term = rf.currentTerm
                    args.CandidateId = me   // right?
                    args.LastLogIndex = len(rf.log)
                    args.LastLogTerm = rf.log[len(rf.log-1)].Term
                    var reply RequestVoteReply
                    ok := rf.sendRequestVote(i, &args, &reply)
                    if ok == false {
                        // back to follower, and update the term
                        rf.currentTerm := reply.Term
                    } else {
                        // update the voted counter, if the counter's value > len(peers)/2, this peer becomes the new leader.
                        vote += 1
                        if vote > len(rf.peers) / 2 {
                            // become the new leader of currentTerm! Send heartbeats to establish the authority.
                            rf.becomeLeader()
                        }
                    }
                }
            }
        }
    }()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

// send heartbeat to all the peers to establish its authority.
func (rf *Raft) becomeLeader() {
    for i := 0; i < len(rf.peers); i++ {
        if i == me {
            continue
        }
        // create a goroutine to send heartbeat for each peer.
        go func(){
            var heartbeat_args AppendEntriesArgs
            var heartbeat_reply AppendEntriesReply
            ok := rf.sendAppendEntries(i, &heartbeat_args, &heartbeat_reply)
        }()
    }
}

//reset the given Timer to value
func resetTimer(timer *Timer, value int) {
    // first stop it.
    if !timer.Stop() {
        select {
        case <-timer.C:
        default:
    }
    // then reset it.
    timer.Reset(time.Millisecond * value)
}

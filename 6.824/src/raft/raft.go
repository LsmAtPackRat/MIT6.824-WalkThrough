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
    resetElectionCh chan int   // used to notify the peer to reset its election timeout.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
        reply.Term = rf.currentTerm
        return
    }

    // Then, we should check whether this server has voted for another server in the same term
    if args.Term > rf.currentTerm {
        rf.votedFor = args.CandidateId  // First-come-first-served
        rf.currentTerm = args.Term
        reply.VoteGranted = true
        reply.Term = rf.currentTerm
        return
    }

    /* Section 5.5 : 
     * The server may crash after it completing an RPC but before responsing, then it will receive the same RPC again after it restarts.
     * Raft RPCs are idempotent, so this causes no harm.
     */
    if args.Term == rf.currentTerm {
        if rf.votedFor == args.CandidateId {
            reply.VoteGranted = true
            rf.resetElectionCh <- 1   // ?? if this peer is the leader??
        } else {
            reply.VoteGranted = false    // First-come-first-served, this server has voted for another server before.
        }
        reply.Term = rf.currentTerm
        return
    }
}


// AppendEntries RPC handler
// reset the election timeout! send a value to related channel
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // 1. detect obsolete information 
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    // reset the election timeout
    rf.resetElectionCh <- 1
    // consistent check
    // 2. Reply false(refuse the new entries) if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm($5.3)
    if len(rf.log)-1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.Term {
        // Then the leader will learn this situation and adjust this follower's matchIndex/nextIndex in its state, and AppendEntries RPC again.
        // 3. delete the log entries from PrevLogIndex to end
        rf.log = rf.log[:args.PrevLogIndex] // log[i:j] contains i~j-1, and we don't want to reserve log entry at PrevLogIndex. So...
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    // 4. Now this peer's log matches the leader's log at PrevLogIndex. Append any new entries not already in the log
    rf.log = rf.log[:args.PrevLogIndex+1]
    rf.log = append(rf.log, args.Entries[args.PrevLogIndex+1:]...)

    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if rf.commitIndex < args.LeaderCommit {
        // we need to update commitIndex locally. Explictly update the old entries. See my note upon Figure8.
        // This step will exclude some candidates to be elected as the new leader!
        if args.LeaderCommit > len(rf.log)-1 {
            rf.commitIndex = len(rf.log)-1
        } else {
            rf.commitIndex = args.LeaderCommit
        }
    }
    reply.Term = rf.currentTerm
    reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    // only the leader could send AppendEntries RPC. And it should handle the situations reflected in reply 
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
    rf.state = Follower

    // create a channel in Raft
    rf.resetElectionCh = make(chan int)

	// Your initialization code here (2A, 2B, 2C).
    // Code for 2A : fill the raft structure
    //rf.currentTerm = 
    //rf.votedFor = 
    // Initialize volatile state on all servers.
    rf.commitIndex = 0
    rf.lastApplied = 0
    // Create a goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while
    go func() {
        // create some timers here.
        for {
            switch rf.state {
            case Leader:
                rf.doLeaderLoop()
            default:
                rf.doNonLeaderLoop()
            }
            // then reset the timer to the expected value, now 300 is the base, and 100 is the interval.
        }
    }()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}


// if you are a leader, periodically send heartbeat AppendEntries RPCs to all the peers.
func (rf *Raft) doLeaderLoop() {
    // heartbeat per 100ms
    time.Sleep(100 * time.Millisecond)
    // send heartbeat AppendEntries RPCs to all the peers.
    rf.broadcastHeartbeats()
}


// if you are a follower or a candidate, update election timeout under associated cases and may convert to a Candidate.
func (rf *Raft) doNonLeaderLoop() {
    //for {
        election_timeout := time.NewTimer(time.Second * 10)
        select {
        // if receiving AppendEntries RPC from current leader or granting vote to candidate, will reset timer
        case <-rf.resetElectionCh:    // if someone sent sth in the channel, reset the timer.
            resetElectionTimeout(election_timeout)
        case <-election_timeout.C:
            // election timeout!
            rf.state = Candidate
            // now this peer should converse to a candidate, and kick off a leader election!
            rf.currentTerm += 1
            vote_channel := make(chan bool, len(rf.peers))
            vote_channel <- true    // This peer has voted for itself
            // reset election timer
            resetElectionTimeout(election_timeout)
            // send RequestVote RPCs to each peer. 
            for i := 0; i < len(rf.peers); i++ {
                if i == rf.me {
                    continue
                }
                go func() {
                    // send RequestVote RPC in a goroutine for each peer 
                    var args RequestVoteArgs
                    // fill the args
                    args.Term = rf.currentTerm
                    args.CandidateId = rf.me   // right?
                    args.LastLogIndex = len(rf.log)
                    args.LastLogTerm = rf.log[len(rf.log)-1].Term
                    var reply RequestVoteReply
                    ok := rf.sendRequestVote(i, &args, &reply)
                    if ok == false {
                        // if requested peer's term is larger, converse back to follower, and update the term
                        if reply.Term > rf.currentTerm {
                            rf.currentTerm = reply.Term
                            rf.state = Follower
                        } else  {
                            // maybe the requested peer has voted for another server, as FCFS, so it didn't vote for you.
                        }
                        vote_channel <- false
                    } else {
                        vote_channel <- true   // get a vote!
                    }
                }()
            }
            // start a goroutine to collect the votes.
            go func() {
                vote_pos_counter := 0   // how many peers have voted for you
                vote_all_counter := 0   // how many peers have taken part in this votes
                // block at a vote_channel
                for vote_pos_counter <= len(rf.peers) / 2 && vote_all_counter < len(rf.peers) {
                    vote_content := <-vote_channel
                    if vote_content == true {
                        vote_pos_counter++
                    }
                    vote_all_counter++
                }

                if vote_pos_counter > len(rf.peers) / 2 {
                    rf.becomeLeader()   // FIXME: someone may been voted as the leader??
                }
            }()
        }
    //}
}

// send heartbeats to all the peers to establish its authority.
func (rf *Raft) becomeLeader() {
    // set the state
    rf.state = Leader
}

func (rf *Raft) broadcastHeartbeats() {
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        // create a goroutine to send heartbeat for each peer.
        go func(){
            var heartbeat_args AppendEntriesArgs
            heartbeat_args.Term = rf.currentTerm
            heartbeat_args.LeaderId = rf.me
            heartbeat_args.PrevLogIndex = 1
            heartbeat_args.PrevLogTerm = 1
            heartbeat_args.Entries = make([]LogEntry, 0)   // needed?
            heartbeat_args.LeaderCommit = 1
            var heartbeat_reply AppendEntriesReply
            ok := rf.sendAppendEntries(i, &heartbeat_args, &heartbeat_reply)
            if ok != false {}
        }()
    }
}

// adjust the election timeout interval in this method
func resetElectionTimeout(timer *time.Timer) {
    election_timeout_interval :=  300 + rand.Intn(100)  // randomized timeouts to ensure that split votes are rare
    resetTimer(timer, election_timeout_interval)
}

// reset the given Timer to value
func resetTimer(timer *time.Timer, value int) {
    // first stop it.
    if !timer.Stop() {
        select {
        case <-timer.C:
        default:
        }
    }
    // then reset it.
    timer.Reset(time.Millisecond * time.Duration(value))
}

/*
func stopTimer(timer *Timer) {
    if !timer.Stop() {
        select {
        case <-timer.C:
        default:
        }
    }
}*/

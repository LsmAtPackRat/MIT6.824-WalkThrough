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
//import "strconv"
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
	Term int
	//Index   int // need?
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
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries, each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	repCount   []int // repCount[i] is the count of log[i] in all of the replicas.

	state                    RaftState // Follower/Candidate/Leader
	voteCount                int
	resetElectionCh          chan int      // used to notify the peer to reset its election timeout.
	electionTimeoutStartTime time.Time     // nanosecond precision. used for election timeout.
	electionTimeoutInterval  time.Duration // nanosecond count.
	nonleaderCh              chan bool     // block/unblock nonleader's election timeout long-running goroutine
	leaderCh                 chan bool     // block/unblock leader's heartbeat long-running goroutine
	applyCh                  chan ApplyMsg
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

	setdown := false
	// step down and convert to follower, adopt the args.Term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		setdown = true
	}

	// 5.4.1 Election restriction : if the requester's log isn't more up-to-date than this peer's, don't vote for it.
	// check whether the requester's log is more up-to-date.(5.4.1 last paragraph)
	if len(rf.log) > 0 { // At first, there's no log entry in rf.log
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
			// this peer's log is more up-to-date than requester's.
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		} else if rf.log[len(rf.log)-1].Term == args.LastLogTerm {
			if len(rf.log) > args.LastLogIndex {
				// this peer's log is more up-to-date than requester's.
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}
		}
	}

	// requester's log is more up-to-date than requester's.
	// Then, we should check whether this server has voted for another server in the same term
	if setdown {
		rf.resetElectionTimeout()
		// now we need to reset the election timer.
		rf.votedFor = args.CandidateId // First-come-first-served
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
		//rf.resetElectionTimeout() // I follow the Figure2's <Rules for Servers>'s Followers, is this place right?
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
		DPrintf("peer-%d gets an AppendEntries RPC(args.LeaderId = %d, args.PrevLogIndex = %d, args.LeaderCommit = %d, args.Term = %d, rf.currentTerm = %d).", rf.me, args.LeaderId,  args.PrevLogIndex, args.LeaderCommit, args.Term, rf.currentTerm)
	} else {
		DPrintf("peer-%d gets an heartbeat(args.LeaderId = %d, args.Term = %d, args.PrevLogIndex = %d, args.LeaderCommit = %d).", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.LeaderCommit)
	}
	//DPrintf("peer-%d log[]'s length is %d.", rf.me, len(rf.log))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. detect obsolete information, this can filter out old leader's heartbeat.
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
	// 2. Reply false(refuse the new entries) if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm($5.3)
	if len(rf.log) < args.PrevLogIndex {
		// Then the leader will learn this situation and adjust this follower's matchIndex/nextIndex in its state, and AppendEntries RPC again.
		reply.Success = false
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// 3. If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it.
		// delete the log entries from PrevLogIndex to end(including PrevLogIndex).
		DPrintf("peer-%d fail to pass the consistency check, truncate the log", rf.me)
		rf.log = rf.log[:args.PrevLogIndex-1] // log[i:j] contains i~j-1, and we don't want to reserve log entry at PrevLogIndex. So...
        //FIXME: why cannot delete the next line????
		rf.log = append(rf.log, args.Entries...)
		reply.Success = false
		return
	}

	// 4. Now this peer's log matches the leader's log at PrevLogIndex. Append any new entries not already in the log
	DPrintf("peer-%d AppendEntries RPC pass the consistent check at PrevLogIndex = %d!", rf.me, args.PrevLogIndex)
	// now logs match at PrevLogIndex
	// NOTE: only if the logs don't match at PrevLogIndex, truncate the rf.log.
	pos := args.PrevLogIndex // pos is the index of the slice just after the element at PrevLogIndex.
	i := 0
	mismatch := false
	for pos < len(rf.log) && i < len(args.Entries) {
		if rf.log[pos].Term == args.Entries[i].Term {
			i++
			pos++
		} else {
			// conflict!
			mismatch = true
			break
		}
	}
	if mismatch {
		// need adjustment. rf.log[pos].Term != args.Entries[i].Term
		// truncate the rf.log and append entries.
		rf.log = rf.log[:pos]
		rf.log = append(rf.log, args.Entries[i:]...)
	} else {
		// there some elements in entries but not in rf.log
		if pos == len(rf.log) && i < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}
	// now the log is consistent with the leader's.
	reply.Success = true

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if rf.commitIndex < args.LeaderCommit {
		// we need to update commitIndex locally. Explictly update the old entries. See my note upon Figure8.
		// This step will exclude some candidates to be elected as the new leader!
		// commit!
		old_commit_index := rf.commitIndex
		//old_commit_index := rf.commitIndex
		if args.LeaderCommit <= len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
		for i := old_commit_index + 1; i <= rf.commitIndex; i++ {
			//DPrintf("peer-%d (not the leader) apply the command() at index %d!!!!!!!!", rf.me, i)
            DPrintf("peer-%d apply command-%d at index-%d.", rf.me, rf.log[i-1].Command.(int), i)
			var committed_log ApplyMsg
			committed_log.CommandValid = true
			committed_log.Command = rf.log[i-1].Command
			committed_log.CommandIndex = i
			rf.applyCh <- committed_log
            rf.lastApplied = i
		}
		DPrintf("peer-%d Nonleader update its commitIndex from %d to %d. And it's len(rf.log) = %d.", rf.me, old_commit_index, rf.commitIndex, len(rf.log))
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
	DPrintf("peer-%d ----------------------Start()-----------------------", rf.me)
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	//term, isLeader = rf.GetState()
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
        log_next_index := make([]int, len(rf.peers))
        copy(log_next_index, rf.nextIndex)
        log_copy := make([]LogEntry, len(rf.log))
        copy(log_copy, rf.log)
		index = len(rf.log)
		log_len := len(rf.log)
		rf_copy := rf // use rf_copy to fill the AppendEntries RPC args.
		for len(rf.repCount) < log_len {
			rf.repCount = append(rf.repCount, 1)
		}
		rf.mu.Unlock()
		// start agreement and return immediately.
		for peer_index, _ := range rf.peers {
			if peer_index == rf.me {
				continue
			}
			// send AppendEntries RPC to each peer. And decide when it is safe to apply a log entry to the state machine.
			go func(i int) {
				for {
					// sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply)
                    rf.mu.Lock()
                    if len(rf.log) < rf.nextIndex[i] || rf.state != Leader {
                        // don't need to send AppendEntries RPC to peer-i.
                        // FIXME
                        //defer rf.mu.Unlock()
                        //return
                    }
					var args AppendEntriesArgs
					var reply AppendEntriesReply
					//-------------------------------------------------------------------------------------
					// Note!! when we fill the args, the leader's raft instance will still go ahead.
					// the log[] may grow, and other AppendEntries RPCs will be sent to the same peer concurrently.
					args.Term = rf_copy.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf_copy.commitIndex
					// If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
					args.PrevLogIndex = log_next_index[i] - 1
					if args.PrevLogIndex > 0 {
						args.PrevLogTerm = rf_copy.log[args.PrevLogIndex-1].Term
					}
					args.Entries = log_copy[log_next_index[i]-1 : log_len] // log[nextIndex~end](including nextIndex). Note that the log index start from 1.
					//-------------------------------------------------------------------------------------
                    rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, &args, &reply)
					// handle RPC reply in the same goroutine.
					if ok == true {
						if reply.Success == true {
							DPrintf("peer-%d AppendEntries success!", rf.me)
							// If successful: update nextIndex and matchIndex for follower.
							rf.mu.Lock()
                            // FIXME: nextIndex[i] should not decrease, so check and set.
                            if index >= rf.nextIndex[i] {
                                rf.nextIndex[i] = index + 1
                                // Ass's QA
                                rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
                            }
							// test whether we can update the leader's commitIndex.
							rf.repCount[index-1]++
							// update leader's commitIndex!
							if rf.repCount[index-1] > len(rf.peers)/2 && rf.commitIndex < index {
                                // apply the command.
								DPrintf("peer-%d Leader moves its commitIndex from %d to %d.", rf.me, rf.commitIndex, index)
                                // NOTE: the Leader should commit one by one.
                                old_commit_index := rf.commitIndex
								rf.commitIndex = index
                                for curr_commit_index := old_commit_index + 1; curr_commit_index <= index; curr_commit_index++ {
                                    DPrintf("peer-%d apply command-%d at index-%d.", rf.me, rf.log[curr_commit_index-1].Command.(int), curr_commit_index)
								    var committed_log ApplyMsg
								    committed_log.CommandValid = true
								    committed_log.Command = rf.log[curr_commit_index-1].Command
								    committed_log.CommandIndex = curr_commit_index
								    rf.applyCh <- committed_log
                                    rf.lastApplied = curr_commit_index
                                }
							}
							rf.mu.Unlock()
							break // jump out of the loop.
						} else {
							// AppendEntries RPC fails because of log inconsistency: Decrement nextIndex and retry
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = reply.Term
                                rf.resetElectionTimeout()
								DPrintf("peer-%d Degenerate from Leader into Follower!!!", rf.me)
                                rf.mu.Unlock()
								rf.nonleaderCh <- true
							} else {
                                // FIXME: right?
							    rf.nextIndex[i] -= 1
                                rf.mu.Unlock()
                            }
						}
					} else {
						// RPC fails. Retry!
                        time.Sleep(time.Millisecond * time.Duration(20))
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
	rf.nonleaderCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	// set election timeout
	rf.voteCount = 0
	rf.resetElectionTimeout()

	// Initialize volatile state on all servers.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.repCount = make([]int, 0)
	// Leader's heartbeat long-running goroutine.
	go func() {
		for {
			if rf.state == Leader {
				// send heartbeats
				rf.broadcastHeartbeats()
				time.Sleep(time.Millisecond * time.Duration(200)) // 100ms per heartbeat. (heartbeat time interval << election timeout)
			} else {
				// block until be elected as the new leader.
				//
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
					// vote for itself.
					rf.voteCount = 1
					rf.resetElectionTimeout()
					// send RequestVote RPCs to all other peers in seperate goroutines.
					term_copy := rf.currentTerm // create the copy of the RequestVoteArgs
					last_log_index_copy := len(rf.log)
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
							if args.LastLogIndex > 0 {
								// not an empty log
								args.LastLogTerm = rf.log[last_log_index_copy-1].Term
							}
							var reply RequestVoteReply
							DPrintf("peer-%d send a sendRequestVote RPC to peer-%d", rf.me, i)
							ok := rf.sendRequestVote(i, &args, &reply)
							// handle the RPC reply in the same goroutine.
							if ok == true {
								if reply.VoteGranted == true {
									// whether the peer is still a Candidate and the previous term? if yes, increase rf.voteCount; if no, ignore.
                                    rf.mu.Lock()
									if rf.state == Candidate && reply.Term == rf.currentTerm {
										rf.voteCount += 1
										DPrintf("peer-%d gets a vote!", rf.me)
										if rf.voteCount > len(rf.peers)/2 {
											rf.convertToLeader()
										}
									}
                                    rf.mu.Unlock()
								} else {
									rf.mu.Lock()
									if reply.Term > rf.currentTerm && rf.state == Candidate {
										rf.state = Follower
                                        rf.currentTerm = reply.Term
                                        // FIXME: should resetElectionTimeout()??
                                        //rf.resetElectionTimeout()
										DPrintf("peer-%d calm down from a Candidate to a Follower!!!", rf.me)
									}
									rf.mu.Unlock()
								}
							}
						}(peer_index)
					}
				}
			} else {
				// block until become a Follower or Candidate.
				//
				DPrintf("peer-%d non-leader's election timeout long-running goroutine. block.", rf.me)
				<-rf.nonleaderCh
				DPrintf("peer-%d non-leader's election timeout long-running goroutine. get up.", rf.me)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) convertToLeader() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
//    rf.resetElectionTimeout()
	rf.state = Leader
	DPrintf("peer-%d becomes the new leader!!!", rf.me)
	// when a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log. (Section 5.3)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
    rf.commitIndex = 0
    rf.lastApplied = 0
	next_index_initval := len(rf.log) + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = next_index_initval
		rf.matchIndex[i] = 0 // increases monotonically.
	}
	DPrintf("peer-%d Leader's log array's length = %d.", rf.me, len(rf.log))
	rf.leaderCh <- true
}

// set the electionTimeoutStartTime to now.
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutStartTime = time.Now()
	// randomize election timeout, 300~400ms
	rf.electionTimeoutInterval = time.Duration(time.Millisecond * time.Duration(800+rand.Intn(200)))
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
	rf.mu.Lock()
	// FIXME: it seems that the bug occurs because the AppendEntries RPC's consistency check fail when the logs match.
	rf_copy := rf
    log_next_index := make([]int, len(rf.peers))
    copy(log_next_index, rf.nextIndex)
	rf.mu.Unlock()
	for peer_index, _ := range rf.peers {
		if peer_index == rf.me {
			continue
		}
		// create a goroutine to send heartbeat for each peer.
		go func(i int) {
            rf.mu.Lock()
            if rf.state != Leader {
                return
            }
			var args AppendEntriesArgs
			args.Term = rf_copy.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = log_next_index[i] - 1
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf_copy.log[args.PrevLogIndex-1].Term
			}
			args.Entries = make([]LogEntry, 0)
			args.LeaderCommit = rf_copy.commitIndex
			//args.LeaderCommit = rf.commitIndex // section 5.3 metioned.
			var reply AppendEntriesReply
            rf.mu.Unlock()
			ok := rf.sendAppendEntries(i, &args, &reply)
			// handle the PRC reply in the same goroutine.
			if ok == true {
				//rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.resetElectionTimeout()
					rf.state = Follower
					DPrintf("peer-%d degenerate from a Leader into a Follower!!!", rf.me)
					rf.nonleaderCh <- true
				}
				//rf.mu.Unlock()
			}
		}(peer_index)
	}
}

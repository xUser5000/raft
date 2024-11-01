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

import (
	"bytes"
	"fmt"
	"raft/labgob"
	"raft/labrpc"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const MinElectionTimeout = 500 * time.Millisecond
const MaxElectionTimeout = 1000 * time.Millisecond
const BackgroundJobPeriod = 20 * time.Millisecond
const HeartBeatPeriod = 150 * time.Millisecond

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command any // command to be executed by the state machine
	Term    int // term when entry was received by leader
	Index   int // index of entry in Raft's log slice
}

// Raft A Go object implementing a // send hearsingle Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // used to send committed log entries to the application
	majority  int

	// persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries

	// volatile state on all servers
	serverState     ServerState   // either Follower, Candidate, or Leader
	lastBeat        time.Time     // last time a heartbeat has been received from the leader
	electionTimeout time.Duration // election timeout
	commitIndex     int           // index of highest log entry known to be commited (initialized to 0, increases monotonically)
	lastApplied     int           // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // used to redirect clients to the leader
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's CommitIndex
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"AppendEntriesArgs{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit,
	)
}

type AppendEntriesReply struct {
	Term    int  // current term, for the leader to update itself
	Success bool // true if follower container entry matching prevLogIndex and prevLogTerm

	// for the leader to back up quickly over incorrect follower logs
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with xTerm (if any)
	XLen   int // follower's log length
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	buff := new(bytes.Buffer)
	enc := labgob.NewEncoder(buff)
	_ = enc.Encode(rf.currentTerm)
	_ = enc.Encode(rf.votedFor)
	_ = enc.Encode(rf.log)
	data := buff.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
	var currentTerm int
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		_ = fmt.Errorf("rf.readPersist(): something went wrong while decoding data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) applyCommittedEntries() {
	l := rf.lastApplied + 1
	r := rf.commitIndex
	DPrintf("(server %v, term %v): trying to apply log entries from %v to %v", rf.me, rf.currentTerm, l, r)
	if l <= r {
		for _, entry := range rf.log[l : r+1] {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- msg
			DPrintf("(server %v, term %v): applied log entry with index %v", rf.me, rf.currentTerm, entry.Index)
			rf.lastApplied++
		}
	}
}

func (rf *Raft) matchTerm(term int) {
	if term > rf.currentTerm {
		rf.serverState = Follower
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) isCandidateLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term
	if myLastLogTerm == lastLogTerm {
		return lastLogIndex >= myLastLogIndex
	}
	return lastLogTerm >= myLastLogTerm
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("(server %v, term %v): recevied RequestVote() from (server %v, term %v) that contains the following %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, args)

	rf.applyCommittedEntries()
	rf.matchTerm(args.Term)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("(server %v, term %v): rejected vote from (server %v, term %v) due to outdated term", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.lastBeat = time.Now()
		rf.electionTimeout = randomDuration(MinElectionTimeout, MaxElectionTimeout)
		DPrintf("(server %v, term %v): granted vote to (server %v, term %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		var rejectionReason string
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rejectionReason = "vote being given to another candidate"
		} else {
			rejectionReason = "candidate's log not being up to date"
		}
		DPrintf("(server %v, term %v): rejected vote from (server %v, term %v) due to %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, rejectionReason)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(server %v, term %v): sending RequestVote() to %v that contains the following: %v", rf.me, rf.currentTerm, server, args)
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("(server %v, term %v): recevied AppendEntries() from (server %v, term %v) that contains %v", rf.me, rf.currentTerm, args.LeaderId, args.Term, args)

	rf.matchTerm(args.Term)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		// send info so that the leader can back up quickly over incorrect logs
		if args.PrevLogIndex < len(rf.log) {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex = slices.IndexFunc(rf.log, func(entry LogEntry) bool {
				return entry.Term == reply.XTerm
			})
		} else {
			reply.XTerm = -1
			reply.XIndex = -1
		}
		reply.XLen = len(rf.log)
		return
	}

	rf.serverState = Follower
	rf.lastBeat = time.Now()
	rf.votedFor = -1

	if args.PrevLogIndex+1 < len(rf.log) {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	rf.applyCommittedEntries()

	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("(server %v, term %v): accepted AppendEntries() from (server %v, term %v) that contains %v", rf.me, rf.currentTerm, args.LeaderId, args.Term, args)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("(server %v, term %v): sending AppendEntries() to server %v that contains %v", rf.me, rf.currentTerm, server, args)
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	return ok
}

func (rf *Raft) electionTimerBackgroundJob() {
	for {
		time.Sleep(BackgroundJobPeriod)

		if rf.killed() {
			return
		}

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.serverState == Leader {
				return
			}
			if time.Now().After(rf.lastBeat.Add(rf.electionTimeout)) {
				rf.runElection()
			}
		}()
	}
}

// Lock must be held before calling this function
func (rf *Raft) runElection() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastBeat = time.Now()
	rf.electionTimeout = randomDuration(MinElectionTimeout, MaxElectionTimeout)

	DPrintf("(server %v, term %v): became a candidate", rf.me, rf.currentTerm)

	// send RequestVote() RPCs in parallel to all peers
	grantedVotes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			sent := rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.serverState != Candidate || args.Term != rf.currentTerm {
				return
			}
			if !sent {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.serverState = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				return
			}

			if reply.VoteGranted {
				grantedVotes++
			}

			if grantedVotes >= rf.majority && rf.serverState == Candidate {
				// I won the election, I'm the leader now
				DPrintf("(server %v, term %v): became the leader", rf.me, rf.currentTerm)
				rf.serverState = Leader
				rf.votedFor = -1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.replicateEntries()
			}
		}(i, args, reply)
	}
}

func (rf *Raft) heartBeatsBackgroundJob() {
	for {
		time.Sleep(HeartBeatPeriod)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.serverState != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.replicateEntries()
		rf.mu.Unlock()
	}
}

// Lock must be held before calling this function
func (rf *Raft) replicateEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      rf.log[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go func(server int, nextIndex int, matchIndex int, args AppendEntriesArgs, reply AppendEntriesReply) {
			sent := rf.sendAppendEntries(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if sent && reply.Term > rf.currentTerm {
				rf.serverState = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}

			if !sent ||
				rf.serverState != Leader ||
				rf.currentTerm != args.Term ||
				rf.nextIndex[server] != nextIndex ||
				rf.matchIndex[server] != matchIndex {
				return
			}

			if reply.Success {

				if len(args.Entries) > 0 {
					rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					DPrintf("(server %v, term %v): replicated entries from index %v to index %v on server %v", rf.me, rf.currentTerm, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server)
				}

				// search for N, such that
				// N > commitIndex && a majority of matchIndex[i] >= N && log[N].term == currentTerm
				var n int
				found := false
				for n = len(rf.log) - 1; n > rf.commitIndex; n-- {
					if rf.log[n].Term != rf.currentTerm {
						continue
					}

					replicationCount := 1
					for p := range rf.peers {
						if p == rf.me {
							continue
						}

						if rf.matchIndex[p] >= n {
							replicationCount++
						}
					}

					if replicationCount >= rf.majority {
						found = true
						break
					}
				}

				// commit all log entries up until log[n]
				if found {
					rf.commitIndex = n
					DPrintf("(server %v, term %v): commited all entries up to index %v", rf.me, rf.currentTerm, n)

					rf.applyCommittedEntries()
				}

			} else {
				// back up quickly over incorrect follower logs
				hasXTerm := slices.ContainsFunc(rf.log, func(entry LogEntry) bool {
					return entry.Term == reply.XTerm
				})

				if reply.XTerm != -1 {
					if !hasXTerm {
						rf.nextIndex[server] = reply.XIndex
					} else {
						var idx int
						for idx = len(rf.log) - 1; idx > 0; idx-- {
							if rf.log[idx].Term == reply.XTerm {
								break
							}
						}
						rf.nextIndex[server] = idx
					}
				} else {
					rf.nextIndex[server] = reply.XLen
				}
			}

		}(i, rf.nextIndex[i], rf.matchIndex[i], args, reply)
	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.me
	term := -1
	isLeader := rf.serverState == Leader

	if !isLeader {
		return index, term, isLeader
	}

	// append the entry to local log
	entry := LogEntry{Command: command, Term: rf.currentTerm, Index: len(rf.log)}
	rf.log = append(rf.log, entry)

	// attempt to replicate it on followers
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.replicateEntries()
	}()

	return entry.Index, rf.currentTerm, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.majority = len(peers)/2 + 1

	rf.currentTerm = 0
	rf.votedFor = -1

	dummyEntry := LogEntry{
		Command: nil,
		Term:    rf.currentTerm,
		Index:   0,
	}
	rf.log = []LogEntry{dummyEntry}

	rf.serverState = Follower
	rf.lastBeat = time.Now()
	rf.electionTimeout = randomDuration(MinElectionTimeout, MaxElectionTimeout)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//rand.New(rand.NewSource(time.Now().UnixNano()))
	go rf.electionTimerBackgroundJob()
	go rf.heartBeatsBackgroundJob()
	//go rf.commitBackgroundJob()

	return rf
}

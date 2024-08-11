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
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

const MinElectionTimeout = 500 * time.Millisecond
const MaxElectionTimeout = 1000 * time.Millisecond
const TimedActivityPeriod = 50 * time.Millisecond
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
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers
	currentTerm int
	votedFor    int // -1 in case I didn't vote
	log         []LogEntry

	// volatile state on all servers
	serverState     ServerState
	lastBeat        time.Time
	electionTimeout time.Duration
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
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(server %v, term %v): recevied RequestVote() from (server %v, term %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.serverState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.serverState = Follower
		rf.lastBeat = time.Now()
		rf.electionTimeout = randomDuration(MinElectionTimeout, MaxElectionTimeout)
		DPrintf("(server %v, term %v): granted vote to (server %v, term %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
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

	DPrintf("(server %v, term %v): sending RequestVote() to %v", rf.me, rf.currentTerm, server)

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()

	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(server %v, term %v): recevied AppendEntries() from (server %v, term %v)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if args.Term > rf.currentTerm {
		rf.serverState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.serverState = Follower
	rf.lastBeat = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(server %v, term %v): sending AppenEntries() to server %v", rf.me, rf.currentTerm, server)

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	return ok
}

func (rf *Raft) electionTimer() {
	for {
		time.Sleep(TimedActivityPeriod)

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

func (rf *Raft) runElection() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastBeat = time.Now()
	rf.electionTimeout = randomDuration(MinElectionTimeout, MaxElectionTimeout)

	DPrintf("(server %v, term %v): became a candidate", rf.me, rf.currentTerm)

	// send RequestVote() RPCs in parallel to all peers
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			sent := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.serverState != Candidate || args.Term != rf.currentTerm {
				return
			}

			if sent {
				if reply.Term > rf.currentTerm {
					rf.serverState = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					return
				}

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						// I won the election, I'm the leader now
						DPrintf("(server %v, term %v): became the leader", rf.me, rf.currentTerm)
						rf.serverState = Leader
						rf.votedFor = -1
						return
					}
				}
			}
		}(i, args, reply)

	}
}

func (rf *Raft) sendHeartBeats() {
	for {
		rf.mu.Lock()

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.serverState != Leader {
			rf.mu.Unlock()
			continue
		}

		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			Entries:  nil,
		}
		reply := AppendEntriesReply{}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
				sent := rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				if sent && reply.Term > rf.currentTerm {
					rf.serverState = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
				rf.mu.Unlock()

			}(i, args, reply)
		}

		rf.mu.Unlock()

		time.Sleep(HeartBeatPeriod)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}

	rf.serverState = Follower
	rf.lastBeat = time.Unix(0, 0) // beginning of time

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//rand.New(rand.NewSource(time.Now().UnixNano()))
	go rf.electionTimer()
	go rf.sendHeartBeats()

	return rf
}

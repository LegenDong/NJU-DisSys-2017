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

// import "bytes"
// import "encoding/gob"

import (
	"math/rand"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	// the server state 0, 1, 2
	Follower = iota
	Candidate
	Leader
)

const (
	// the heart beat interval from leader
	HeartbeatInterval       = 50 * time.Millisecond
	MinElectionInterval     = 150
	MaxElectionInterval     = 300
	DiffForElectionInterval = MaxElectionInterval - MinElectionInterval
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // the term now
	votedFor    int // the vote for
	voteCount   int // the vote get in election stage

	state int // follower, leader or candidate

	electionTimer *time.Timer // timer

	appendChan chan bool // commend append channel
	voteChan   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int // the term for the Candidate
	CandidateId int // the me for the Candidate
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // the term from the follower
	VoteGranted bool // true if get the vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// term in candidate old than this follower
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor == -1 {
		// haven't vote to any server
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// the current server is older than the candidate
	// TODO a server may vote for more than one server, may cause bug, need fix
	if args.Term > rf.currentTerm {
		rf.ResetTimer()

		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}
}

type AppendEntriesArgs struct {
	Term     int // the term of the leader
	LeaderId int // the heart beat from
}

type AppendEntriesReply struct {
	Term    int  // the term get from the follower
	Success bool // the result of append entry method
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// the heart beat fall wrong
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// heart beat pass, update the term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	go func() {
		rf.appendChan <- true
	}()

}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) ResetTimer() {
	// timer for election
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(time.Duration(MinElectionInterval+rand.Int()%DiffForElectionInterval) * time.Millisecond)
	} else {
		rf.electionTimer.Reset(time.Duration(MinElectionInterval+rand.Int()%DiffForElectionInterval) * time.Millisecond)
	}
}

func (rf *Raft) BroadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			rf.SendAppendEntries(server, args, &reply)
			if reply.Success {
				// nothing to do
			} else {
				// many threads execute same time, need lock
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.mu.Unlock()
				}
			}

		}(i, args)
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me // the candidate will vote for itself
	rf.voteCount = 1
	rf.ResetTimer() // reset timer
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, args, &reply) {
				if reply.VoteGranted == true {
					rf.mu.Lock()
					rf.voteCount += 1
					rf.mu.Unlock()
				} else {
					// nothing to do, will not reach
				}
			}
		}(i)
	}
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

	// Your initialization code here.

	// -1 means no candidate you are voting
	rf.votedFor = -1
	rf.state = Follower
	rf.appendChan = make(chan bool)
	rf.voteChan = make(chan bool)

	rf.ResetTimer()

	go func() {
		for {
			switch rf.state {
			case Follower:
				select {
				// wait until one of the case is satisfied
				case <-rf.appendChan:
					rf.ResetTimer()
				case <-rf.voteChan:
					rf.ResetTimer()
				case <-rf.electionTimer.C:
					// the time for waiting for heart beat is out, change the state to candidate
					rf.state = Candidate
					rf.StartElection()
				}
			case Candidate:
				select {
				case <-rf.appendChan:
					// get the heart beat from one server
					// the leader is cheesed, become the follower
					rf.state = Follower
				case <-rf.electionTimer.C:
					// time for election is out, another is begin
					rf.StartElection()
				default:
					// more than half, become the leader
					if rf.voteCount > len(rf.peers)/2 {
						rf.state = Leader
					}
				}
			case Leader:
				// leader create the heart beat message every HeartbeatInterval
				rf.BroadcastAppendEntries()
				time.Sleep(HeartbeatInterval)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

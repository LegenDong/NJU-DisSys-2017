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
	"bytes"
	"encoding/gob"
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
	HeartbeatInterval = 600
	RandomInterval    = 150
)

type LogEntry struct {
	Term int
	Log  interface{}
}

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

	//persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//extend
	state          int
	stateCh        chan int
	heartBeatTimer *time.Timer
	commitTimer    *time.Ticker
	applyCh        chan ApplyMsg

	raftData interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.raftData)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.raftData)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // the term for the Candidate
	CandidateId  int // the me for the Candidate
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // the term from the follower
	VoteGranted bool // true if get the vote
	Timeout     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Follower {
		rf.ResetHeartBeatTimer()
	}

	// term in candidate old than this follower
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.UpdateNewTerm(args.Term)
		rf.stateCh <- Follower
	}

	logIndexSelf := len(rf.log) - 1

	var isNew bool
	if args.LastLogTerm == rf.log[logIndexSelf].Term {
		isNew = args.LastLogIndex >= logIndexSelf
	} else {
		isNew = args.LastLogTerm > rf.log[logIndexSelf].Term
	}

	if (rf.votedFor == -1 || rf.me == args.CandidateId) && isNew {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) checkCanCommitted() {
	rf.mu.Lock()

	n := rf.commitIndex
	serverNum := len(rf.peers)
	rf.mu.Unlock()
	okNum := serverNum
	for okNum > serverNum/2 {
		okNum = 0
		n++
		for i := 0; i < serverNum; i++ {
			if n <= rf.matchIndex[i] {
				okNum++
			}
		}
	}

	canCommitIndex := n - 1
	beginCommitIndex := rf.commitIndex + 1
	for i := canCommitIndex; i >= beginCommitIndex; i-- {
		if rf.currentTerm == rf.log[i].Term {
			rf.commitIndex = i

			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
			break
		} else {

		}
	}
}

type AppendEntriesArgs struct {
	Term         int // the term of the leader
	LeaderId     int // the heart beat from
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int  // the term get from the follower
	Success        bool // the result of append entry method
	NeedCheckIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if rf.state == Follower {
		rf.ResetHeartBeatTimer()
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

		rf.stateCh <- Follower
	}

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NeedCheckIndex = -1
		rf.mu.Unlock()
		return
	}
	if len(rf.log)-1 < args.PrevLogIndex {

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NeedCheckIndex = len(rf.log)
		rf.mu.Unlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NeedCheckIndex = max(args.PrevLogIndex-50, 1)
		rf.mu.Unlock()
		return
	}

	if args.LeaderId != rf.votedFor {

		reply.Term = args.Term
		reply.Success = false
		reply.NeedCheckIndex = -1
		if rf.votedFor == -1 {
			rf.votedFor = args.LeaderId
		}
		rf.mu.Unlock()
		return
	}

	beginIndex := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		if cap(rf.log) < beginIndex+len(args.Entries) {

		}
		if i+beginIndex < len(rf.log) {
			if rf.log[i+beginIndex] != args.Entries[i] {
				rf.log[i+beginIndex] = args.Entries[i]
				rf.log = rf.log[: i+beginIndex+1 : i+beginIndex+1]
			}

		} else {
			rf.log = append(rf.log, args.Entries[i])
		}

	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if len(args.Entries) != 0 {
		rf.persist()
	}

	rf.mu.Unlock()
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
send:
	if rf.state != Leader {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	if ok {
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			go rf.checkCanCommitted()
		} else {
			if reply.Term > rf.currentTerm {
				rf.UpdateNewTerm(reply.Term)
				rf.stateCh <- Follower
				rf.persist()
			} else {
				if reply.NeedCheckIndex != -1 {
					rf.nextIndex[server] = min(reply.NeedCheckIndex, rf.nextIndex[server])
				} else if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}

				rf.initPeerEntries(args, server)
				rf.mu.Unlock()
				goto send
			}
		}
	}

	rf.mu.Unlock()
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
	if ok {
		reply.Timeout = false
	} else {
		reply.Timeout = true
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isLeader = true
		index = len(rf.log)
		term = rf.currentTerm

		rf.log = append(rf.log, LogEntry{term, command})
		rf.persist()
		go rf.BroadcastAppendEntries()
	} else {
		isLeader = false
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

	rf.mu.Lock()

	close(rf.applyCh)
	rf.applyCh = nil
	close(rf.stateCh)
	rf.stateCh = nil
	rf.mu.Unlock()
}

func (rf *Raft) CreateHeartBeatTimer() {
	rf.heartBeatTimer = time.NewTimer(time.Duration(rand.Intn(RandomInterval)+HeartbeatInterval) * time.Millisecond)
	go func() {
		for {
			<-rf.heartBeatTimer.C
			if rf.stateCh == nil {
				break
			}
			rf.stateCh <- Candidate
		}
	}()
}

func (rf *Raft) ResetHeartBeatTimer() {
	rf.heartBeatTimer.Reset(time.Duration(rand.Intn(RandomInterval)+HeartbeatInterval) * time.Millisecond)
}

func (rf *Raft) BroadcastAppendEntries() {
	rf.mu.Lock()
	l := len(rf.peers)
	for i := 0; i < l; i++ {
		if i == rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = len(rf.log) - 1
			continue
		}
		appendEntriesArgs := new(AppendEntriesArgs)
		rf.initPeerEntries(appendEntriesArgs, i)

		appendEntriesReply := new(AppendEntriesReply)
		appendEntriesReply.Success = false
		appendEntriesReply.Term = -1
		appendEntriesReply.NeedCheckIndex = -1

		go rf.SendAppendEntries(i, appendEntriesArgs, appendEntriesReply)
	}

	rf.mu.Unlock()
}

func (rf *Raft) CreateCommitTimer() {
	rf.commitTimer = time.NewTicker(200 * time.Millisecond)
	for range rf.commitTimer.C {
		rf.mu.Lock()
		cIndex := rf.commitIndex
		for cIndex > rf.lastApplied {
			rf.lastApplied++
			log := rf.log[rf.lastApplied]

			//apply
			rf.raftData = log.Log
			rf.persist()

			func(index int) {
				rf.applyCh <- ApplyMsg{index, rf.log[index].Log, false, nil}
			}(rf.lastApplied)

		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) UpdateNewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) CyclicStateMachine() {
	for {
		switch rf.state {
		case Follower:
			if rf.heartBeatTimer == nil {
				rf.CreateHeartBeatTimer()
			} else {
				rf.ResetHeartBeatTimer()
			}

			rf.state = <-rf.stateCh
		case Candidate:
			rf.mu.Lock()
			rf.UpdateNewTerm(rf.currentTerm + 1)
			rf.mu.Unlock()
			rf.heartBeatTimer.Stop()
			go time.AfterFunc(time.Duration(rand.Intn(RandomInterval)+HeartbeatInterval)*time.Millisecond, func() {
				if rf.state == Candidate {
					rf.stateCh <- Candidate
				}
			})
			go rf.atCandidate()
			rf.state = <-rf.stateCh

		case Leader:
			rf.heartBeatTimer.Stop()
			go rf.atLeader()
			rf.state = <-rf.stateCh
		default:
		}
	}
}

func (rf *Raft) initPeerEntries(appendEntriesArgs *AppendEntriesArgs, i int) {
	appendEntriesArgs.Term = rf.currentTerm
	appendEntriesArgs.LeaderId = rf.me
	appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
	appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].Term
	appendEntriesArgs.LeaderCommit = rf.commitIndex
	appendEntriesArgs.Entries = make([]LogEntry, 0)

	logTag := len(rf.log)
	for j := rf.nextIndex[i]; j < logTag; j++ {
		appendEntriesArgs.Entries = append(appendEntriesArgs.Entries, rf.log[j])
	}
}

func (rf *Raft) atLeader() {
	for rf.state == Leader {
		rf.BroadcastAppendEntries()
		time.Sleep((time.Duration(rand.Intn(RandomInterval)+HeartbeatInterval) * time.Millisecond) / 2)
	}
}

func (rf *Raft) atCandidate() {
	rf.mu.Lock()
	var requestVoteArgs RequestVoteArgs
	requestVoteReplys := make([]RequestVoteReply, len(rf.peers))
	requestVoteArgs.Term = rf.currentTerm
	requestVoteArgs.CandidateId = rf.me
	requestVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	requestVoteArgs.LastLogIndex = len(rf.log) - 1
	rf.mu.Unlock()

	replyCh := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		requestVoteReplys[i].Timeout = true
		go func(i int) {
			ok := rf.sendRequestVote(i, requestVoteArgs, &requestVoteReplys[i])
			if replyCh != nil {
				replyCh <- ok
			}
		}(i)
	}

	for i := 0; i < len(rf.peers); i++ {
		ok := <-replyCh
		if !ok {
			continue
		}

		var voteSuccess int
		for i := 0; i < len(requestVoteReplys); i++ {
			if !requestVoteReplys[i].Timeout && requestVoteReplys[i].VoteGranted && requestVoteReplys[i].Term == rf.currentTerm {
				voteSuccess++
			}

			if requestVoteReplys[i].Term > rf.currentTerm {
				rf.mu.Lock()
				rf.UpdateNewTerm(requestVoteReplys[i].Term)
				rf.persist()
				rf.mu.Unlock()
				rf.stateCh <- Follower
			}
		}

		if voteSuccess == len(requestVoteReplys)/2+1 {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log) - 1
				} else {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
			rf.stateCh <- Leader
		}
	}
	close(replyCh)
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
	rf.stateCh = make(chan int)
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.CreateCommitTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.CyclicStateMachine()

	return rf
}

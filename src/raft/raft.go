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
	"github.com/yunuskilicdev/distributedsystems/src/labgob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "github.com/yunuskilicdev/distributedsystems/src/labrpc"

// import "bytes"
// import "../labgob"

const (
	Follower = iota
	Candidate
	Leader
)

type CommandTerm struct {
	Command interface{}
	Term    int
}

const MinElectionTimeout = 500
const MaxElectionTimeout = 1000

func randTimeout() time.Duration {
	randTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status       int
	applyMsg     chan ApplyMsg
	currentTerm  int
	votedFor     int
	log          []CommandTerm
	commitIndex  int
	lastApplied  int
	lastLogIndex int
	nextIndex    []int
	matchIndex   []int
	lastAccessed time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.status == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor := 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = len(rf.log) - 1
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.status = Follower
	}
	reply.Term = args.Term
	if rf.lastLogIndex-1 >= 0 {
		lastLogTerm := rf.log[rf.lastLogIndex-1].Term
		if lastLogTerm > args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
	}
	rf.status = Follower
	rf.lastAccessed = time.Now()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []CommandTerm
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Xterm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if len(rf.log) < args.PrevLogIndex+1 {
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Xterm = rf.log[args.PrevLogIndex].Term
		for i, v := range rf.log {
			if v.Term == reply.Xterm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	index := 0
	for ; index < len(args.Entry); index++ {
		currentIndex := args.PrevLogIndex + 1 + index
		if currentIndex > len(rf.log)-1 {
			break
		}
		if rf.log[currentIndex].Term != args.Entry[index].Term {
			rf.log = rf.log[:currentIndex]
			rf.lastLogIndex = len(rf.log) - 1
			rf.persist()
			break
		}
	}

	reply.Success = true
	rf.lastAccessed = time.Now()
	if len(args.Entry) > 0 {
		rf.log = append(rf.log, args.Entry[index:]...)
		rf.lastLogIndex = len(rf.log) - 1
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		min := min(args.LeaderCommit, rf.lastLogIndex)
		for i := rf.commitIndex + 1; i <= min; i++ {
			rf.commitIndex = i
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == Leader
	if !isLeader {
		return 0, 0, false
	}

	term = rf.currentTerm
	rf.log = append(rf.log, CommandTerm{
		Command: command,
		Term:    term,
	})
	rf.lastLogIndex = len(rf.log) - 1
	index = rf.lastLogIndex
	rf.persist()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) manageLifecycle() {
	for true {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == Follower {
			rf.manageFollower()
		} else if status == Candidate {
			rf.manageCandidate()
		} else if status == Leader {
			rf.manageLeader()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) manageFollower() {
	duration := randTimeout()
	time.Sleep(duration)
	rf.mu.Lock()
	lastAccessed := rf.lastAccessed
	rf.mu.Unlock()
	if time.Now().Sub(lastAccessed).Milliseconds() >= duration.Milliseconds() {
		rf.mu.Lock()
		rf.status = Candidate
		rf.currentTerm++
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) manageCandidate() {
	timeOut := randTimeout()
	start := time.Now()
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	count := 0
	total := len(peers)
	finished := 0
	majority := (total / 2) + 1
	for peer := range peers {
		if me == peer {
			rf.mu.Lock()
			count++
			finished++
			rf.mu.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = me
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				finished++
				return
			}
			if reply.VoteGranted {
				finished++
				count++
			} else {
				finished++
				if args.Term < reply.Term {
					rf.status = Follower
					rf.persist()
				}
			}
		}(peer)
	}

	for {
		rf.mu.Lock()
		if count >= majority || finished == total || time.Now().Sub(start).Milliseconds() >= timeOut.Milliseconds() {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	if time.Now().Sub(start).Milliseconds() >= timeOut.Milliseconds() {
		rf.status = Follower
		rf.mu.Unlock()
		return
	}

	if rf.status == Candidate && count >= majority {
		rf.status = Leader
		for peer := range peers {
			rf.nextIndex[peer] = rf.lastLogIndex + 1
		}
	} else {
		rf.status = Follower
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {

	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	peers := rf.peers
	nextIndex := rf.nextIndex

	lastLogIndex := rf.lastLogIndex
	matchIndex := rf.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := rf.log
	rf.mu.Unlock()
	for n := commitIndex + 1; n <= lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= n && log[n].Term == term {
				count++
			}
		}

		if count >= majority {
			rf.mu.Lock()
			i := rf.commitIndex + 1
			for ; i <= n; i++ {
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      log[i].Command,
					CommandIndex: i,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Unlock()
		}
	}

	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log[prevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me
		if nextIndex[peer] <= lastLogIndex {
			args.Entry = rf.log[prevLogIndex+1 : lastLogIndex+1]
		}
		rf.mu.Unlock()

		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Success {
				rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entry), rf.lastLogIndex+1)
				rf.matchIndex[peer] = prevLogIndex + len(args.Entry)
			} else {
				if reply.Term > args.Term {
					rf.status = Follower
					rf.mu.Unlock()
					return
				}
				if reply.Xterm == -1 {
					rf.nextIndex[peer] = reply.XLen
					rf.mu.Unlock()
					return
				}
				index := -1
				for i, v := range rf.log {
					if v.Term == reply.Xterm {
						index = i
					}
				}
				if index == -1 {
					rf.nextIndex[peer] = reply.XIndex
				} else {
					rf.nextIndex[peer] = index
				}
			}
			rf.mu.Unlock()
		}(peer)
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
	rf.mu.Lock()
	rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.log = []CommandTerm{
		{
			Command: nil,
			Term:    0,
		},
	}
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyMsg = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.manageLifecycle()

	return rf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

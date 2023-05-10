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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/arindas/mit-6.824-distributed-systems/pkg/labgob"
	"github.com/arindas/mit-6.824-distributed-systems/pkg/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type aeMessage struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	isTimingOut bool

	// Your data here (2A, 2B, 2C).
	currentTerm int
	votedFor    int // candidateID
	leaderId    int
	state       string
	aeChan      chan aeMessage
	leaderChan  chan aeMessage
	// blackList   []int
	log []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "leader"
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	VoteFor     int
	VoteGranted bool
	// Voted       bool
}

type AppendEntries struct {
	// Why dont you just call this Heartbeat?!
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit string
}

type AppendEntriesReply struct {
	Accepted bool
}

func (rf *Raft) Heartbeat(ae *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.currentTerm < ae.Term {
		DPrintf("%v receive HEARTBEAT from %v", rf.me, ae.LeaderID)
		rf.state = "follower"
		rf.votedFor = -1
		rf.currentTerm = ae.Term
		// TODO: also need a channel to send update log!!!
		rf.leaderId = ae.LeaderID
		rf.aeChan <- aeMessage{}
		reply.Accepted = true
	} else if rf.currentTerm == ae.Term {
		if rf.state == "leader" {
			rf.state = "follower"
			reply.Accepted = false
		} else {
			DPrintf("%v receive HEARTBEAT from %v", rf.me, ae.LeaderID)
			rf.state = "follower"
			rf.leaderId = ae.LeaderID
			rf.votedFor = -1
			rf.aeChan <- aeMessage{}
			reply.Accepted = true
		}
	} else {
		reply.Accepted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) startHeartBeat() {
	for {
		rf.mu.Lock()
		cur_term := rf.currentTerm
		me := rf.me
		rf.mu.Unlock()

		not_accepted := 0
		splitBrain_chance := 0
		args := &AppendEntries{
			Term:     cur_term,
			LeaderID: me,
		}

		rf.mu.Lock()
		if rf.state != "leader" {
			rf.leaderChan <- aeMessage{}
			rf.mu.Unlock()
			break
		}

		for id := range rf.peers {
			if id != me {
				// if !isIn(id, rf.blackList) {
				reply := &AppendEntriesReply{Accepted: true}
				DPrintf("%v sent HEARTBEAT to %v", me, id)
				if !rf.sendHeartbeat(id, args, reply) {
					DPrintf("%v DEAD to %v", me, id)
					not_accepted += 1
				}
				if !reply.Accepted {
					splitBrain_chance += 1
				}
			}
		}

		if not_accepted+1 >= len(rf.peers) {
			rf.state = "follower"
			rf.leaderId = -1
			DPrintf("IM DEAD")
			rf.mu.Unlock()
			break
		}

		if splitBrain_chance >= 1 {
			rf.leaderChan <- aeMessage{}
			rf.mu.Unlock()
			break
		}

		rf.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		DPrintf("raft %v Receive request from %v for term %v", rf.me, args.CandidateID, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = args.Term
		reply.VoteGranted = true
		reply.VoteFor = args.CandidateID
	} else if rf.currentTerm == args.Term {
		if rf.votedFor < 0 { // not yet vote
			DPrintf("raft %v Receive request from %v for term %v", rf.me, args.CandidateID, args.Term)
			// DPrintf("%v didn't voted Term %v", rf.me, rf.currentTerm)
			rf.votedFor = args.CandidateID
			reply.Term = args.Term
			reply.VoteFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			// reply.Term = args.Term
			// DPrintf("%v voted on Term", rf.me)
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
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

func (rf *Raft) sendHeartbeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// rf.timeout(true)
	go rf.startLeaderElection()
	for !rf.killed() {
		DPrintf("Raft %v tick", rf.me)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// The way is sleep blocking technique right in this for loop!!!
		r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
		// no need waitGroup, here block forever
		timer := rf.timeout(true)
		for range rf.aeChan { // received HEARTBEAT
			// already check if term is latest
			new_duration := time.Duration((6 + r.Intn(7)) * int(time.Second))
			rf.mu.Lock()
			DPrintf("%v: Reset timer for %v seconds", rf.me, new_duration)
			rf.mu.Unlock()
			if !timer.Reset(new_duration) {
				timer = rf.timeout(true)
			}
		}
	}
}

func (rf *Raft) startLeaderElection() {
	for range rf.leaderChan { // block
		rf.timeout(false) // also kick off timeout

		rf.mu.Lock()
		DPrintf("%v Start leader election", rf.me)
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.leaderId = -1
		rf.state = "candidate"
		gather_vote := 1
		quorum := 1
		current_term := rf.currentTerm
		me := rf.me
		rf.mu.Unlock()

		x := []int{}
		for id := range rf.peers {
			if id != me {
				DPrintf("%v sent request vote to %v with current term %v", me, id, current_term)
				vote_me := &RequestVoteArgs{
					Term:        current_term,
					CandidateID: me,
				}
				reply := &RequestVoteReply{VoteGranted: false}
				if !rf.sendRequestVote(id, vote_me, reply) { // send request vote here!!!
					// quorum -= 1
				} else {
					if reply.VoteGranted {
						x = append(x, id)
						gather_vote += 1
					}
					quorum += 1
				}

			}
		}

		rf.mu.Lock()
		DPrintf("%v Done sent, check vote: %v>%v from %+v", me, gather_vote, quorum/2, x)
		if gather_vote > quorum/2 && quorum > 1 && rf.state == "candidate" { // already floor division
			rf.state = "leader"
			rf.leaderId = rf.me
			rf.votedFor = rf.me
			go rf.startHeartBeat()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			continue
		}
	}
}

// Automatic check if no leader, start a leader election
func (rf *Raft) timeout(from_ticker bool) *time.Timer {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	new_duration := time.Duration((6 + r.Intn(7)) * int(time.Second))
	timer := time.NewTimer(new_duration)
	go func() { // should start another go-routine since this maybe call in multiple concurrent cases
		tStart := time.Now()
		<-timer.C // block this shit
		tStop := time.Now()

		rf.mu.Lock()
		var name string
		if from_ticker {
			name = "ticker"
		} else {
			name = "leader election"
		}
		DPrintf("%v Slept for %s from %v", rf.me, tStop.Sub(tStart), name)
		rf.mu.Unlock()

		// case 1: heartbeat time out, reset everything
		// case 2: leader timeout
		if from_ticker { // from heartbeat
			rf.mu.Lock()
			if rf.state == "follower" {
				rf.leaderChan <- aeMessage{}
			}
			rf.mu.Unlock()
		} else { // from leader election
			rf.mu.Lock()
			if rf.state == "candidate" {
				rf.leaderChan <- aeMessage{}
			}
			rf.mu.Unlock()
		}
	}()

	return timer
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
	rf.isTimingOut = false

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderChan = make(chan aeMessage)
	rf.aeChan = make(chan aeMessage)
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.state = "follower"
	// rf.blackList = make([]int, 0)

	// DPrintf("%v", rf.blackList)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

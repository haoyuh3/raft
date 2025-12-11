//
// raft.go
// =======
// Implements multiple Raft peers to form a Raft consensus cluster
// Implements leader election, log replication, and safety properties

package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"raftProject/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const HeartbeatInterval = 100 // in milliseconds

const (
	ElectionTicker      = 10 // in milliseconds
	ApplyCommitedTicker = 10
)

// Election timeout range
const (
	ElectionTimeoutMin = 300 // in milliseconds
	ElectionTimeoutMax = 600 // in milliseconds
)

// Define server roles
type serverRole int

const (
	Follower serverRole = iota
	Candidate
	Leader
)

// candidateVoteCnt
// =================
//
// Track votes received by a candidate during an election
type candidateVoteCnt struct {
	peer    int
	voteCnt int
	voteMux sync.Mutex
}

type ApplyCommand struct {
	Index   int
	Command interface{}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft struct
// ===========
type Raft struct {
	// Rpc peers
	mux   sync.Mutex
	peers []*rpc.ClientEnd
	me    int

	// Logger
	logger *log.Logger

	// Persistent State on Server
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile State on Server
	commitIndex int
	lastApplied int

	// Volatile State on Leaders
	nextIndex  []int
	matchIndex []int

	// Timer
	lastHeartbeat     time.Time
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	// Role
	role serverRole

	// Apply channel
	applyCh chan ApplyCommand

	// Shutdown channel
	shutdown chan struct{}
}

// GetState
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	var me int
	var term int
	var isLeader bool
	me = rf.me
	term = rf.currentTerm
	isLeader = rf.role == Leader
	return me, term, isLeader
}

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply
// ================
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs
// =================
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply
// ==================
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote
// ===========
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Acquire lock
	rf.mux.Lock()
	defer rf.mux.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm:
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}
	reply.Term = rf.currentTerm // set reply term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// Grant vote
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
	} else {
		// Deny vote
		reply.VoteGranted = false
	}

}

// A candidate's log is considered at least as up-to-date as
// the receiver's log if its last log term is greater than
// the receiver's last log term, or if the last log terms
// are equal and the candidate's last log index is greater
// than or equal to the receiver's last log index (§5.4.1)
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 && len(rf.log) > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	if candidateLastLogTerm > lastLogTerm { // compare term
		return true
	} else if candidateLastLogTerm < lastLogTerm {
		return false
	} else {
		// candidateLastLogTerm == lastLogTerm
		return candidateLastLogIndex >= lastLogIndex
	}
}

// AppendEntries
// =============
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// require lock
	rf.mux.Lock()
	defer rf.mux.Unlock()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Change To Follower
	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	// Check Log Consistency
	if args.PrevLogIndex >= len(rf.log) ||
		(args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// Log inconsistency
		reply.Success = false
		return
	}

	// Exist New entries to append
	if len(args.Entries) > 0 {
		// Append any new entries not already in the log
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}
	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries
// ===============
//
// Example code to send a AppendEntries RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *AppendEntriesArgs -- RPC arguments in args
//
// reply *AppendEntriesReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	index := -1
	term := -1
	isLeader := rf.role == Leader
	if !isLeader {
		rf.mux.Unlock()
		return index, term, isLeader
	}

	// Append command to log
	term = rf.currentTerm
	newLogEntry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	index = len(rf.log) - 1

	rf.mux.Unlock()
	go rf.sendReplicateLogsToAll()
	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	close(rf.shutdown)
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	// Initialize volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Initialize role
	rf.role = Follower
	rf.lastHeartbeat = time.Now()

	// Set timing parameters
	rf.heartbeatInterval = HeartbeatInterval * time.Millisecond // 10 heartbeats per second max
	rf.electionTimeout = rf.randomElectionTimeout()

	// Initialize leader state (will be set when becoming leader)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Initialize shutdown channel
	rf.shutdown = make(chan struct{})

	// Start background goroutines
	go rf.electionTimer()  // monitor election timeouts
	go rf.heartbeatTimer() // send heartbeats if leader
	go rf.applyCommitted() // apply committed log entries
	return rf
}

// applyCommitted
// ==============
//
// Apply committed log entries to the state machine
func (rf *Raft) applyCommitted() {
	ticker := time.NewTicker(ApplyCommitedTicker * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rf.mux.Lock()

			// Apply Commited Entries
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++

				if rf.lastApplied >= len(rf.log) {
					// No more entries to apply
					break
				}
				entry := rf.log[rf.lastApplied]
				applyMsg := ApplyCommand{
					Index:   rf.lastApplied,
					Command: entry.Command,
				}
				// Release lock before sending to channel (avoid deadlock)
				rf.mux.Unlock()
				rf.applyCh <- applyMsg
				rf.mux.Lock() // Re-acquire lock for next iteration
			}
			rf.mux.Unlock()

		case <-rf.shutdown:
			return
		}
	}
}

// electionRequestVote
// ===================
//
// Send a RequestVote RPC to a specific peer during an election
func (rf *Raft) electionRequestVote(peer int, term int, candidateId int, lastLogIndex int, lastLogTerm int, cnt *candidateVoteCnt) {
	// send RequestVote RPC to peer
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, reply)
	if ok {
		rf.mux.Lock()
		defer rf.mux.Unlock()
		if rf.role != Candidate || rf.currentTerm != term {
			// No longer candidate or term has changed
			return
		}
		if reply.Term > rf.currentTerm {
			// Convert to follower if we discover a higher term
			rf.changeToFollower(reply.Term)
			return
		}

		// vote granted
		if reply.VoteGranted {
			cnt.voteMux.Lock()
			cnt.voteCnt++ // increment vote count
			voteCnt := cnt.voteCnt
			cnt.voteMux.Unlock()

			// Check if we have won the election
			if voteCnt > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
	}
}

// startElection
// =============
//
// Start a new election by converting to candidate,
// incrementing current term, voting for self, and
// sending RequestVote RPCs to all other peers
func (rf *Raft) startElection() {
	rf.mux.Lock()

	rf.role = Candidate                             // Convert to candidate
	rf.currentTerm++                                // increment current term
	rf.votedFor = rf.me                             // vote for self
	rf.lastHeartbeat = time.Now()                   // reset last heartbeat time
	rf.electionTimeout = rf.randomElectionTimeout() // reset election timeout

	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 && len(rf.log) > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term // get last log term
	}
	rf.mux.Unlock()

	// Initialize vote counter
	candidateVote := &candidateVoteCnt{
		peer:    candidateId,
		voteCnt: 1, // vote for self
	}

	// Send RequestVote RPCs to all other peers
	for peer := range rf.peers {
		if peer != candidateId {
			go rf.electionRequestVote(peer, currentTerm, candidateId, lastLogIndex, lastLogTerm, candidateVote) // send RequestVote RPC to peer
		}
	}
}

// electionTimer
// =============
//
// Monitor election timeouts and start elections as needed
func (rf *Raft) electionTimer() {
	ticker := time.NewTicker(ElectionTicker * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rf.shutdown:
			return
		case <-ticker.C:
			rf.mux.Lock()
			role := rf.role
			elapsedTime := time.Since(rf.lastHeartbeat) // time since last heartbeat
			timeout := rf.electionTimeout               // current election timeout
			rf.mux.Unlock()

			// Only followers and candidates have election timeouts
			if role != Leader && elapsedTime >= timeout {
				rf.startElection()
			}
		}
	}
}

// sendAppendEntriesToPeer
// ========================
//
// Send AppendEntries RPC to a specific peer
// This handles both heartbeats (empty entries) and log replication
func (rf *Raft) sendAppendEntriesToPeer(server int, isHeartbeat bool) {
	rf.mux.Lock()
	if rf.role != Leader {
		rf.mux.Unlock()
		return
	}

	// Prepare AppendEntries RPC arguments
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	var entries []LogEntry
	if !isHeartbeat && nextIndex < len(rf.log) {
		// Send log entries starting from nextIndex (only for replication, not heartbeat)
		entries = make([]LogEntry, len(rf.log)-nextIndex)
		copy(entries, rf.log[nextIndex:])
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	currentTerm := rf.currentTerm
	rf.mux.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, reply)

	if ok {
		rf.mux.Lock()
		defer rf.mux.Unlock()

		// Ignore stale responses
		if rf.role != Leader || rf.currentTerm != currentTerm {
			return
		}

		// If found higher term, convert to follower
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term)
			return
		}

		// Handle response
		if reply.Success {
			// Update nextIndex and matchIndex
			newMatchIndex := prevLogIndex + len(entries)
			matchIndexUpdated := false
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				matchIndexUpdated = true
			}

			// Try to commit if matchIndex was updated
			// (even heartbeats can confirm log replication)
			if matchIndexUpdated {
				rf.tryCommitLogEntries()
			}
		} else {
			// If AppendEntries fails due to log inconsistency, decrement nextIndex
			rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
		}
	}
}

// replicateLog
// =============
//
// Replicate log entries to a specific follower
// Keeps retrying until all entries are replicated
func (rf *Raft) replicateLog(server int) {
	for {
		rf.mux.Lock()
		if rf.role != Leader {
			rf.mux.Unlock()
			return
		}

		// Check if log is up-to-date
		nextIndex := rf.nextIndex[server]
		hasEntriesToSend := nextIndex < len(rf.log)
		rf.mux.Unlock()

		if !hasEntriesToSend {
			// No more entries to send, exit
			return
		}

		// Send AppendEntries with log entries (will retry on failure)
		rf.sendAppendEntriesToPeer(server, false)
	}
}

func (rf *Raft) sendReplicateLogsToAll() {
	rf.mux.Lock()
	if rf.role != Leader {
		rf.mux.Unlock()
		return
	}
	rf.mux.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateLog(i)
		}
	}
}

// tryCommitLogEntries
// ===================
//
// Try to commit log entries if a majority of peers have replicated them
// Note: This function assumes that the caller holds the rf.mux lock
// - Leader only commits entries from its current term
// - Once a current-term entry is committed, all prior entries are also committed
func (rf *Raft) tryCommitLogEntries() {
	// Find the highest index N such that:
	// 1. N > commitIndex
	// 2. A majority of matchIndex[i] >= N
	// 3. log[N].term == currentTerm
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Only commit entries from current term (Raft paper §5.4.2)
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// Count how many servers have replicated this entry
		count := 1 // count self
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}

		// If a majority have replicated this entry, commit it
		// This also commits all previous entries (commitIndex advances to n)
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			return // Found the highest committable index
		}
	}
}

// becomeLeader
// ============
//
// Convert the current Raft peer to a leader
func (rf *Raft) becomeLeader() {
	// Check if still a candidate
	if rf.role != Candidate {
		return
	}

	rf.role = Leader

	// Initialize leader state
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// Raft paper §5.2: Upon election, send initial empty AppendEntries
	// RPCs (heartbeat) to each server to establish authority and
	// prevent new elections
	go rf.sendHeartbeatsToAll()
	go rf.sendReplicateLogsToAll()
}

// heartbeatTimer
// ==============
//
// Send heartbeats to all peers at regular intervals
func (rf *Raft) heartbeatTimer() {
	ticker := time.NewTicker(rf.heartbeatInterval) // set heartbeat interval ticker
	defer ticker.Stop()
	for {
		select {
		case <-rf.shutdown:
			return
		case <-ticker.C:
			_, _, isLeader := rf.GetState()

			if isLeader {
				rf.sendHeartbeatsToAll()
			}
		}
	}
}

// sendHeartbeatsToAll
// ====================
//
// Send heartbeats (AppendEntries RPCs with no log entries) to all peers
func (rf *Raft) sendHeartbeatsToAll() {
	rf.mux.Lock()
	if rf.role != Leader {
		rf.mux.Unlock()
		return
	}
	rf.mux.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i, true) // true = heartbeat (empty entries)
	}
}

// changeToFollower
// =================
//
// Convert the current Raft peer to a follower
func (rf *Raft) changeToFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
}

// randomElectionTimeout
// =====================
//
// Generate a random election timeout duration
func (rf *Raft) randomElectionTimeout() time.Duration {
	minV := ElectionTimeoutMin
	maxV := ElectionTimeoutMax
	return time.Duration(minV+rand.Intn(maxV-minV)) * time.Millisecond
}

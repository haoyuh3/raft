//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

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

const (
	ElectionTimeoutMin = 300 // in milliseconds
	ElectionTimeoutMax = 600 // in milliseconds
)

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Define server roles
type serverRole int

const (
	Follower serverRole = iota
	Candidate
	Leader
)

// LogEntry
// ========
//
// A single log entry
type LogEntry struct {
	Term    int
	Command interface{}
}

// candidateVoteCnt
// =================
//
// Track votes received by a candidate during an election
type candidateVoteCnt struct {
	peer    int
	voteCnt int
	voteMux sync.Mutex
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Edstem or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	// Persistent state on all servers:
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders: Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	role serverRole
	// shutdown channel
	shutdown chan struct{}

	// timing parameters
	lastHeartbeat     time.Time
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	// apply channel
	applyCh chan ApplyCommand
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

// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs
// =================
//
// Example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply
// ==================
//
// Example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// require lock
	rf.mux.Lock()
	defer rf.mux.Unlock()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// voteFor depend on log up-to-date check
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower // convert to follower if exist higher term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// Reset election timer
		rf.lastHeartbeat = time.Now()
	} else {
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
//
// Example AppendEntries RPC handler
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

	// Convert to follower if we receive AppendEntries from valid leader
	rf.role = Follower
	rf.lastHeartbeat = time.Now() // Reset election timer
	reply.Term = rf.currentTerm

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// check the log consistency
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
	}

	// If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow (§5.3)
	// Append any new entries not already in the log (§5.3)
	if len(args.Entries) > 0 {
		// Directly overwrite from PrevLogIndex+1
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}

	//  If leaderCommit > commitIndex, set commitIndex =
	//  min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// Only advance commitIndex to entries that have been verified consistent
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
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

	// Start log replication to all followers
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateLog(i)
		}
	}
	rf.mux.Unlock()
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
	rf.heartbeatInterval = 100 * time.Millisecond // 10 heartbeats per second max
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
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rf.mux.Lock()

			// Apply all committed but not yet applied log entries
			// Process each entry one by one
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++

				// Get the entry to apply
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

				// Re-acquire lock for next iteration
				rf.mux.Lock()
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
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = -1
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
	ticker := time.NewTicker(10 * time.Millisecond)
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
			rf.changeToFollower(reply)
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
	for p := range rf.peers {
		if p != rf.me {
			go rf.replicateLog(p)
		}
	}
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
func (rf *Raft) changeToFollower(reply *AppendEntriesReply) {
	rf.currentTerm = reply.Term
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

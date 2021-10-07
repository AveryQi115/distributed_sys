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
	"log"
	"sync"
	"time"
)
import "sync/atomic"
import "distributed_sys/src/labrpc"
import "math/rand"

// import "bytes"
// import "../labgob"



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

type ElectionState int8

const(
	LEADER = ElectionState(0)
	CANDIDATE = ElectionState(1)
	FOLLOWER = ElectionState(2)
)

type LogEntry struct {
	Term		int
	Command		interface{}
}

//
// A Go object implementing a single Raft peer.
//
// function: maintain heartbeat and start election
//			 respond to applyEntries RPC
//			 respond to requestVote RPC
//			 leader must send heartbeats

// Raft is a server obeying raft protocol
// maintains : 	peers(ClientEnd arrays)
// 				persister
//				peer index
//				peer state(dead)
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
	currentTerm		int					// mark the current election term
	votedFor		int					// mark the leader server votedFor
	state			ElectionState		// the current state of the server
	lastHeartBeat	time.Time			// last time the server receives heart beat msg
	electionTimeout time.Duration

	// attributes relative to logs
	logEntries		[]LogEntry			// log entries Raft server maintain
	commitIndex		int					// index of highest log entry known to be committed
	lastApplied		int					// index of highest log entry applied to state machine

	// attributes used by leader
	nextIndex		[]int				// for each server,index of the next log entry to send
	matchIndex		[]int				// for each server,index of highest log entry known to be replicated
	stopSendingHeartBeat chan bool		// Done signal for the SendHeartBeat Process
	applyChan		chan ApplyMsg		// applyChan for sending committed command reply back to client
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state==LEADER
	rf.mu.Unlock()
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

type AppendEntriesArgs struct{
	Term			int
	LeaderID 		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct{
	Term 	int
	Success bool
}


// SendHeartBeat send empty AppendEntries messages
// happens along with an active leader
// 1.(concurrent) Done if the leader change the state
// 2.(concurrent) if the leader is outdated, call ConvertToFollower
// 3.(concurrent) wait for 100 millisecond, go on next cycle
// TODO: if the leader is outdated, the process will wait 100 millisecond and receive the signal after converting to follower
func (rf *Raft) SendHeartBeat(){
	rf.mu.Lock()
	me := rf.me
	peerCount := len(rf.peers)
	rf.mu.Unlock()

	for{
		select{
			case <- rf.stopSendingHeartBeat:
				return
			default:
				countLock := sync.Mutex{}
				count:=0
				i := 0
				rf.mu.Lock()
				currentTerm := rf.currentTerm
				commitIndex := rf.commitIndex
				entries := rf.logEntries
				matchIndex := rf.matchIndex
				rf.mu.Unlock()

				for i < peerCount{
					if i != me{
						go func(i int){
							args := AppendEntriesArgs{
								Term		: currentTerm,
								LeaderID	: me,
								PrevLogIndex: matchIndex[i],				//TODO:prevLogIndex
								PrevLogTerm	: entries[matchIndex[i]].Term,	//TODO:prevLogTerm
								Entries		: entries,
								LeaderCommit: commitIndex,

							}
							reply := AppendEntriesReply{}
							log.Printf("[raft.SendHeartBeat] raft server %v send heartBeat to server %v args:%v\n",me, i, args)

							ok := false
							ok=rf.sendAppendEntries(i,&args,&reply)

							log.Printf("[raft.SendHeartBeat] raft server %v receive heartBeat reply from server %v:%v\n",me,i,reply)

							// the leader is outdated
							if ok && !reply.Success && reply.Term > currentTerm{
								log.Printf("[raft.SendHeartBeat] raft server %v convert to follower\n",me)
								go rf.ConvertToFollower()
								return
							}

							if ok && reply.Success{
								countLock.Lock()
								count+=1
								countLock.Unlock()
							}
						}(i)
					}
					i += 1
				}

				time.Sleep(50*time.Millisecond)

				//TODO：论文里要求leader需要在没有得到大部分的yes reply之后转为follower state
				// 设计难点：统计yes个数，两种策略：yes小于半数，转follower；yes大于半数，继续
				// 设计策略1，yes大于半数继续：如果follower不响应，leader就卡住了，不会继续传heartbeat；server可能转candidate，增加term，leader转follower但是当前goroutine泄漏
				// 设计策略2，yes小于半数转follower：什么时候统计小于半数？如果follower不响应，需要等待一段时间，一段时间怎么设计？
				// 设计策略3： 不统计yes，对于没有reply的server，自然会转candidate，增加term，会导致leader转follower；但是浪费了很多资源给老leader发heartbeat
		}
	}
}

// ConvertToFollower convert the raft server to follower state
// happens once the leader is outdated or the candidate lose the election
// 1. change the state to follower
// 2. reset the election clock
// 3. if from leader, send the done signal for sending heartbeat
func (rf *Raft) ConvertToFollower(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[raft.ConvertToFollower] server %v convert to follower\n", rf.me)

	if rf.state==LEADER{
		rf.stopSendingHeartBeat <- true
	}
	rf.state = FOLLOWER
	rf.lastHeartBeat = time.Now()
}

// ConvertToLeader is to convert the raft server to leader state
// happens once win the election
// 1. change the state to leader(so the check election time out won't be effective)
// 2.(concurrent) send AppendEntries RPC to peers repeatedly during idle periods
// 3.(concurrent) send AppendEntries RPC to peers if nextIndex satisfied
// 4.(concurrent)(done by SendHeartBeat) if currentTerm is lower, convert to follower
func (rf *Raft) ConvertToLeader(){
	rf.mu.Lock()
	log.Printf("[Raft.ConvertToLeader] raft server %v convert to leader\n",rf.me)
	rf.state = LEADER
	Done := make(chan bool)
	rf.stopSendingHeartBeat = Done
	rf.nextIndex = make([]int,len(rf.peers),len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers),len(rf.peers))
	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = len(rf.logEntries)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	//TODO: reinitialize nextIndex[] and matchIndex[]

	go rf.SendHeartBeat()
	//TODO: AppendEntries according serveral index
}


// ConvertToCandidate is to convert the raft server to candidate state
// happens once the election timed out
// 1. change state to candidate
// 2. reset election clock
// 3. increase current term and vote for itself
// 4. send request vote to peers
// 5a. exit and continue checking election time out, start another term then
// 5b. convert to leader
func (rf *Raft) ConvertToCandidate(){
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.lastHeartBeat = time.Now()
	rf.currentTerm += 1
	log.Printf("[Raft.ConvertToCandidate] raft server %v update term: %v\n",rf.me, rf.currentTerm)
	rf.votedFor = rf.me

	peerLen := len(rf.peers)
	me := rf.me
	currentTerm := rf.currentTerm
	logEntries := rf.logEntries
	rf.mu.Unlock()

	countLock := sync.Mutex{}
	voteChannel := make(chan bool)
	count := 1
	i := 0
	for i < peerLen{
		if i == me{
			i += 1
			continue
		}

		go func(i int){
			args := RequestVoteArgs{
				Term			: currentTerm,
				CandidateId		: me,
				LastLogIndex	: len(logEntries)-1,						// not commitIndex nor lastApplied,but the largest index
				LastLogTerm		: logEntries[len(logEntries)-1].Term,
			}
			reply := RequestVoteReply{}
			log.Printf("[Raft.ConvertToCandidate] raft server %v send request to server %v for vote: %v\n",me, i, args)
			rf.sendRequestVote(i,&args,&reply)
			log.Printf("[Raft.ConvertToCandidate] raft server %v receive reply from server %v: %v\n",me, i, reply)
			if reply.VoteGranted{
				countLock.Lock()
				count += 1
				if count > peerLen/2{
					voteChannel <- true
				}
				countLock.Unlock()
			}
		}(i)
		i += 1
	}

	// Todo: 如果一直没有收到ok，这条goroutine不会return，check发起第二轮checkTimeout
	if ok := <-voteChannel;ok{
		go rf.ConvertToLeader()
		return
	}
}

func (rf *Raft) CheckElectionTimeout(){
	rf.mu.Lock()
	if rf.state != LEADER {
		log.Printf("[Raft.CheckElectionTimeout] raft server %v check for the election timeout\n",rf.me)
		if time.Now().Sub(rf.lastHeartBeat) >= rf.electionTimeout {
			log.Printf("[Raft.CheckElectionTimeout] raft server %v convert to candidate\n",rf.me)
			rf.mu.Unlock()
			go rf.ConvertToCandidate()
			return
		}
	}
	rf.mu.Unlock()
}


func (rf *Raft) Check(){
	rf.mu.Lock()
	electionTimeout := rf.electionTimeout
	rf.mu.Unlock()

	for true{
		time.Sleep(electionTimeout)
		go rf.CheckElectionTimeout()
	}
}

func MatchPrevLog(entries []LogEntry, prevLogIndex int, prevLogTerm int)bool{
	if prevLogIndex >= len(entries){
		return false
	}
	return entries[prevLogIndex].Term == prevLogTerm
}

func DeleteConflictLogEntries(entries []LogEntry, newEntries []LogEntry){

}

func AppendNewEntries(entries []LogEntry, newEntries []LogEntry){
	
}

// AppendEntries is the heartbeat msg or the appending entries msg
// 1. if the leader is outdated, return false
// 2. if prevLogIndex and prevLogTerm don't match, return false
// 3. delete conflict entries and append new entries
// 4. update commit index
// 5a. reset election clock
// 5b. if the server is behind the leader term, convert to follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	log.Printf("[raft.AppendEntries] server %v receive append entries call, arg = %v",rf.me,args)

	// false leader has lower term, return false
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// prevLogIndex and prevLogTerm don't match, return false
	if !MatchPrevLog(rf.logEntries,args.PrevLogIndex,args.PrevLogTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	DeleteConflictLogEntries(rf.logEntries,args.Entries)

	AppendNewEntries(rf.logEntries,args.Entries)

	if args.LeaderCommit > rf.commitIndex{
		if args.LeaderCommit < len(rf.logEntries)-1{
			rf.commitIndex = args.LeaderCommit
		}else {
			rf.commitIndex = len(rf.logEntries)-1
		}
	}

	// the term of current server is behind the leader term, convert to follower, reset timer
	currentTerm := rf.currentTerm
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		currentTerm = rf.currentTerm
		rf.mu.Unlock()

		go rf.ConvertToFollower()
	} else {
		rf.lastHeartBeat = time.Now()
		rf.mu.Unlock()
	}

	reply.Term = currentTerm
	reply.Success = true
	return
}

// sendAppendEntries call appendEntries of the given server
// return bool representing of success or not
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}


// RequestVote RPC handler.
// 1. if the leader is outdated return false
// 2. if the leader have already voted and votedFor != args.candidateId return false
// 3. grant vote if the candidate log is more or equally up-to-date
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("[raft.RequestVote.BeforeLock] server receive vote request from server %v args = %v\n",args.CandidateId, args)
	rf.mu.Lock()
	log.Printf("[raft.RequestVote] server %v receive vote request from server %v args = %v\n",rf.me, args.CandidateId, args)

	// vote并且立马更新currentTerm，convertToFollower
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		log.Printf("[raft.RequestVote] server %v vote for a higher term candidate %v,currentTerm =%v\n",rf.me,args.CandidateId,rf.currentTerm)
		log.Printf("[raft.RequestVote] server %v convert to follower \n",rf.me)
		rf.mu.Unlock()

		go rf.ConvertToFollower()
		return
	}

	if args.Term < rf.currentTerm{
		log.Printf("[raft.RequestVote] server %v refuse to vote for server %v cause leader term %v outdated.\n",rf.me,args.CandidateId,args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if rf.votedFor!=-1 && rf.votedFor != args.CandidateId{
		log.Printf("[raft.RequestVote] server %v refuse to vote for server %v cause already vote for %v.\n",rf.me,args.CandidateId,rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// grant vote if the candidate's log more up-to-date
	lastIndex := len(rf.logEntries)-1
	lastTerm := rf.logEntries[lastIndex].Term
	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex){
		rf.lastHeartBeat = time.Now()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	log.Printf("[raft.RequestVote] server %v refuse to vote for server %v cause last log(Index:%v,Term:%v) are more uptodate than args:(Index:%v,Term:%v).\n",rf.me,args.CandidateId,lastIndex,lastTerm,args.LastLogIndex,args.LastLogTerm)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	index := 0
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
// applyCh is for sending the ApplyMsg to the client
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1	// null
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logEntries = make([]LogEntry,0,5)
	rf.logEntries = append(rf.logEntries,LogEntry{0,nil})	// default value, not used, initial index = 1
	rf.applyChan = applyCh
	rf.lastHeartBeat = time.Now()	// so that every server is initialized with a different time
	rf.electionTimeout = time.Duration(100 + rand.Intn(200)) * time.Millisecond

	rf.state = FOLLOWER
	rf.mu.Unlock()

	go rf.Check()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

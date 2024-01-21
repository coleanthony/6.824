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
	"lab/src/labgob"
	"lab/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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
	UseSnapshot  bool
	Snapshot     []byte
}

//the machine state
const (
	Leader = iota
	Candidate
	Follower
)

//definition of log entry
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	//persistent state on all servers
	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//something else
	State int //状态
	//electionTimeout time.Duration //记录超时时间,选举的间隔时间不同 可以有效的防止选举失败
	electiontimer *time.Ticker  //每个节点中的计时器,判断选举是否超时,选举计时器
	applyCh       chan ApplyMsg //client从applych取日志
	cond          *sync.Cond    //sync.Cond可以用于等待和通知goroutine，等待发送applyentry通知
}

// example AppendEntries RPC arguments and reply structure.
type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term         int  //currentTerm, for leader to update itself
	Success      bool //true if follower contained entry matchingprevLogIndex and prevLogTerm
	NextTryIndex int  //if appendentry fail because of the mismatch of the term, decrease the nextindex and retry
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//SnapshotRPC
//Send the entire snapshot in a single InstallSnapshot RPC.
//Don't implement Figure 13's offset mechanism for splitting up the snapshot.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	//Offset          int
	//Done            bool
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm

	if rf.State == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

//注意，currentTerm, voteFor 和 logs 这三个变量一旦发生变化就一定要在被其他协程感知到之前（释放锁之前，发送 rpc 之前）持久化，这样才能保证原子性。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetRfState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentterm int
	var votefor int
	var log []LogEntry
	if d.Decode(&currentterm) != nil || d.Decode(&votefor) != nil || d.Decode(&log) != nil {
		//error
		fmt.Println("read persist data error")
	} else {
		rf.currentTerm = currentterm
		rf.votedFor = votefor
		rf.log = log
	}
}

//RequestVote RPC handler.
// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//if args.Term < rf.currentTerm || rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == rf.currentTerm {
	//	reply.Term = rf.currentTerm
	//	reply.VoteGranted = false
	//	return
	//}

	if args.Term < rf.currentTerm {
		// reject request with stale term number
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.BecomeFollower()
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.Isuptodate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//rf.chanGrantVote <- true
		rf.electiontimer.Reset(GetRamdomTimeout())
	}

	//
	//	if rf.votedFor == -1 {
	//		currentlogterm, currentlogindex := 0, -1
	//		currentlogindex = len(rf.log) - 1
	//		if currentlogindex >= 0 {
	//			currentlogterm = rf.log[currentlogindex].Term
	//		}
	//		if args.LastLogTerm < currentlogterm || args.LastLogIndex < currentlogindex {
	//			reply.Term = rf.currentTerm
	//			reply.VoteGranted = false
	//			return
	//		}
	//		rf.votedFor = args.CandidateId
	//		reply.VoteGranted = true
	//		reply.Term = rf.currentTerm

	//		rf.electiontimer.Reset(rf.electionTimeout)
	//	} else {
	//		//if rf.votedfor=candidateid
	//		reply.VoteGranted = false

	//		if rf.votedFor != args.CandidateId {
	//			return
	//		} else {
	//			rf.BecomeFollower()
	//		}
	//		rf.electiontimer.Reset(rf.electionTimeout)
	//	}
	//
}

func (rf *Raft) Isuptodate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.lastLogTerm(), rf.lastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

//AppendEntry Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	//1.返回假 如果领导人的任期小于接收者的当前任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.lastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		//如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者
		rf.currentTerm = args.Term
		rf.BecomeFollower()
		rf.votedFor = -1
	}

	//rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm
	rf.electiontimer.Reset(GetRamdomTimeout())

	//2.在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.NextTryIndex = rf.lastLogIndex() + 1
		return
	}

	//3.如果一个已经存在的条目和新条目（即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		//4.追加日志中尚未存在的任何新条目
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)

		reply.Success, reply.NextTryIndex = true, args.PrevLogIndex+len(args.Entries)

		if args.LeaderCommit > rf.commitIndex {
			lastlogIndex := rf.lastLogIndex()
			rf.commitIndex = min(args.LeaderCommit, lastlogIndex)
			rf.cond.Broadcast()
		}
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || args.Term != rf.currentTerm || rf.State != Leader {
		//fmt.Println("send append entries error")
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.BecomeFollower()
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	//>1/2 and leader apply
	if reply.Success {
		//如果成功：更新相应跟随者的 nextIndex 和 matchIndex
		if len(args.Entries) > 0 {
			//rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			//rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	} else {
		//因为日志不一致而失败，则 nextIndex 递减并重试
		rf.nextIndex[server] = reply.NextTryIndex
	}

	baseIndex := rf.log[0].Index
	//假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，则令 commitIndex = N
	for N := rf.lastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		countvotes := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				countvotes++
			}
		}
		if countvotes > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.cond.Broadcast()
			break
		}
	}

	return ok
}

func (rf *Raft) CreateSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("create snapshot")

	baseIndex, lastIndex := rf.log[0].Index, rf.lastLogIndex()
	if index <= baseIndex || lastIndex < index {
		fmt.Println("index is invalid")
		return
	}

	//trim log
	newlog := make([]LogEntry, 0)
	lastIncludedIndex, lastIncludedTerm := index, rf.log[index-baseIndex].Term
	newlog = append(newlog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newlog = append(newlog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newlog

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//lastIncludedIndex, lastIncludedTerm
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	data := w.Bytes()
	kvsnapshot := append(data, snapshot...)

	rf.persister.SaveStateAndSnapshot(rf.GetRfState(), kvsnapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//fmt.Printf("server[%d] install snapshot\n", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.BecomeFollower()
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.electiontimer.Reset(GetRamdomTimeout())

	if args.LastIncludedIndex > rf.commitIndex {
		newlog := make([]LogEntry, 0)
		lastIncludedIndex, lastIncludedTerm := args.LastIncludedIndex, args.LastIncludedTerm
		newlog = append(newlog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
				newlog = append(newlog, rf.log[i+1:]...)
				break
			}
		}
		rf.log = newlog

		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.GetRfState(), args.Data)

		//使用快照重置状态机（并加载快照的集群配置）
		applymsg := ApplyMsg{
			CommandValid: true,
			UseSnapshot:  true,
			Snapshot:     args.Data,
		}
		rf.applyCh <- applymsg
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//fmt.Printf("server[%d] send install snapshot\n", server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || args.Term != rf.currentTerm || rf.State != Leader {
		//can not installsnapshot
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.BecomeFollower()
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	return ok
}

func (rf *Raft) recoverFromSnapShot(snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("read persist snapshot error")
	}

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	//trim log
	newlog := make([]LogEntry, 0)
	newlog = append(newlog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newlog = append(newlog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newlog

	//使用快照重置状态机（并加载快照的集群配置）
	applymsg := ApplyMsg{
		CommandValid: true,
		UseSnapshot:  true,
		Snapshot:     snapshot,
	}
	rf.applyCh <- applymsg
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.State != Leader {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})

	return index, term, isLeader
}

func (rf *Raft) ApplyEntries() {
	//将logEntry应用到状态机
	for {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			//fmt.Println("wait")
			rf.cond.Wait()
		}
		rf.lastApplied++
		baseIndex := rf.log[0].Index
		applymsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-baseIndex].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applymsg
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electiontimer.Stop()
	//rf.heartbeattimer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) BecomeLeader() {
	//一旦成为领导人：发送空的附加日志（AppendEntries）RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以防止跟随者超时
	if rf.State == Leader {
		return
	}
	rf.State = Leader

	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	//fmt.Printf("id[%d].state[%v].term[%d]: 转换为Leader\n", rf.me, rf.State, rf.currentTerm)
	//rf.chanWinElect <- true

	rf.persist()
	go rf.LeaderHeartbeat()
}

func (rf *Raft) BecomeCandidate() {
	rf.State = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	//fmt.Printf("id[%d].state[%v].term[%d]: 转换为Candidate\n", rf.me, rf.State, rf.currentTerm)
}

func (rf *Raft) BecomeFollower() {
	if rf.State == Follower {
		return
	}
	rf.State = Follower
	rf.persist()
	//fmt.Printf("id[%d].state[%v].term[%d]: 转换为Follower\n", rf.me, rf.State, rf.currentTerm)
}

func (rf *Raft) StartElection() {
	//candidate startelection
	//fmt.Println("id", rf.me, " start election")
	rf.BecomeCandidate()
	defer rf.persist()
	votenum := 1
	for i := range rf.peers {
		if i == rf.me || rf.State != Candidate {
			continue
		}
		requestvoteargs := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}
		go func(i int) {
			//requestvote simulaneously
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, requestvoteargs, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term == rf.currentTerm && reply.VoteGranted && rf.State == Candidate {
					votenum++
					if votenum > len(rf.peers)/2 {
						rf.BecomeLeader()
					}
					rf.persist()
				} else if reply.Term > rf.currentTerm {
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.BecomeFollower()
					rf.persist()
				}
			}
		}(i)
	}
}

//start a ticker to start election
func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.electiontimer.C:
			//一段时间（election timeout）没收到其他节点的消息时，通过RequestVote RPC发起选举
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				break
			}
			if rf.State != Leader {
				//fmt.Printf("id[%d].state[%v].term[%d]: 选举计时器到期\n", rf.me, rf.State, rf.currentTerm)
				rf.StartElection()
			}
			rf.electiontimer.Reset(GetRamdomTimeout())
			rf.mu.Unlock()
		}

	}
	//fmt.Println("is killed")
}

//
//func (rf *Raft) Run() {
//	for {
//		switch rf.State {
//		case Follower:
//			select {
//			case <-rf.chanGrantVote:
//			case <-rf.chanHeartbeat:
//			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
//				rf.State = Candidate
//				rf.persist()
//			}
//		case Candidate:
//			rf.mu.Lock()
//			rf.currentTerm++
//			rf.votedFor = rf.me
//			rf.persist()
//			rf.mu.Unlock()
//			go rf.StartElection()
//
//			select {
//			case <-rf.chanHeartbeat:
//				rf.State = Follower
//			case <-rf.chanWinElect:
//			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
//			}
//		case Leader:
//
//		}
//	}
//}
//

func (rf *Raft) LeaderHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.State != Leader {
			rf.mu.Unlock()
			break
		}
		baseIndex := rf.log[0].Index
		snapshot := rf.persister.ReadSnapshot()

		//send empty heartbeat or logentry
		for i := range rf.peers {
			if i == rf.me || rf.State != Leader {
				continue
			}
			if rf.nextIndex[i] > baseIndex {
				prevlogindex := rf.nextIndex[i] - 1
				var prevlogterm int
				if prevlogindex >= baseIndex {
					prevlogterm = rf.log[prevlogindex-baseIndex].Term
				}
				var entries []LogEntry
				if rf.nextIndex[i] <= rf.lastLogIndex() {
					entries = rf.log[rf.nextIndex[i]-baseIndex:]
				}
				//if not, the entry is empty, it is a heartbeat
				appendentriesargs := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevlogindex,
					PrevLogTerm:  prevlogterm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				go rf.sendAppendEntry(i, appendentriesargs, &AppendEntriesReply{})

			} else {
				//<= baseindex, sendsnapshot
				installSnapshotArgs := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              snapshot,
				}

				go rf.sendInstallSnapshot(i, installSnapshotArgs, &InstallSnapshotReply{})

			}
		}
		rf.mu.Unlock()
		time.Sleep(GetStableHeartbeattime())
	}
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func GetRamdomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func GetStableHeartbeattime() time.Duration {
	return time.Duration(60) * time.Millisecond
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
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]LogEntry, 1),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		State:         Follower,
		applyCh:       applyCh,
		electiontimer: time.NewTicker(GetRamdomTimeout()),
	}
	rf.log[len(rf.log)-1].Term = 0
	rf.log[len(rf.log)-1].Index = 0

	rf.cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initalize from snapshot
	rf.recoverFromSnapShot(persister.ReadSnapshot())
	rf.persist()
	//// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyEntries()
	// fmt.Println("Start raft id:" + strconv.Itoa(me))
	return rf
}

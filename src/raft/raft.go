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
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)

type LogEntry struct {
	Term int
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
	applyCh chan ApplyMsg

	state int32
	CurrentTerm int
	VotedFor int32
	Log []*LogEntry

	commitIndex int
	lastApplied int32
	leaderId int

	electionTimeout int32 // milliseconds
	heartbeatTime time.Time

	nextIndexes []int

	logger *log.Logger
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, atomic.LoadInt32(&rf.state) == LEADER
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
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
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
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []*LogEntry
	if d.Decode(&CurrentTerm) != nil ||
	   d.Decode(&VotedFor) != nil ||
	   d.Decode(&Log) != nil  {
	  rf.logger.Fatal("读取持久化状态失败")
		return
	}

	rf.logger.Printf("读取持久化 currentTerm: %v, votedFor: %v, logs:%s", CurrentTerm, VotedFor, logs2Str(Log))
	rf.CurrentTerm = CurrentTerm
	rf.setVotedFor(VotedFor)
	rf.Log = Log

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
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()


	reply.Term = rf.CurrentTerm
	reply.VoteGranted = true

	// 竞选者的Term是+1过的，肯定要原来至少和我同一个Term才有资格当我的leader
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		rf.logger.Printf("Id(%d)想让我给他投票，我不给, CurrentTerm:%d, CandidateTerm:%d", args.CandidateId, rf.CurrentTerm, args.Term)
		return
	}

	if args.LastLogIndex < len(rf.Log) {
		reply.VoteGranted = false
		rf.logger.Printf("Id(%d)想让我给他投票，我不给, 他记录的日志没我多", args.CandidateId)
		return
	}

	if len(rf.Log) > 0 && args.LastLogTerm < rf.Log[len(rf.Log) - 1].Term {
		reply.VoteGranted = false
		rf.logger.Printf("Id(%d)想让我给他投票，我不给, 他的日志比我的旧", args.CandidateId)
		return
	}

	if rf.VotedFor != -1 && rf.getVotedFor() != args.CandidateId{
		reply.VoteGranted = false
		rf.logger.Printf("Id(%d)想让我给他投票，我不给，我给了Id(%d)", args.CandidateId, rf.VotedFor)
		return
	}
	// 只有给他投票才更新心跳时间，否则别人一直请求投票，我没有机会
	rf.heartbeatTime = time.Now()

	rf.setVotedFor(args.CandidateId)
	// 在别人拉票的时候，把最新的Term记录下来
	rf.CurrentTerm = args.Term
	rf.changeState(FOLLOWER)
	rf.logger.Printf("Id(%d)想让我给他投票，我给了", args.CandidateId)

	rf.persist()
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

// RPCHandler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("领导Id(%d)发来命令, Args: entries:%+v, preLogIndex: %v, leaderCommit: %v", args.LeaderId, logs2Str(args.Entries), args.PrevLogIndex, args.LeaderCommit)
	reply.Success = true

	// (如果我正在竞选，那么我的currentTerm是加1过的 || 如果我投票给了一个竞选者，那么我的currentTerm会和那个竞选者的一样)，所以此时应该减一之后再判断
	if args.Term < rf.CurrentTerm && !((rf.state == CANDIDATE || rf.VotedFor != -1) && args.Term == rf.CurrentTerm - 1){
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.logger.Printf("拒绝这个领导的命令(Term:%d)，跟不上我的脚步(currentTerm:%d)", args.Term, rf.CurrentTerm)
		return
	}

	rf.CurrentTerm = args.Term

	// 验证entries
	if args.PrevLogIndex > len(rf.Log) {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ConflictIndex = len(rf.Log)
		if reply.ConflictIndex > 0 {
			reply.ConflictTerm = rf.Log[reply.ConflictIndex - 1].Term
		}
		rf.logger.Printf("拒绝添加Entries指令, 日志不连续, PrevLogIndex:%d > len(rf.log):%d", args.PrevLogIndex, len(rf.Log))
		return
	}

  if args.PrevLogIndex != 0 && rf.Log[args.PrevLogIndex-1].Term > args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ConflictIndex = args.PrevLogIndex
		if reply.ConflictIndex > 0 {
			reply.ConflictTerm = rf.Log[reply.ConflictIndex - 1].Term
		}
		rf.logger.Printf("拒绝添加Entries指令,  与我的记录中的Term不匹配，他的比我的旧, rf.log[args.PrevLogIndex-1].term:%d > args.PrevLogTerm:%d", rf.Log[args.PrevLogIndex-1].Term, args.PrevLogTerm)
		return
	}

	// 只有接收请求才更新心跳，避免一直拒绝但却不竞选新的
	rf.heartbeatTime = time.Now()
	rf.CurrentTerm = args.Term
	rf.changeState(FOLLOWER)
	rf.leaderId = args.LeaderId
	rf.setVotedFor(-1)

	if args.PrevLogIndex == 0 {
		rf.Log = args.Entries
	} else if len(args.Entries) == 0 {
		// 心跳

	} else {
		rf.Log = append(rf.Log[:args.PrevLogIndex], args.Entries...)
	}

	if rf.commitIndex != args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.Log) {
			rf.commitIndex = len(rf.Log)
		}
	}

	rf.logger.Printf("当前状态: logs:%s, commitIndex:%v, leaderId:%v", logs2Str(rf.Log), rf.commitIndex, rf.leaderId)

	rf.applyCommited(rf.Log[:rf.commitIndex], rf.commitIndex)

	rf.persist()
}

func (rf *Raft) applyCommited(commitedLogs []*LogEntry, commitIndex int) {
	for i:=0; i < len(commitedLogs); i++ {
		rf.applyCh <- ApplyMsg{true, commitedLogs[i].Command, i+1, false, nil, -1, -1}
	}
	atomic.StoreInt32(&rf.lastApplied, int32(commitIndex))
}

func logs2Str(slice []*LogEntry) string {
	const threshold = 10
	result := "["
	if len(slice) <= threshold {
		for _,v := range slice {
			result += fmt.Sprintf("%+v,", v)
		}
	} else {
		result += "..."
		for i:=len(slice) - threshold + 1; i < len(slice); i++ {
			result += fmt.Sprintf("%+v,", slice[i])
		}
	}
	return result + fmt.Sprintf("(%d items)]", len(slice))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.callRpc("Raft.AppendEntries", server, args, reply)
}

func (rf *Raft) callRpc(method string, server int, args interface{}, reply interface{}) bool {
	var wg = sync.WaitGroup{}
	wg.Add(1)
	var isOk int32 = 0
	go func(){
		var isFinish int32 = 0
		go func(){
			rf.sleep(0.95)
			if atomic.LoadInt32(&isFinish) == 1 {
				return
			}
			if atomic.CompareAndSwapInt32(&isFinish, 0, 1) {
				wg.Done()
			}
		}()
		ok := rf.peers[server].Call(method, args, reply)
		if atomic.LoadInt32(&isFinish) == 1 {
			return
		}
		if atomic.CompareAndSwapInt32(&isFinish, 0, 1) {
			if ok {
				atomic.StoreInt32(&isOk, 1)
			}
			wg.Done()
		}
	}()
	wg.Wait()
	return atomic.LoadInt32(&isOk) == 1
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
	return rf.callRpc("Raft.RequestVote", server, args, reply)
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

	index := len(rf.Log) + 1
	term := rf.CurrentTerm
	isLeader := atomic.LoadInt32(&rf.state) == LEADER

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	rf.logger.Println("Start Agreement!!!")
	rf.Log = append(rf.Log, &LogEntry{term, command})

	// go rf.sendAppendEntriesToAll() 直接等下一轮心跳，可以让多个Agreement合并一起发
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 让领导者少睡一会，减少竞选的概率
		if atomic.LoadInt32(&rf.state) == LEADER {
			rf.sleep(0.5)
		} else {
			rf.sleep(1)
		}
		if rf.killed() {
			rf.logger.Print("我死了")
			return
		}

		rf.mu.Lock()
		rf.logger.Println("tick!")
		rf.logger.Printf("当前状态: logs:%s, commitIndex:%v, leaderId:%v", logs2Str(rf.Log), rf.commitIndex, rf.leaderId)

		if rf.VotedFor != -1 {
			rf.logger.Print("正在进行选举，选举超时了")

			rf.setVotedFor(-1)
			// 选举时会将currentTerm + 1
			rf.CurrentTerm -= 1
			rf.changeState(FOLLOWER)
		}

		if rf.state == LEADER {
			// 我是领导者，发送心跳
			if rf.sendAppendEntriesToAll() <= 1{
				rf.logger.Print("所有心跳发送失败，我断网了")
				rf.changeState(FOLLOWER)
			}
			rf.mu.Unlock()
		} else {
			// 不是领导者，就看看领导者是不是还活着
			if time.Since(rf.heartbeatTime) > time.Duration(time.Millisecond * time.Duration(rf.electionTimeout)){
				rf.logger.Print("心跳时间超时，我直接竞选！")
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		}

	}
}


// 返回成功的数量 是否大部分失败（2 * count >= total）
func (rf *Raft) sendAppendEntriesToAll() int {

	var successCount int32 = 1 // 先把自己算上
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.doSendAppendEntries(i, &wg, &successCount)
	}
	wg.Wait()

	if int(successCount) * 2 >= len(rf.peers) {
		if rf.commitIndex < len(rf.Log) {
			rf.logger.Println("大部分人接受了Agreement，进行提交")
			rf.commitIndex = len(rf.Log)
			rf.persist()
			go rf.applyCommited(rf.Log[:rf.commitIndex], rf.commitIndex)
			rf.sendAppendEntriesToAll()
		}
	} else {
		rf.logger.Println("Agreement没有被大部分人接受")
	}
	return int(atomic.LoadInt32(&successCount))
}

func(rf *Raft) doSendAppendEntries(server int, wg *sync.WaitGroup, successCount *int32) {
	defer wg.Done()

	preLogIndex := rf.nextIndexes[server] - 1

	prevLogTerm := 0
	if preLogIndex > 0 {
		prevLogTerm = rf.Log[preLogIndex-1].Term
	}

	var entries  []*LogEntry
	if preLogIndex < len(rf.Log) {
		entries = rf.Log[preLogIndex:]
	}

	appendEntriesArgs := &AppendEntriesArgs{rf.CurrentTerm, rf.me, preLogIndex, prevLogTerm, entries, rf.commitIndex}

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, appendEntriesArgs, reply)
	if !ok {
		rf.logger.Printf("发送指令失败！Id(%d)", server)
	} else {
		if !reply.Success {
			if reply.Term > rf.CurrentTerm {
				rf.logger.Printf("Id(%d)的Term:%d比我的%d新，我直接退位", server, reply.Term, rf.CurrentTerm)
				rf.changeState(FOLLOWER)
			} else {

				rf.nextIndexes[server] = reply.ConflictIndex
				if rf.nextIndexes[server] > len(rf.Log) {
					rf.logger.Printf("Id(%d)的日志记录比我的多...", server)
					rf.nextIndexes[server] = len(rf.Log)
				}
				for rf.nextIndexes[server] > 1 && rf.Log[rf.nextIndexes[server] - 1].Term < reply.ConflictTerm {
					rf.nextIndexes[server] -= 1
				}
				rf.logger.Printf("Id(%d)竟然拒绝了我的添加指令, 降低他的nextIndex(%d)后重试", server, rf.nextIndexes[server])
				// rf.doSendAppendEntries(server, wg, successCount)
			}
		} else {
			atomic.AddInt32(successCount, 1)
			rf.nextIndexes[server] += len(entries)
		}
	}
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.CurrentTerm += 1
	electionTerm := rf.CurrentTerm
	rf.changeState(CANDIDATE)
	var votes int64 = 1 // 直接投一票给自己
	rf.setVotedFor(rf.me)

	requestVoteArgs := &RequestVoteArgs{
		electionTerm,
		rf.me,
		len(rf.Log),
		0,
	}
	if requestVoteArgs.LastLogIndex > 0 {
		requestVoteArgs.LastLogTerm = rf.Log[requestVoteArgs.LastLogIndex-1].Term
	}

	rf.persist()
	rf.mu.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers)-1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int){
			reply := RequestVoteReply{}
			isOk := rf.sendRequestVote(i,	requestVoteArgs, &reply)
			if !isOk {
				rf.logger.Printf("请求投票超时，Id(%d)", i)
			} else if reply.VoteGranted {
				rf.logger.Printf("拉票成功！Id(%d)", i)
				atomic.AddInt64(&votes, 1)
			} else if reply.Term > electionTerm {
					rf.changeState(FOLLOWER)
					rf.setVotedFor(-1)
					rf.logger.Printf("我落伍了，放弃竞选Id(%d)", i)
			} else {
				rf.logger.Printf("他票给别人了Id(%d)", i)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setVotedFor(-1)

	if rf.state == FOLLOWER {
		rf.logger.Printf("有人先当选了 : ) Id:%d", rf.leaderId)
		return
	}
	if votes == 1 {
		rf.logger.Print("只有我自己投票给了自己，我断网了")
	}
	rf.logger.Printf("竞选得到了%d张票，投票率%f", votes, float64(votes) / float64(len(rf.peers)))
	if votes * 2 > int64(len(rf.peers)) {
		rf.logger.Println("我当选了")
		rf.changeState(LEADER)
		// 初始化nextIndexes
		rf.nextIndexes = make([]int, len(rf.peers))
		for i := range rf.nextIndexes {
			rf.nextIndexes[i] = len(rf.Log) + 1
		}

		rf.sendAppendEntriesToAll()
	} else {
		// 没人当选才重新随机选举时间，毕竟如果有人当选，就尽量保持当前状态
		rf.randomizeElectionTimeout()
		// 还原Term，否则断网重连后他就变成Term最大的了
		rf.CurrentTerm = electionTerm - 1
	}
	rf.persist()
}

func (rf *Raft) changeState(s int32) {
	atomic.StoreInt32(&rf.state, s)
	str := "f"
	if s == CANDIDATE {
		str = "c"
	} else if s == LEADER {
		str = "l"
	}
	rf.logger.SetPrefix(fmt.Sprintf("%d(%s):\t", rf.me, str))
	rf.logger.Print("我不一样了")
}

func (rf *Raft) sleep(factor float32) {
	t := atomic.LoadInt32(&rf.electionTimeout) - rand.Int31n(100)
	time.Sleep(time.Millisecond * time.Duration(factor * float32(t)))
}

func (rf *Raft) randomizeElectionTimeout() {
	atomic.StoreInt32(&rf.electionTimeout, int32(250 + rand.Int() % 100))
}

func (rf *Raft) setVotedFor(i int) {
	atomic.StoreInt32(&rf.VotedFor, int32(i))
}

func (rf *Raft) getVotedFor() int {
	return int(atomic.LoadInt32(&rf.VotedFor))
}

const enableLog = true

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

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = log.New(os.Stdout, fmt.Sprintf("%d: ", me), log.Lshortfile|log.Ldate|log.Ltime)
	if !enableLog {
		rf.logger.SetOutput(&BlackHoleWriter{})
	}
	rf.changeState(FOLLOWER)
	rf.setVotedFor(-1)
	rf.randomizeElectionTimeout()
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.nextIndexes = make([]int, len(rf.peers))
	for i := range rf.nextIndexes {
		rf.nextIndexes[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

type BlackHoleWriter struct {}
func (w *BlackHoleWriter) Write(b []byte) (n int, err error) {
	return len(b), nil
}

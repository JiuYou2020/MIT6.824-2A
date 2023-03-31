package raft

//
// 这是Raft必须向服务（或测试者）公开的API大纲。有关每个函数的详细信息，请参见下面的注释。
//
// rf = Make(...)
// 创建一个新的Raft服务器。
// rf.Start（command interface{}）（index，term，isleader）
// 开始就新的日志条目达成一致意见
// rf.GetState()（term，isLeader）
// 询问Raft其当前任期以及其是否认为自己是领导者
// ApplyMsg
// 每次将新条目提交到日志中时，每个Raftraft节点都应将ApplyMsg发送到同一服务器中的服务（或测试者）。

import (
	"../labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// 每个Raftraft节点意识到连续的日志条目已提交时，应通过传递给Make（）的applyCh将ApplyMsg发送到同一服务器上的服务（或测试者）。将CommandValid设置为true，以表明ApplyMsg包含一个新提交的日志条目。
//
// 在实验三中，您将希望在applyCh上发送其他类型的消息（例如快照）；在那时，您可以向ApplyMsg添加字段，但对于这些其他用途，请将CommandValid设置为false。
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//节点状态
type Status int

//投票状态
type VoteStatus int

//全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

//raft节点类型：跟随者，竞选者，领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// 一个实现单个Raftraft节点的Go对象。
//
type Raft struct {
	mu        sync.Mutex          // 锁，用于保护共享访问此对等体状态
	peers     []*labrpc.ClientEnd // 所有raft节点的RPC端点
	persister *Persister          // 用于保存此对等体持久化状态的对象
	me        int                 // 此raft节点在peers[]中的索引
	dead      int32               // 由Kill（）设置

	// 在这里添加您的数据（2A、2B、2C）。
	// 参考论文图2，描述Raft服务器必须维护的状态。
	currentTerm   int           //当前任期
	voteFor       int           //当前任期把票投给了谁
	logs          []LogEntry    //日志条目，每个条目包含了命令，任期等信息
	commitIndex   int           //已提交的最大日志条目索引
	lastApplied   int           //最后应用到状态机的日志条目索引
	nextIndex     []int         // nextIndex是一个数组，它记录了每个Follower节点下一个要发送给它的日志条目的索引。
	matchIndex    []int         // 各个节点已知的最大匹配日志条目索引
	status        Status        //当前raft节点角色
	electionTimer *time.Timer   // 选举超时定时器
	applyChan     chan ApplyMsg //raft节点通过这个存取日志
}

// 获取当前节点最后一条日志的索引值
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

// 获取当前节点最后一条日志的任期号
func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex < 0 {
		return 0
	}
	return rf.logs[lastLogIndex].Term
}

// 重置选举计时器
func (rf *Raft) resetElectionTimer() {
	// 取消之前设置的定时器
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	// 随机生成一个时间间隔，用于等待其他节点的投票响应
	randTimeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	// 设置一个新的定时器
	rf.electionTimer = time.NewTimer(randTimeout)
}

// 返回 currentTerm 和该服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

//
// 将Raft的持久状态保存到稳定存储中，在崩溃和重启后可以检索。
// 参见论文图2，了解应该保留哪些内容的说明。
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
// 恢复先前持久化的状态
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
//示例RequestVote RPC参数结构。字段名称必须以大写字母开头！
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //自己当前的任期号
	CandidateId  int //自己的ID
	LastLogIndex int //自己最后一个日志号
	LastLogTerm  int //自己最后一个日志的任期
}

//
//以大写字母开头
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //自己当前任期号
	VoteGranted bool //自己会不会投票给这个candidate
}

//追加日志RPC Request
type AppendEntriesArgs struct {
	Term         int        //自己当前的任期号
	LeaderId     int        //leader(也就是自己)的ID,告诉follower自己是谁
	PrevLogIndex int        //前一个日志的日志号		用于进行一致性检查
	PrevLogTerm  int        //前一个日志的任期号		用于进行一致性检查,只有这两个都与follower中的相同,follower才会认为日志是一致的
	Entries      []LogEntry //当前日志体,也就是命令内容
	LeaderCommit int        //leader的已提交日志号
}

//追加日志RPC Response
type AppendEntriesReply struct {
	Term          int  // 自己当前任期号
	Success       bool //如果follower包括前一个日志,则返回true
	ConflictIndex int
}

//
// RequestVote RPC处理程序示例。
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("节点", rf.me, "收到投票，节点", rf.me, "当前任期为", rf.currentTerm, "当前状态为", rf.status, "请求节点任期为：", args.Term, "请求节点ID为", args.CandidateId)
	// 如果对方节点的 term 比当前节点大，则更新当前节点的 term，并变成 follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.voteFor = -1
	}

	reply.Term = rf.currentTerm

	// 如果对方节点的 term 比当前节点小，则直接拒绝请求
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// 如果当前节点已经投过票，则直接拒绝请求
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		return

	}

	// 如果对方节点的日志比当前节点的日志新，则投票给对方节点
	if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		return

	}

	reply.VoteGranted = false
	return

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("节点", rf.me, "准备添加日志", "节点", rf.me, "当前任期为", rf.currentTerm, "当前状态为", rf.status, "请求节点任期为：", args.Term, "请求节点ID为", args.LeaderId)
	// 如果对方节点的 term 比当前节点大，则更新当前节点的 term，并变成 follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.voteFor = -1
	}

	reply.Term = rf.currentTerm

	// 如果对方节点的 term 比当前节点小，则直接拒绝请求
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 如果日志不匹配，则在本地删除非共同部分的日志条目
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictIndex = rf.getLastIndexOfTerm(args.PrevLogTerm)
		reply.Success = false
		return
	}

	// 添加新的日志条目
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index >= len(rf.logs) {
			rf.logs = append(rf.logs, make([]LogEntry, index-len(rf.logs)+1)...)
		}
		if rf.logs[index].Term != entry.Term {
			rf.logs[index] = entry
		}
	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		//取args.LeaderCommit , rf.getLastLogIndex()较小值,不使用min函数
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastLogIndex()
		}
	}
	fmt.Println("节点", rf.me, "添加日志成功", "节点", rf.me, "当前任期为", rf.currentTerm, "当前状态为", rf.status, "请求节点任期为：", args.Term, "请求节点ID为", args.LeaderId)
	// 设置响应结果
	reply.Success = true
	reply.ConflictIndex = -1
}

//实现getastIndexOfTerm函数
func (rf *Raft) getLastIndexOfTerm(term int) int {
	for i := rf.getLastLogIndex(); i >= 0; i-- {
		if rf.logs[i].Term == term {
			return i
		}
	}
	return -1
}

// 发送RequestVote RPC到服务器的示例代码。
// server是rf.peers[]中目标服务器的索引。
// 期望将RPC参数传递给args。
// 使用RPC响应填充*reply，因此调用者应传递&reply。
// 传递给Call()的args和reply的类型必须与处理程序函数中声明的参数类型相同（包括它们是否为指针）。
// labrpc软件包模拟了一个不稳定的网络，其中服务器可能无法访问，
// 请求和回复可能会丢失。Call()发送请求并等待回复。
// 如果在超时时间内收到回复，则Call()返回true；否则Call()返回false。因此，Call()可能需要一段时间才能返回。
// 返回false可能由死亡的服务器、无法访问的活动服务器、丢失的请求或回复引起。
// Call()保证会返回（可能有延迟），除非服务器端的处理程序函数没有返回。因此，在Call()周围没有必要实现自己的超时。
// 查看../labrpc/labrpc.go中的注释，以获取更多详细信息。
// 如果您发现RPC无法正常工作，请检查是否对通过RPC传输的结构体中的所有字段名称都使用了大写字母，并且调用方使用&而不是结构本身来传递响应结构体的地址。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println("节点", rf.me, "向节点", server, "请求投票，请求节点当前任期是", args.Term, "ID为", args.CandidateId, "自己最后一个日志号为", args.LastLogIndex, "自己最后一个任期为", args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Println("节点", rf.me, "向节点", server, "请求追加日志，追加节点当前任期为", args.Term, "ID为", args.LeaderId, "自己最后一个日志号为", args.PrevLogIndex, "自己最后一个任期为", args.PrevLogTerm, "已提交的日志：", args.LeaderCommit, "日志长度：", len(args.Entries))
	// 发送请求
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return true
	}
	return false
}

//
// 使用Raft的服务（例如k/v服务器）希望在下一条要附加到Raft日志中的命令上开始达成共识。如果该服务器不是领导者，则返回false。否则，开始协商并立即返回。不能保证此命令将被提交到Raft日志中，因为领导者可能会失败或丢失选举。即使Raft实例已被关闭，该函数也应该优雅地返回。
// 第一个返回值是该命令将出现的索引，如果它被提交了。第二个返回值是当前的任期。第三个返回值为true，如果此服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	fmt.Println("节点", rf.me, "Start", rf.me, rf.status)
	// 如果不是领导者，则返回false
	if rf.status != Leader {
		return index, term, false
	}

	// 如果是领导者，则将命令添加到日志中
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 获取当前任期
	term = rf.currentTerm

	// 获取当前日志的最后一条日志的索引
	index = rf.getLastLogIndex() + 1

	// 将命令添加到日志中
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})

	// 将日志持久化到磁盘
	rf.persist()

	// 将日志发送给其他服务器
	fmt.Println("节点", rf.me, "Start", rf.me, rf.status, "broadcastAppendEntries")
	rf.broadcastAppendEntries()

	return index, term, isLeader
}

//
// 测试程序在每个测试之后不会停止Raft创建的goroutine，但它确实调用了Kill()方法。您的代码可以使用killed()检查是否已调用Kill()。使用原子操作可以避免使用锁。
// 问题是长时间运行的goroutine会使用内存，并可能消耗CPU时间，从而导致后续测试失败并生成混乱的调试输出。任何具有长时间运行循环的goroutine都应该调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// 服务或测试程序想要创建一个Raft服务器。所有Raft服务器（包括此服务器）的端口都在peers[]中。该服务器的端口是peers[me]。所有服务器的peers[]数组具有相同的顺序。
// persister是该服务器保存其持久状态的地方，如果有的话，最初还保存着最近保存的状态。applyCh是一个通道，测试程序或服务希望Raft将ApplyMsg消息发送到该通道。
// Make()必须快速返回，因此应为任何长时间运行的工作启动goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 1)
	//logs初始化为一个空的日志，是因为在一开始启动时，Raft集群中的所有节点都是follower角色，还没有进行过日志条目的复制。此时，如果leader向follower发送AppendEntries消息来同步日志，则 PrevLogIndex 为 -1 是无效的。
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.status = Follower
	// 初始化选举超时定时器为 150ms - 300ms 的随机值
	randomTime := time.Duration(rand.Intn(150) + 150)
	rf.electionTimer = time.NewTimer(randomTime * time.Millisecond)
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//建立选举机制,因为需要快速返回，为此启用线程
	go func() {
		for rf.killed() == false {
			fmt.Println("节点", rf.me, "当前状态：", rf.status, "当前任期：", rf.currentTerm, "投票对象：", rf.voteFor)
			switch rf.status {
			case Follower:
				select {
				case <-rf.electionTimer.C:
					fmt.Println("节点", rf.me, "选举超时")
					rf.mu.Lock()
					// 超时后切换为候选人状态
					rf.status = Candidate
					rf.mu.Unlock()
				}
			case Candidate:
				fmt.Println("节点", rf.me, "成为候选人")
				rf.mu.Lock()
				// 成为候选人后增加任期号，重置选举计时器和投票记录，并发起自己的投票请求
				rf.currentTerm += 1
				rf.voteFor = rf.me
				votesReceived := 1
				rf.resetElectionTimer()
				rf.mu.Unlock()
				// 向其他服务器发送投票请求
				for i := range rf.peers {
					if i != rf.me && rf.killed() == false {
						fmt.Println("节点", rf.me, "向其他服务器发送投票请求")
						go func(server int) {
							args := &RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateId:  rf.me,
								LastLogIndex: rf.getLastLogIndex(),
								LastLogTerm:  rf.getLastLogTerm(),
							}
							var reply RequestVoteReply
							ok := rf.sendRequestVote(server, args, &reply)
							fmt.Println("节点", rf.me, "收到投票回复：", ok, "{任期 是否同意投票}", reply)
							if ok {
								rf.mu.Lock()
								defer rf.mu.Unlock()
								// 如果收到了更高任期的回复，则转为 Follower 状态并更新当前任期
								if reply.Term > rf.currentTerm {
									rf.currentTerm = reply.Term
									rf.status = Follower
									rf.voteFor = -1
									return
								}
								//fmt.Println("节点",rf.me,"收到投票回复：", reply.VoteGranted)
								// 如果在当前任期的选举中收到了多数派的投票，则转为 Leader 状态
								if reply.VoteGranted {
									votesReceived += 1
									if votesReceived > len(rf.peers)/2 && rf.status == Candidate {
										fmt.Println("节点", rf.me, "收到投票", votesReceived, "张，成为领导人")
										rf.status = Leader
										for i := range rf.peers {
											rf.nextIndex[i] = len(rf.logs)
											rf.matchIndex[i] = 0
										}
										return
									}
								}
							}
						}(i)
					}
				}
				time.Sleep(10 * time.Millisecond) // 减少竞争，避免出现死锁
			case Leader:
				//在Raft协议中，一旦一个节点成为leader，它需要周期性地向其它节点发送心跳消息（AppendEntries RPC）以保持自己的领导地位。这个周期性发送心跳消息的时间间隔称为“心跳间隔”（heartbeat interval），是一个固定的时间段。
				fmt.Println("节点", rf.me, "成为领导人")
				rf.broadcastAppendEntries()
				time.Sleep(HeartBeatTimeout)
			}

		}
	}()

	return rf
}

// 如果自己当选leader，发送心跳消息或附加日志的请求
func (rf *Raft) broadcastAppendEntries() {
	fmt.Println("节点", rf.me, "发送心跳消息或附加日志的请求")
	for i := range rf.peers {
		if i != rf.me && rf.killed() == false {
			go func(server int) {
				// 创建 AppendEntriesArgs 结构体
				fmt.Println("节点", rf.me, "前一个日志：", rf.nextIndex[server]-1, rf.logs[rf.nextIndex[server]-1].Term)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				var reply AppendEntriesReply
				fmt.Println("节点", rf.me, "向节点", i, "发送心跳消息或附加日志的请求")
				ok := rf.sendAppendEntries(server, args, &reply)
				fmt.Println("节点", rf.me, "收到节点", i, "心跳消息或附加日志的回复", ok, "{任期 成功与否 冲突地址}", reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Success {
						if len(args.Entries) > 0 {
							// 如果发送了日志条目，则更新 nextIndex 和 matchIndex 数组
							rf.nextIndex[server] = len(args.Entries)
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						} else {
							// 如果只发送了心跳消息，则只更新 nextIndex 数组
							rf.nextIndex[server] = args.PrevLogIndex + 1
						}
						fmt.Println("节点", rf.me, "更新 nextIndex 和 matchIndex 数组", rf.nextIndex, rf.matchIndex)
						// 检查是否有新的日志条目被提交
						for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
							if rf.logs[N].Term == rf.currentTerm {
								count := 1
								for i := range rf.peers {
									if i != rf.me && rf.matchIndex[i] >= N {
										count += 1
									}
								}
								if count > len(rf.peers)/2 {
									// 大多数节点都已经提交该日志条目，可以将其提交并告知客户端
									rf.commitIndex = N
									applyMsg := ApplyMsg{
										CommandValid: true,
										Command:      rf.logs[N].Command,
										CommandIndex: N,
									}
									rf.applyChan <- applyMsg
								}
							}
						}
					} else {
						fmt.Println("节点", rf.me, "如果对方的日志没有当前节点新，则递减 nextIndex 并重试", reply.ConflictIndex)
						// 如果对方的日志没有当前节点新，则递减 nextIndex 并重试
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.status = Follower
							rf.voteFor = -1
							return
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}
				}

			}(i)
		}
	}
}

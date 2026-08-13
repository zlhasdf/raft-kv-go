// Raft 核心实现 —— 共识模块。

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// DebugCM = 1（当前值）→ 日志开启，代码里所有 cm.dlog(...) 会打印，
// 并且每行前面带上服务器 ID（如 [0] ...），方便区分多节点输出。
// 改成 0 → 所有 dlog 调用直接跳过，日志关闭。
const DebugCM = 1

// CommitEntry 是 Raft 通过提交通道上报的数据。每一条提交条目都通知客户端：
// 某个命令已达成共识，可以应用到客户端的状态机。
type CommitEntry struct {
	// Index 是客户端命令被提交时的日志索引。
	Index int

	// Command 是正在被提交的客户端命令。
	Command any

	// Term 是客户端命令被提交时的 Raft 任期。
	Term int
}

type LogEntry struct {
	Term    int
	Command any
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// ConsensusModule（CM）实现了 Raft 共识中的单个节点。
type ConsensusModule struct {
	// mu 保护对 CM 的并发访问。
	mu sync.Mutex

	// id 是此 CM 的服务器 ID。
	id int

	// peerIds 列出集群中此节点的对等节点 ID。
	peerIds []int

	// server 是包含此 CM 的服务器，用于向对等节点发起 RPC 调用。
	server *Server

	// commitChan 是此 CM 用来上报已提交日志条目的通道，由客户端在构造时传入。
	commitChan chan<- CommitEntry

	// newCommitReadyChan 是一个内部通知通道，提交新日志条目的 goroutine 通过它
	// 通知这些条目可能已可以发送到 commitChan。
	newCommitReadyChan chan struct{}
	// 所有服务器上的持久化 Raft 状态
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 所有服务器上的易失性（非持久化）Raft 状态
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// leader 上的易失性 Raft 状态
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule 使用给定的 ID、对等节点 ID 列表和 server 创建一个新的 CM。
// ready 通道向 CM 发出信号：所有对等节点都已连接，可以安全启动其状态机。
// commitChan 将用于让 CM 发送已被 Raft 集群提交的日志条目。
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	go func() {
		// 在 ready 被触发之前，CM 保持静默；之后它会启动一次选举倒计时。
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	go cm.commitChanSender()
	return cm
}

// Report 报告此 CM 的状态。
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit 向 CM 提交一条新命令。该函数不会阻塞；客户端通过读取构造函数中传入的
// 提交通道来获知新的已提交条目。仅当此 CM 是 leader 时返回 true——此时命令被
// 接受。如果返回 false，客户端需要找到另一个 CM 来提交这条命令。
func (cm *ConsensusModule) Submit(command any) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.dlog("... log=%v", cm.log)
		return true
	}
	return false
}

// Stop 停止此 CM 并清理其状态。该方法会快速返回，但所有 goroutine 完全退出
// 可能需要一点时间（最多约一个选举超时）。
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("Stopping")
	close(cm.newCommitReadyChan)
}

// 如果 DebugCM > 0，dlog 会记录一条调试消息。
func (cm *ConsensusModule) dlog(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// 参见论文中的图 2。
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC。
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// 参见论文中的图 2。
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// 我们的日志在 PrevLogIndex 处是否包含任期与 PrevLogTerm 匹配的条目？
		// 注意在 PrevLogIndex=-1 的极端情况下，该条件恒为真。
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && cm.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			reply.Success = true

			// 寻找插入点——即从 PrevLogIndex+1 开始的现有日志与 RPC 中携带的
			// 新条目之间存在任期不一致的位置。
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex += 1
				newEntriesIndex += 1
			}
			// 循环结束时：
			// - logInsertIndex 指向日志末尾，或指向与 leader 条目任期不匹配的索引
			// - newEntriesIndex 指向 Entries 末尾，或指向与对应日志条目任期不匹配的索引

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			// 设置提交索引（commit index）。
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... commitIndex advanced to %d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// electionTimeout 生成一个伪随机的选举超时时长。
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 如果设置了 RAFT_FORCE_MORE_REELECTION，则通过刻意频繁生成硬编码数值来
	// 进行压力测试。这会在不同服务器之间制造冲突，从而强制发生更多次重新选举。
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// runElectionTimer 实现选举定时器。每当需要为新一轮选举启动一个通向候选者状态
// 的定时器时，都应启动它。
//
// 该函数是阻塞式的，应在单独的 goroutine 中启动；它被设计为用于单个（一次性）
// 选举定时器，因为只要 CM 状态从 follower/candidate 发生变化或任期发生变化，
// 它就会退出。
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	cm.dlog("Election timer started in term %d, timeout in %v", termStarted, timeoutDuration)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// 该循环会一直运行，直到出现以下情况之一：
	// - 我们发现选举定时器不再需要，或
	// - 选举定时器超时，此 CM 成为候选者
	// 在 follower 中，它通常在 CM 的整个生命周期内持续在后台运行。
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 如果在超时时长内既没有收到 leader 的消息，也没有为任何人投票，则开始一次选举。
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// startElection 以本 CM 为候选者开始新一轮选举。
// 要求调用前已持有 cm.mu 锁。
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.votedFor = cm.id
	cm.electionResetEvent = time.Now()
	cm.dlog("Starting election for term %d", savedCurrentTerm)

	votesReceived := 1

	// 并发地向所有其他服务器发送 RequestVote RPC。
	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			var reply RequestVoteReply

			cm.dlog("Sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("not candidate, bailing out")
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVote reply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(cm.peerIds)+1 {
							// 赢得了选举！
							cm.dlog("wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}()
	}
	// 再运行一个选举定时器，以防本次选举未能成功。
	go cm.runElectionTimer()
}

// becomeFollower 使 cm 成为 follower 并重置其状态。
// 要求调用前已持有 cm.mu 锁。
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// startLeader 将 cm 切换到 leader 状态，并开始心跳发送流程。
// 要求调用前已持有 cm.mu 锁。
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// 只要仍然是 leader，就周期性地发送心跳。
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// leaderSendHeartbeats 向所有对等节点发送一轮心跳，收集它们的回复并调整 cm 的状态。
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						cm.nextIndex[peerId] = ni - 1
						cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}()
	}
}

// lastLogIndexAndTerm 返回本服务器最后一条日志的索引和任期（若没有日志则返回 -1）。
// 要求调用前已持有 cm.mu 锁。
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// commitChanSender 负责在 cm.commitChan 上发送已提交的条目。它监听
// newCommitReadyChan 的通知，并计算哪些新条目已准备好发送。该方法应在独立的
// 后台 goroutine 中运行；cm.commitChan 可以是带缓冲的，并会限制客户端消费新
// 提交条目的速度。当 newCommitReadyChan 被关闭时返回。
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// 找出我们接下来需要应用的条目。
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

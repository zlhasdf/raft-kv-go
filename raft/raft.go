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
	// mu 用于保护对共享访问的并发访问。
	mu sync.Mutex

	// id 是此CM的服务器ID
	id int

	// peers 是集群中所有服务器的ID列表（包括自己）。
	peerIds []int

	// server 是包含此 CM 的服务器，用于向对等节点发起 RPC 调用。
	server *Server

	// 所有服务器上的持久化 Raft 状态
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 所有服务器上的易失性 Raft 状态
	state              CMState
	electionResetEvent time.Time
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1
	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("Stopping")
}

func (cm *ConsensusModule) dlog(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// 投票请求，参见论文中的图 2。
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

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
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

// 日志追加请求，参见论文中的图 2。
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
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	cm.dlog("Election timer started in term %d, timeout in %v", termStarted, timeoutDuration)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

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

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.votedFor = cm.id
	cm.electionResetEvent = time.Now()
	cm.dlog("Starting election for term %d", savedCurrentTerm)

	var votesReceived int32 = 1

	for _, peerId := range cm.peerIds {
		if peerId == cm.id {
			continue
		}

		go func() {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
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
						if votesReceived*2 > int32(len(cm.peerIds)) {
							cm.dlog("wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}()
	}
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// startLeader 将 cm 切换到 leader 状态，并启动周期性心跳发送。
// 要求调用前已持有 cm.mu 锁。
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
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

// leaderSendHeartbeats 向所有对等节点发送一轮心跳，收集回复并调整 cm 的状态。
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		if peerId == cm.id {
			continue
		}
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func() {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}()
	}
}

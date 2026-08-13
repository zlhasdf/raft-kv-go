// 用于编写 Raft 测试的测试框架（harness）。
package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu sync.Mutex

	// cluster 是参与同一个集群的所有 raft 服务器列表。
	cluster []*Server

	// commitChans 中为集群中的每个服务器保存一个通道，即该服务器的提交通道。
	commitChans []chan CommitEntry

	// commits 中索引 i 处保存服务器 i 到目前为止提交的条目序列。它由监听对应
	// commitChans 通道的 goroutine 填充。
	commits [][]CommitEntry

	// connected 为集群中的每个服务器保存一个布尔值，表示该服务器当前是否与
	// 对等节点保持连接（若为 false，则它已被分区，任何消息都无法进出）。
	connected []bool

	n int
	t *testing.T
}

// NewHarness 创建一个新的测试 Harness，初始化为 n 个相互连接的服务器。
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan any)

	// 创建该集群中的所有服务器，并分配 id 和对等节点 id。
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewServer(i, peerIds, ready, commitChans[i])
		ns[i].Serve()
	}

	// 将所有对等节点相互连接。
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster:     ns,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		n:           n,
		t:           t,
	}
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

// Shutdown 关闭 harness 中的所有服务器，并等待它们停止运行。
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}
}

// DisconnectPeer 将一个服务器与集群中的所有其他服务器断开连接。
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer 将一个服务器与集群中的所有其他服务器重新连接。
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// CheckSingleLeader 检查是否只有一个服务器认为自己是 leader。
// 返回 leader 的 id 和任期。如果尚未识别出 leader，它会重试若干次。
func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader 检查没有任何已连接的服务器认为自己是 leader。
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// collectCommits 读取通道 commitChans[i]，并将收到的所有条目添加到对应的
// commits[i] 中。它是阻塞式的，应在单独的 goroutine 中运行；当 commitChans[i]
// 被关闭时返回。
func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

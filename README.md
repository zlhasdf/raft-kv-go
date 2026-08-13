# raft-kv-go

基于 Go 的 Raft 共识算法学习项目。当前实现对应教程的 part2 阶段：在「选举」之上补全了「日志复制」与「提交」机制，使用 `net/rpc` 在多个节点之间通信，并配有选举场景测试。

> 项目参考 Eli Bendersky 的 Raft 系列教程：[https://eli.thegreenplace.net/tag/raft](https://eli.thegreenplace.net/tag/raft)

## 项目结构

```
raft/
├── raft.go          # 共识模块核心：状态机、选举、日志复制与提交
├── server.go        # RPC 服务器容器：节点通信、RPC 代理、断连/不可靠网络模拟
├── testharness.go   # 测试框架：多节点集群搭建、断连重连、提交条目收集
├── raft_test.go     # 测试用例（目前为选举场景）
├── go.mod
└── go.sum
```

## 模块设计思路

### raft.go —— 共识模块核心

整个项目最核心的文件，实现 Raft 论文 Figure 2 中的状态机。

**角色状态机**

- `CMState` 定义 `Follower` / `Candidate` / `Leader` / `Dead` 四种状态；
- `Follower → Candidate`：选举超时，且超时内没有收到有效心跳或投票请求；
- `Candidate → Leader`：获得集群多数派投票；
- 任意状态 → `Follower`：发现更高任期，立即降级。

**选举**

- 每个节点持有随机的选举超时（150–300ms），超时后自增任期、投票给自己，并并发向所有对等节点发送 `RequestVote`；
- `RequestVote` 的授予条件除“任期未过期、未投过票”外，还要求候选者的日志不比自己旧（比较 `LastLogIndex` / `LastLogTerm`），保证已提交日志不会被覆盖；
- 票数达到 `votesReceived*2 > len(peerIds)+1` 即当选 leader。

**日志复制与提交**

- `Submit` 只接受 leader 提交的客户端命令，将其追加到本地日志（返回 `true` 表示本节点是 leader）；
- leader 每 50ms 通过 `AppendEntries` 携带 `PrevLogIndex` / `PrevLogTerm` / `Entries` / `LeaderCommit` 广播给所有对等节点；
- follower 校验 `PrevLogIndex` / `PrevLogTerm` 与本地日志一致后，截断冲突条目并插入新条目，再根据 `LeaderCommit` 推进本地 `commitIndex`；
- leader 根据 `matchIndex` 统计多数派，只提交当前任期内的日志条目；提交后通过 `newCommitReadyChan` 通知 `commitChanSender`，把 `CommitEntry{Command, Index, Term}` 发送到客户端传入的 `commitChan`。

**并发模型**

- 所有共享状态由 `cm.mu` 保护；RPC 调用在锁外执行，收到回复后再重新加锁处理；
- 选举定时器、心跳发送、`commitChanSender` 各自运行在独立的 goroutine 中。

### server.go —— 网络与 RPC 容器

把 `ConsensusModule` 暴露为网络服务：

- `Server` 持有 `ConsensusModule`、`rpc.Server` 以及到各对等节点的 `rpc.Client`；
- `Serve` 创建 RPC 服务器并注册 `RPCProxy`，随后进入 accept 循环处理连接；
- `RPCProxy` 是直通代理：把 `RequestVote` / `AppendEntries` 转发给 CM；设置 `RAFT_UNRELIABLE_RPC` 后按概率丢弃（约 10%）或延迟（75ms）RPC，模拟不可靠网络；
- `ConnectToPeer` / `DisconnectPeer` / `DisconnectAll` / `Call` 提供集群连接管理，供测试框架做断连、重连操作。

### testharness.go —— 测试框架

用于快速搭建多节点集群：

- `NewHarness` 创建 n 个 `Server` 并两两连接，关闭 `ready` 通道后所有节点同时开始选举；
- 每个节点都有一个 `commitChans[i]` 提交通道，`collectCommits` goroutine 持续读取并记录到 `commits[i]`，既避免提交发送阻塞，也便于后续断言提交结果；
- `CheckSingleLeader` / `CheckNoLeader` 轮询检查集群中 leader 的唯一性；
- `DisconnectPeer` / `ReconnectPeer` / `Shutdown` 模拟分区、恢复与清理。

### raft_test.go —— 测试用例

目前包含 8 个选举场景测试（见下方表格），覆盖单 leader、leader 掉线、失去多数派、全部断连恢复、follower 回归、循环断连等。日志复制与提交机制已经实现，但专门的提交测试尚未编写。

## 已实现的功能

- **节点状态机**：Follower / Candidate / Leader / Dead 四种状态
- **选举**：随机超时（150–300ms）、任期管理、日志新旧校验、多数派判定
- **日志复制**：AppendEntries 一致性检查、日志插入/截断、LeaderCommit 推进
- **提交通知**：leader 多数派提交 + `commitChan` 向客户端上报 `CommitEntry`
- **心跳**：leader 每 50ms 广播一次，重置 follower 的选举计时
- **任期处理**：收到更高任期的 RPC 或回复时，自动降级为 Follower 并更新任期
- **RPC 通信**：基于 `net/rpc` 的 TCP 服务，支持节点的断连与重连
- **不可靠网络模拟**：可通过 `RAFT_UNRELIABLE_RPC` 模拟随机丢包和延迟
- **测试**：8 个选举场景测试

## 快速开始

运行全部测试：

```bash
cd raft
go test -v ./...
```

> 节点之间通过 TCP RPC 通信（监听随机端口），请确保运行环境允许监听端口。

## 测试用例

| 测试 | 场景 |
| --- | --- |
| `TestElectionBasic` | 3 节点集群选出唯一 leader |
| `TestElectionLeaderDisconnect` | leader 掉线后选出新 leader，任期递增 |
| `TestElectionLeaderAndAnotherDisconnect` | 失去多数派时无 leader；重连后恢复 |
| `TestDisconnectAllThenRestore` | 全部断开无 leader；全部重连后恢复 |
| `TestElectionLeaderDisconnectThenReconnect` | 旧 leader 回归后保持 follower，不打断新 leader |
| `TestElectionLeaderDisconnectThenReconnect5` | 同上，5 节点集群版本 |
| `TestElectionFollowerComesBack` | follower 回归触发重新选举，任期变化 |
| `TestElectionDisconnectLoop` | 5 轮断开/重连循环，集群始终能恢复 |

## 环境变量

| 环境变量 | 作用 |
| --- | --- |
| `RAFT_FORCE_MORE_REELECTION` | 设置后约 1/3 概率使用固定 150ms 超时，制造超时冲突，强制更多重新选举（压力测试用） |
| `RAFT_UNRELIABLE_RPC` | 设置后模拟不可靠网络：约 10% 概率丢弃 RPC，约 10% 概率延迟 75ms |

开启调试日志：将 `raft.go` 中的 `DebugCM` 改为 `1`（当前默认开启），日志会带上服务器 ID，方便观察多节点行为。

## 当前限制

- 所有状态保存在内存中，未实现持久化（节点重启后日志丢失）
- 未实现快照 / 日志压缩
- 心跳采用固定 50ms ticker 的 part2 风格，未实现 part3 的事件驱动广播与快速冲突回退（`ConflictIndex` / `ConflictTerm`）
- `Submit` 只返回是否为 leader（bool），未返回提交的日志索引
- 提交机制已实现，但测试目前只覆盖选举场景，尚无专门的提交测试

## License

参考代码遵循公有领域（public domain），本项目用于学习目的。

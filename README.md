# raft-kv-go

基于 Go 的 Raft 共识算法学习项目。当前包含一个可运行的 Raft 选举实现（leader election），使用 `net/rpc` 在多个节点之间通信，并配有完整的选举测试。

> 项目参考 Eli Bendersky 的 Raft 系列教程：[https://eli.thegreenplace.net/tag/raft](https://eli.thegreenplace.net/tag/raft)

## 项目结构

```
raft/
├── raft.go          # 共识模块核心：状态机、选举、心跳
├── server.go        # RPC 服务器：节点通信、断连/不可靠网络模拟
├── testharness.go   # 测试工具：多节点集群的搭建、断连与重连
├── raft_test.go     # 选举相关测试
├── go.mod
└── go.sum
```

## 已实现的功能

- **节点状态机**：Follower / Candidate / Leader / Dead 四种状态
- **选举**：随机选举超时（150–300ms）、任期管理、投票机制、多数派判定
- **心跳**：Leader 每 50ms 向所有对等节点发送 AppendEntries 心跳，维持权威并重置 follower 的选举计时
- **任期处理**：收到更高任期的 RPC 或回复时，自动降级为 Follower 并更新任期
- **RPC 通信**：基于 `net/rpc` 的 TCP 服务，支持对节点的断连与重连
- **不可靠网络模拟**：可通过环境变量模拟 RPC 随机丢失和延迟
- **测试**：8 个选举场景测试，覆盖 leader 掉线、分区、恢复、多节点等场景

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

这是一个以理解选举流程为目的的学习实现，日志复制部分尚未完成：

- `AppendEntries` 只处理心跳和任期检查，尚未真正追加日志条目（`Entries`）
- `RequestVote` 尚未校验候选者的日志新旧程度（`LastLogIndex`/`LastLogTerm`）
- 未实现日志提交（`LeaderCommit`）、快照等后续章节内容

## License

参考代码遵循公有领域（public domain），本项目用于学习目的。

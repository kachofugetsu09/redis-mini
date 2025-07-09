# Redis Mini - Raft 分布式一致性模块

## 模块概览

`redis-raft` 模块是 `redis-mini` 项目的分布式一致性核心。它实现了 Raft 分布式共识算法，为 Redis Mini 提供了强一致性保证和容错能力。该模块确保即使在网络分区、节点故障等复杂环境下，集群仍能维持数据一致性并持续提供服务。

Raft 算法以其相对简单和易于理解的设计著称，本模块严格按照 Raft 论文的规范实现了领导者选举、日志复制、持久化等核心功能。它与 Redis Mini 的其他模块深度集成，使得分布式场景下的 Redis 命令能够被正确地复制和执行。

---

## 核心设计

`redis-raft` 模块的架构严格遵循 Raft 算法的设计原则，清晰地分离了核心算法逻辑、网络通信、持久化存储等关键功能。

### 1. 算法核心层 (`raft`)

这一层是 Raft 算法的核心实现，负责维护节点状态和算法逻辑。

* **`Raft` 类**: 这是整个模块的算法核心。它维护了 Raft 论文中定义的所有关键状态：
  * **持久化状态**: `currentTerm`（当前任期）、`votedFor`（投票对象）、`log`（日志条目）
  * **易失性状态**: `commitIndex`（已提交索引）、`lastApplied`（已应用索引）
  * **领导者状态**: `nextIndex`（下一个发送索引）、`matchIndex`（已匹配索引）

* **`RaftNode` 类**: 这是 Raft 节点的高级封装和管理器。它负责：
  * 管理选举定时器和心跳定时器
  * 协调网络层和算法层的交互
  * 处理节点生命周期（启动、停止、状态转换）
  * 优化网络配置以适应真实环境（超时时间为 3-6 秒）

### 2. 状态模型 (`core`)

这一层定义了 Raft 算法中的核心概念和数据结构。

* **`RoleState` 枚举**: 定义了三种节点角色：`FOLLOWER`（跟随者）、`CANDIDATE`（候选人）、`LEADER`（领导者）
* **`LogEntry` 类**: 表示日志条目，包含：
  * `logIndex`: 日志索引
  * `logTerm`: 日志任期
  * `command`: Redis 命令（RespArray 格式）
* **`AppendResult` 类**: 封装日志追加操作的结果，用于客户端接口

### 3. 网络通信层 (`network`)

这一层负责节点间的网络通信，基于 Netty 构建高性能异步网络框架。

* **`RaftNetwork` 接口**: 定义了 Raft 网络通信的抽象接口，支持：
  * `sendRequestVote()`: 发送投票请求
  * `sendAppendEntries()`: 发送日志追加请求
  * 异步操作和超时处理

* **`NettyRaftNetwork` 实现**: 基于 Netty 的具体网络实现，提供：
  * 连接池管理和断线重连
  * 消息序列化和反序列化
  * 网络统计和监控
  * 性能优化（连接复用、批量发送）

* **消息处理器**: 
  * `RaftServerHandler`: 处理接收到的 Raft 请求
  * `RaftClientHandler`: 处理发送请求的响应

### 4. 持久化存储 (`persistence`)

这一层负责 Raft 状态的持久化，确保重启后能恢复状态。

* **`LogSerializer`**: 专门用于日志条目的序列化和反序列化：
  * 高效的二进制格式存储
  * 支持批量读写操作
  * 数据完整性校验

### 5. RPC 消息协议 (`rpc`)

这一层定义了 Raft 节点间通信的消息格式。

* **`RaftMessage`**: 统一的消息封装，支持四种消息类型
* **投票相关**: `RequestVoteArg`、`RequestVoteReply`
* **日志复制相关**: `AppendEntriesArgs`、`AppendEntriesReply`

---

## 算法特性与优化

### 1. 选举机制

* **随机化超时**: 选举超时时间在 3-6 秒间随机，避免选举冲突
* **快速收敛**: 支持多数票立即成为领导者，无需等待所有响应
* **网络适应**: 针对真实网络环境优化的超时参数

### 2. 日志复制

* **批量发送**: 支持批量日志条目传输，提高网络效率
* **并行复制**: 异步并行向所有跟随者发送日志
* **一致性保证**: 严格按照 Raft 算法保证日志一致性

### 3. 状态机集成

* **异步应用**: 使用虚拟线程异步应用已提交的日志到状态机
* **Redis 集成**: 直接调用 `RedisCore` 执行 Redis 命令
* **错误处理**: 完善的异常处理和恢复机制

### 4. 网络优化

* **连接管理**: 智能的连接池和重连机制
* **序列化优化**: 高效的消息序列化格式
* **超时控制**: 细粒度的网络超时控制

---

## 线程安全模型

本模块采用了多层次的线程安全设计：

* **算法状态同步**: 使用 `synchronized` 保护 Raft 核心状态
* **网络异步化**: 基于 Netty 的异步网络 I/O，避免阻塞
* **状态机应用**: 独立的虚拟线程池处理状态机应用
* **定时器管理**: 使用 `ScheduledExecutorService` 管理定时任务

---

## 在项目中的角色

`redis-raft` 模块是 Redis Mini 分布式能力的核心实现。

* **上游**: 被 `redis-server` 模块集成，通过 `RedisContext` 提供分布式一致性服务
* **下游**: 依赖 `redis-core` 执行状态机命令，依赖 `redis-protocal` 处理消息格式，依赖 `redis-common` 提供基础数据结构

### 集成方式

```java
// 在 RedisContext 中集成 Raft
RedisContext context = new RedisContextImpl(redisCore, host, port, config);
Raft raft = new Raft(serverId, peerIds, network, redisCore);
context.setRaft(raft);

// 启动 Raft 节点
RaftNode raftNode = new RaftNode(serverId, peerIds, network, redisCore);
raftNode.start();
```

---

## 使用场景

### 1. 分布式 Redis 集群

* 提供强一致性保证
* 支持自动故障转移
* 保证数据不丢失

### 2. 高可用架构

* 容忍少数节点故障
* 自动选举新的领导者
* 快速故障检测和恢复

### 3. 读写分离

* 写操作仅在领导者执行
* 读操作可以从跟随者执行
* 保证读取的一致性

---

## 性能特点

### 1. 网络性能

* **连接复用**: 减少连接建立开销
* **批量传输**: 支持批量日志传输
* **异步处理**: 全异步网络 I/O

### 2. 存储性能

* **高效序列化**: 紧凑的二进制存储格式
* **批量写入**: 支持日志批量持久化
* **快速恢复**: 优化的状态恢复机制

### 3. 算法性能

* **快速选举**: 平均选举时间在秒级
* **高吞吐**: 支持高频率的日志复制
* **低延迟**: 优化的消息处理流程

---

## 配置与调优

### 1. 网络参数

```java
// 选举超时时间（毫秒）
private final int MIN_ELECTION_TIMEOUT = 3000;
private final int MAX_ELECTION_TIMEOUT = 6000;

// 心跳间隔（毫秒）
private final int HEARTBEAT_INTERVAL = 500;
```

### 2. 持久化参数

* 日志文件路径配置
* 批量写入大小
* 同步策略选择

### 3. 集群配置

* 节点地址配置
* 集群大小设置
* 网络超时参数

---

## 如何构建与测试

该模块可以使用 Maven 独立进行构建和测试。

```bash
# 进入模块根目录
cd redis-raft

# 运行测试并安装构建产物
mvn clean install
```

### 测试覆盖

* **Raft3ATest**: 基础的领导者选举测试
* **Raft3BTest**: 日志复制和一致性测试  
* **Raft3CTest**: 持久化和故障恢复测试
* **LogSerializerTest**: 持久化序列化测试

---

## 扩展与定制

### 1. 自定义网络层

实现 `RaftNetwork` 接口以支持不同的网络协议：

```java
public class CustomRaftNetwork implements RaftNetwork {
    // 自定义网络实现
}
```

### 2. 自定义持久化

扩展 `LogSerializer` 以支持不同的存储后端：

```java
public class CustomLogSerializer extends LogSerializer {
    // 自定义序列化逻辑
}
```

### 3. 监控集成

添加监控指标收集：

```java
// 网络统计
long rpcSentCount = network.getRpcSentCount();
long rpcReceivedCount = network.getRpcReceivedCount();

// 算法状态
RoleState currentState = raft.getState();
int currentTerm = raft.getCurrentTerm();
```

---

通过 `redis-raft` 模块，Redis Mini 获得了企业级的分布式一致性能力，能够在复杂的分布式环境中提供可靠、高性能的服务。该模块的设计充分考虑了实际部署中的各种挑战，为构建大规模分布式 Redis 集群奠定了坚实基础。

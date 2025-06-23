# Redis Mini - Replication 复制模块

## 模块概览

`redis-replication` 模块为 `redis-mini` 项目提供了核心的主从复制功能。它是实现 Redis 高可用性（High Availability）和读写分离（Read/Write Splitting）的基础。本模块完整实现了 Redis 的复制协议，包括数据同步（全量与增量）、命令传播、连接管理和心跳检测。

本模块的设计目标是实现一个稳定、可靠且高效的主从复制系统，为后续的集群（Cluster）功能奠定基础。

---

## 核心组件与工作流程

本模块通过一系列精心设计的组件协同工作，完整模拟了 Redis 的主从复制机制。

### 1. `RedisNode`：节点的核心抽象

无论是主节点（Master）还是从节点（Slave），在架构中都被统一抽象为一个 `RedisNode` 实例。`RedisNode` 封装了节点的网络连接、复制状态 (`NodeState`)、以及与之关联的核心管理器。

### 2. 依赖倒置与 `ReplicationHost` 接口

为了实现高度解耦，本模块遵循**依赖倒置原则**。它定义了 `ReplicationHost` 接口，来描述它需要上层模块（`redis-server`）提供的服务，例如：
* 获取核心数据访问接口 (`getRedisCore()`)
* 执行从主节点传来的命令 (`executeCommand()`)
* 生成 RDB 快照 (`generateRdbSnapshot()`)

`redis-server` 模块通过实现此接口，将自身能力注入到复制模块中，从而避免了 `replication` 模块对 `server` 模块的直接依赖，使架构更加清晰和可测试。

### 3. 复制流程：从 `PSYNC` 开始

1.  **连接与握手**: 从节点启动后，会向主节点发送 `PSYNC ? -1` 命令，请求进行数据同步。
2.  **全量同步 (Full Resynchronization)**: 主节点收到请求后，会响应 `FULLRESYNC <runid> <offset>`。随后，主节点会执行一次BGSAVE（通过`ReplicationHost`接口），生成当前数据库的RDB快照，并将其发送给从节点。从节点接收到RDB文件后，会清空自身数据，然后加载RDB文件以完成数据的全量同步。
3.  **命令传播 (Command Propagation)**: 全量同步完成后，主节点会将所有新的写命令实时地传播给从节点。从节点接收到这些命令后，会通过 `ReplicationHost.executeCommand()` 在本地执行，从而与主节点的数据保持最终一致。

### 4. 增量同步与复制积压缓冲区

为了优化断线重连的效率，本项目实现了 Redis 的**增量同步**机制。

* **`ReplBackLog` (复制积压缓冲区)**: 这是一个在主节点上维护的、固定大小的**环形缓冲区 (circular buffer)**。它记录了主节点最近执行的写命令。
* **部分重同步 (Partial Resynchronization)**: 当从节点因网络问题短暂断开后，它会尝试重连并发送 `PSYNC <master_runid> <offset>` 命令，其中包含了它断开前的主节点ID和复制偏移量。
    * 如果主节点发现这个 `offset` 仍然在它的 `ReplBackLog` 范围内，它就会只发送从节点断线期间错过的命令，而**无需发送完整的RDB快照**。
    * 这个机制大大减少了不必要的全量同步，显著降低了网络开销和数据恢复时间。

### 5. 连接维护

* **`HeartbeatManager`**: 从节点会通过一个心跳管理器，定期向主节点发送 `PING` 命令作为心跳，以检测连接是否存活，并处理连接超时和重连逻辑。
* **`ReplicationStateMachine`**: 内部使用一个有限状态机（`DISCONNECTED`, `CONNECTING`, `SYNCING`, `STREAMING`）来精确管理和追踪复制连接在不同阶段的生命周期，确保状态转换的正确性和一致性。

## 当前实现的设计考量与待改进点

为了快速迭代和验证核心功能，当前 `redis-replication` 模块的实现采用了一种较为直接和紧凑的设计。这也意味着在与原生 Redis 协议的对齐、以及架构的健壮性方面，存在一些已知的待改进点。这些点是未来版本重构的重点，也是当前暂未编写详尽单元测试的原因。

1.  **简化的复制握手流程**
    * **现状**: 当前从节点直接发送 `PSYNC` 命令发起同步。
    * **与原生差异**: 缺少了原生协议中连接初期的 `REPLCONF` 能力通告阶段。原生 Redis 从节点会先发送 `REPLCONF listening-port <port>` 和 `REPLCONF capa eof` 等命令，告知主节点自己的监听端口和支持的功能（如 EOF 风格的 RDB 传输），而当前实现省略了此步骤。

2.  **主节点对 ACK 的处理缺失**
    * **现状**: 主节点在传播命令后，并未实现对从节点 `REPLCONF ACK <offset>` 响应的接收和处理逻辑。
    * **与原生差异**: 这导致主节点无法精确追踪每个从节点的同步进度，也难以通过 ACK 的延迟来判断从节点的健康状况和同步延迟。这是实现高可用监控（如 Sentinel）和保证数据一致性的关键环节。

3.  **心跳与断线重连机制较为基础**
    * **现状**: 心跳机制目前主要用于检测连接存活。当连接断开时，虽然会尝试使用已保存的 `offset` 进行增量同步，但缺乏更复杂的失败处理策略。
    * **待改进点**: 原生 Redis 在处理连接问题时有更精细的逻辑，例如在连续多次 `PSYNC` 失败后的行为定义（如强制全量同步、增加重试间隔、记录错误状态等）。

4.  **模块职责耦合度偏高**
    * **现状**: 当前模块内的核心类（如 `ReplicationHandler`, `ReplicationManager`）承担了较多的复合职责，将网络事件处理、协议解析和业务逻辑耦合在一起。
    * **待改进点**: 设计上缺乏清晰的事件驱动和分层。例如，`ReplicationHandler` 直接处理了 `FULLRESYNC` 等业务逻辑，而不是将协议消息解析后发布为领域事件，交由专门的服务来处理。这种高耦合使得扩展（如支持 Sentinel）和单元测试变得复杂。

## 在项目中的角色

`redis-replication` 是一个提供分布式能力的高级服务模块。

* **上游**: 它被 `redis-server` 模块集成和管理，服务器通过 `RedisNode` 对象来配置和启动主从模式。
* **下游**: 它依赖 `redis-core` 来执行复制的命令，依赖 `redis-persistence` 来生成RDB快照，依赖 `redis-protocal` 来进行网络通信。

## 如何构建与测试

该模块可以使用 Maven 独立进行构建和测试。

```bash
# 进入模块根目录
cd redis-replication

# 运行测试并安装构建产物
mvn clean install
```
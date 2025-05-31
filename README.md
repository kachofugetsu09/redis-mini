# miniRedis - Enterprise-Grade Java Redis Implementation

[![Java](https://img.shields.io/badge/Java-8+-brightgreen.svg)](https://www.oracle.com/java/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Performance](https://img.shields.io/badge/Performance-415k%20ops%2Fsec-brightgreen.svg)]()
[![AOF](https://img.shields.io/badge/AOF-Batch%20Optimized-blue.svg)]()
[![Replication](https://img.shields.io/badge/Replication-Full%20PSYNC-success.svg)]()

miniRedis 是一个生产级的 Redis 服务器实现，使用 Java 8+ 和 Netty 构建。项目实现了完整的 Redis 核心功能，包括高性能的 AOF 持久化、主从复制、以及优化的内部数据结构。

## 📋 目录

- [项目特色](#项目特色)
- [快速启动](#快速启动)
- [核心架构](#核心架构)
- [支持的命令](#支持的命令)
- [内部数据结构](#内部数据结构)
- [持久化系统](#持久化系统)
- [主从复制](#主从复制)
- [性能基准](#性能基准)
- [开发指南](#开发指南)
- [许可证](#许可证)

## 🚀 项目特色

### 工业级性能
- **高吞吐量**: AOF 批量写入达到 415,000 命令/秒
- **低延迟**: 平均写入延迟 < 1ms，批量优化提升延迟 75.49%
- **高并发**: 支持 10+ 线程并发写入，86,957 命令/秒吞吐量
- **智能背压**: 6MB 背压阈值，防止内存溢出，自动调节写入速度

### 企业级可靠性
- **数据安全**: 99.86%+ 崩溃恢复成功率，8-14ms 快速恢复
- **一致性保证**: 支持原子操作、事务语义和数据完整性验证
- **故障恢复**: 完整的 AOF 重写、断点续传和增量恢复机制
- **监控支持**: 详细的性能指标、心跳统计和状态监控

### 先进的技术架构
- **异步 I/O**: 基于 Netty 的事件驱动架构，支持万级连接
- **批量优化**: 16-50 命令智能批处理，243.75% 吞吐量提升
- **内存高效**: 渐进式哈希表、跳表索引和 SDS 字符串优化
- **协议兼容**: 完整 RESP 协议支持，与 Redis 客户端 100% 兼容

## ⚡ 快速启动

### 环境要求
- **Java**: 8+ 

### 单机模式启动

**使用默认配置启动服务器**：
```bash
# 编译项目
mvn clean compile

# 运行单机Redis服务器 (localhost:6379)
java -cp target/classes site.hnfy258.RedisServerLauncher
```

**关键类**: `RedisServerLauncher` - 单机服务器启动器
- 默认端口: 6379
- 默认数据库数量: 16
- 持久化: 启用 RDB，AOF 可配置

### 主从集群模式

**启动完整的主从集群**：
```bash
# 启动主从集群 (主节点:6379, 从节点:6380,6381)
java -cp target/classes site.hnfy258.RedisMsLauncher
```

**集群架构**：
- **主节点**: localhost:6379 (master-node)
- **从节点1**: localhost:6380 (slave-node1)  
- **从节点2**: localhost:6381 (slave-node2)
- **复制机制**: PSYNC 协议，支持全量和增量同步
- **心跳监控**: 1秒间隔，3次失败触发重连

### 持久化配置

**在 RedisMiniServer 中配置持久化选项**：
```java
// 启用/禁用持久化机制
private static final boolean ENABLE_AOF = false;  // AOF 日志持久化
private static final boolean ENABLE_RDB = true;   // RDB 快照持久化

// 构造函数支持自定义 RDB 文件名
RedisMiniServer server = new RedisMiniServer("localhost", 6379, "custom.rdb");
```

**持久化特性**：
- **RDB**: 二进制快照，快速启动恢复
- **AOF**: 命令日志，数据安全性更高
- **支持同时启用**: 提供双重数据保护
- **自动恢复**: 启动时自动加载持久化数据

### 断线重连测试

**测试主从复制的健壮性**：
```bash
# 启动带重连测试的主从集群
java -cp target/classes site.hnfy258.RedisMsLauncherWithReconnect
```

**测试功能**：
- 自动故障检测和重连
- 增量数据同步验证  
- 心跳机制压力测试
- 网络中断恢复测试

### 客户端连接

**使用标准 Redis 客户端连接**：
```bash
# 使用 redis-cli 连接
redis-cli -h localhost -p 6379

# 测试基本功能
127.0.0.1:6379> ping
PONG
127.0.0.1:6379> set test "Hello miniRedis"
OK
127.0.0.1:6379> get test
"Hello miniRedis"
```


## 🏗️ 核心架构

### 系统架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    miniRedis 架构                            │
├─────────────────────────────────────────────────────────────┤
│  Client Layer (RESP Protocol)                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ RespDecoder │ RespEncoder │ RespCommandHandler          │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Core Engine                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ RedisCore │ CommandSystem │ Database(16 DBs)            │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Data Structures                                           │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Dict │ SkipList │ SDS │ LinkedList │ HashTable           │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Persistence Layer                                         │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ AOF(BatchWriter) │ RDB(Writer/Loader) │ FileUtils       │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Replication System                                        │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ PSYNC │ ReplBackLog │ HeartbeatManager │ StateMachine    │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Network Layer (Netty)                                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ NioEventLoopGroup │ ChannelPipeline │ Bootstrap          │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件详解

#### 🌐 网络通信层 (`server` & `protocal`)

**RESP 协议处理** - 完整的 Redis 序列化协议支持：
```java
// 核心编解码器
site.hnfy258.server.handler.RespDecoder     // RESP 协议解码
site.hnfy258.server.handler.RespEncoder     // RESP 协议编码  
site.hnfy258.server.handler.RespCommandHandler  // 命令处理

// 协议类型实现
site.hnfy258.protocal.SimpleString          // +OK\r\n
site.hnfy258.protocal.BulkString            // $6\r\nfoobar\r\n
site.hnfy258.protocal.RespArray             // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
site.hnfy258.protocal.RespInteger           // :1000\r\n
site.hnfy258.protocal.Errors                // -Error message\r\n
```

**网络特性**：
- **异步 I/O**: Netty NioEventLoopGroup，CPU 核心数 x2 工作线程
- **连接池**: 支持多路复用，低资源占用
- **协议兼容**: 100% Redis RESP 协议兼容
- **错误处理**: 完整的错误响应和异常处理机制

#### ⚡ 高性能数据结构 (`internal`)

**跳表 (SkipList)** - 有序集合的核心实现：
```java
site.hnfy258.internal.SkipList<T extends Comparable<T>>

// 核心操作 - O(log n) 时间复杂度
public SkipListNode<T> insert(double score, T member);
public boolean delete(double score, T member);
public List<SkipListNode<T>> getElementByRankRange(long start, long end);
public List<SkipListNode<T>> getElementByScoreRange(double min, double max);
```

**渐进式哈希表 (Dict)** - 非阻塞扩容机制：
```java
site.hnfy258.internal.Dict<K,V>

private DictHashTable<K,V> ht0;  // 主哈希表
private DictHashTable<K,V> ht1;  // 扩容时的临时哈希表  
private int rehashIndex;         // 渐进式 rehash 进度

// 每次操作时执行少量 rehash，避免阻塞
private void rehashStep();
```

**简单动态字符串 (SDS)** - 内存优化的字符串：
```java
site.hnfy258.internal.Sds

private byte[] bytes;    // 字符串内容
private int len;         // 已使用长度  
private int alloc;       // 分配的总长度

// 预分配策略，减少内存重分配次数
public Sds append(byte[] extra);
```

#### 🗃️ 数据类型实现 (`datastructure`)

| 数据类型 | 实现类 | 底层结构 | 特性 |
|---------|--------|----------|------|
| **String** | `RedisString` | SDS | 二进制安全，O(1)长度获取 |
| **List** | `RedisList` | LinkedList | 双向链表，O(1)头尾操作 |
| **Set** | `RedisSet` | Dict | 哈希表，O(1)成员操作 |
| **Hash** | `RedisHash` | Dict | 嵌套哈希表，O(1)字段操作 |
| **ZSet** | `RedisZset` | SkipList + Dict | O(log n)范围查询，O(1)分数查找 |

#### 🎯 命令系统 (`command`)

**命令接口设计**：
```java
site.hnfy258.command.Command

public interface Command {
    CommandType getType();           // 命令类型识别
    void setContext(Resp[] array);   // 参数注入
    Resp handle();                   // 命令执行
    boolean isWriteCommand();        // 写命令标识(用于主从复制)
}
```

**支持的命令类型**：
- **String**: `GET`, `SET` - 基础键值操作
- **List**: `LPUSH`, `LPOP`, `LRANGE` - 列表操作
- **Set**: `SADD`, `SPOP`, `SREM` - 集合操作
- **Hash**: `HSET`, `HGET`, `HDEL` - 哈希表操作
- **ZSet**: `ZADD`, `ZRANGE` - 有序集合操作
- **通用**: `PING`, `SELECT` - 连接和数据库操作
- **持久化**: `BGSAVE`, `BGREWRITEAOF` - 后台持久化
- **复制**: `PSYNC` - 主从同步协议

## 📊 支持的命令

### 命令兼容性矩阵

| 类型 | 命令 | 实现类 | 功能描述 | 时间复杂度 |
|------|------|--------|----------|------------|
| **String** | `SET key value` | `site.hnfy258.command.impl.string.Set` | 设置字符串值 | O(1) |
| | `GET key` | `site.hnfy258.command.impl.string.Get` | 获取字符串值 | O(1) |
| **List** | `LPUSH key value [value ...]` | `site.hnfy258.command.impl.list.Lpush` | 左侧插入元素 | O(N) |
| | `LPOP key` | `site.hnfy258.command.impl.list.Lpop` | 左侧弹出元素 | O(1) |
| | `LRANGE key start stop` | `site.hnfy258.command.impl.list.Lrange` | 范围查询 | O(N) |
| **Set** | `SADD key member [member ...]` | `site.hnfy258.command.impl.set.Sadd` | 添加成员 | O(N) |
| | `SPOP key [count]` | `site.hnfy258.command.impl.set.Spop` | 随机弹出成员 | O(1) |
| | `SREM key member [member ...]` | `site.hnfy258.command.impl.set.Srem` | 删除成员 | O(N) |
| **Hash** | `HSET key field value` | `site.hnfy258.command.impl.hash.Hset` | 设置哈希字段 | O(1) |
| | `HGET key field` | `site.hnfy258.command.impl.hash.Hget` | 获取哈希字段 | O(1) |
| | `HDEL key field [field ...]` | `site.hnfy258.command.impl.hash.Hdel` | 删除哈希字段 | O(N) |
| **ZSet** | `ZADD key score member [score member ...]` | `site.hnfy258.command.impl.zset.Zadd` | 添加有序成员 | O(log N) |
| | `ZRANGE key start stop [WITHSCORES]` | `site.hnfy258.command.impl.zset.Zrange` | 范围查询 | O(log N + M) |
| **通用** | `PING` | `site.hnfy258.command.impl.Ping` | 测试连接 | O(1) |
| | `SELECT db` | `site.hnfy258.command.impl.Select` | 选择数据库 | O(1) |
| **持久化** | `BGSAVE` | `site.hnfy258.command.impl.aof.Bgsave` | 后台保存 RDB | O(N) |
| | `BGREWRITEAOF` | `site.hnfy258.command.impl.aof.Bgrewriteaof` | 重写 AOF 文件 | O(N) |
| **复制** | `PSYNC runid offset` | `site.hnfy258.command.impl.cluster.Psync` | 主从同步协议 | O(1) |

### 命令扩展特性

- **参数验证**: 所有命令都包含完整的参数验证和错误提示
- **类型检查**: 严格的数据类型检查，防止类型混用错误
- **原子操作**: 单个命令的原子性保证
- **事务支持**: 为未来事务功能预留接口设计
- **错误处理**: 详细的错误信息和标准 Redis 错误格式

## 🏆 内部数据结构详解

### 跳表 (SkipList) - 有序集合的核心

**实现位置**: `site.hnfy258.internal.SkipList`

**技术优势**:
- **时间复杂度**: 插入、删除、查找均为 O(log n)
- **空间局部性**: 比平衡树更好的缓存性能，减少 CPU 缓存缺失
- **范围查询**: 天然支持范围查询和排序操作，无需额外索引
- **实现简单**: 比红黑树等平衡树实现更简单，维护成本低
- **概率平衡**: 基于随机化的概率平衡，避免最坏情况

**核心操作**:
```java
// O(log n) 复杂度的核心操作
public SkipListNode<T> insert(double score, T member);     // 插入元素
public boolean delete(double score, T member);              // 删除元素  
public List<SkipListNode<T>> getElementByRankRange(long start, long end);  // 按排名范围查询
public List<SkipListNode<T>> getElementByScoreRange(double min, double max); // 按分数范围查询
```

### 渐进式哈希表 (Dict) - 非阻塞扩容

**实现位置**: `site.hnfy258.internal.Dict`

**设计理念**:
- **非阻塞扩容**: 避免大数据量时的长时间阻塞，保证服务可用性
- **内存效率**: 逐步迁移数据，避免扩容时的内存使用峰值
- **高可用性**: 扩容期间持续提供服务，不影响正常操作
- **自适应**: 根据负载因子自动触发扩容和收缩

**核心机制**:
```java
private DictHashTable<K,V> ht0;  // 主哈希表
private DictHashTable<K,V> ht1;  // 扩容时的临时哈希表
private int rehashIndex;         // 渐进式 rehash 进度跟踪

// 每次操作时进行少量数据迁移，分摊扩容成本
private void rehashStep() {
    // 每次操作迁移1个桶的数据，避免单次阻塞
}
```

### SDS 字符串 (Simple Dynamic String) - 内存优化

**实现位置**: `site.hnfy258.internal.Sds`

**性能特性**:
- **O(1) 长度获取**: 预存储长度信息，避免遍历计算
- **二进制安全**: 支持任意二进制数据，包含空字符
- **减少内存分配**: 预分配策略和惰性释放，减少系统调用
- **内存对齐**: 优化内存访问模式，提升缓存命中率

**核心优化**:
```java
private byte[] bytes;    // 字符串内容
private int len;         // 已使用长度，O(1)获取
private int alloc;       // 分配的总长度

// 智能预分配策略
public Sds append(byte[] extra) {
    // 1. 小字符串(<1MB): 预分配2倍空间
    // 2. 大字符串(>=1MB): 预分配1MB额外空间
    // 3. 减少内存重分配次数，提升性能
}
```

### 数据结构时间复杂度

| 数据结构 | 插入 | 删除 | 查找 | 范围查询 |
|---------|------|------|------|----------|
| **SkipList** | O(log n) | O(log n) | O(log n) | O(log n + k) |
| **Dict** | O(1) | O(1) | O(1) | N/A |
| **SDS** | O(n) | O(1) | O(n) | N/A |


## 💾 持久化

### RDB 快照
- **触发方式**: `BGSAVE` 命令或配置定时保存
- **文件格式**: 二进制格式，紧凑高效
- **恢复速度**: 启动时快速加载
- **多数据类型支持**: String、List、Set、Hash、ZSet
- **数据库隔离**: 支持多数据库的独立保存和加载
- **文件完整性**: 内置头部验证和EOF标记
- **临时文件机制**: 支持主从复制的临时RDB生成

#### RDB 架构设计

**核心组件：**
- `RdbManager` - RDB管理器，统一协调读写操作
- `RdbWriter` - 数据写入器，支持二进制序列化
- `RdbLoader` - 数据加载器，支持二进制反序列化
- `RdbUtils` - 工具类，提供编码/解码基础功能
- `RdbConstants` - 常量定义，包含操作码和数据类型

**文件格式结构：**
```java
// RDB 文件格式
REDIS0009                    // 9字节头部标识
[SELECT-DB] [DB-ID]         // 数据库选择标记
[TYPE] [KEY] [VALUE]        // 数据条目
...                         // 更多数据条目
[EOF] [CHECKSUM]            // 结束标记和校验和
```

**数据类型编码：**
```java
public static final byte STRING_TYPE = (byte) 0;   // 字符串类型
public static final byte LIST_TYPE = (byte) 1;     // 列表类型  
public static final byte SET_TYPE = (byte) 2;      // 集合类型
public static final byte ZSET_TYPE = (byte) 3;     // 有序集合类型
public static final byte HASH_TYPE = (byte) 4;     // 哈希类型
public static final byte RDB_OPCODE_SELECTDB = (byte) 254;  // 数据库选择
public static final byte RDB_OPCODE_EOF = (byte) 255;       // 文件结束
```

**长度编码优化：**
- 小于64的数字用1字节编码
- 小于16384的数字用2字节编码  
- 更大的数字用4字节编码
- 有效减少文件大小

### AOF 日志
- **批量写入**: 异步批处理机制，提升写入性能
- **后台重写**: 支持 `BGREWRITEAOF` 命令，压缩AOF文件大小
- **缓冲区管理**: 重写期间的命令缓冲和应用机制
- **背压控制**: 智能背压处理，防止内存溢出
- **故障恢复**: 完整的崩溃恢复和数据一致性验证
- **性能优化**: 大命令阈值处理、可配置刷盘间隔
- **文件安全**: 原子文件替换、备份恢复机制
- **磁盘预分配**: 4MB预分配机制，减少文件碎片，提升写入性能

#### AOF 架构设计

**核心组件：**
- `AofManager` - AOF管理器，统一协调各组件
- `AofWriter` - 核心写入器，支持重写和缓冲
- `AofBatchWriter` - 批量写入器，异步批处理优化
- `AofLoader` - 文件加载器，支持大文件分块读取
- `AofUtils` - 工具类，支持多种数据类型的AOF序列化

**磁盘预分配机制：**
```java
// 预分配配置
private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;  // 4MB预分配

// 预分配流程
1. 文件创建时预先分配4MB磁盘空间
2. 写入操作在预分配空间内进行，避免频繁扩展
3. 空间不足时自动扩展，保持连续性
4. 文件关闭时截断到实际数据大小，释放多余空间
```

**预分配优势：**
- **减少磁盘碎片**: 连续空间分配，提升磁盘I/O效率
- **降低写入延迟**: 避免文件扩展的系统调用开销
- **优化批量写入**: 支持高频写入场景的性能需求
- **Kafka风格优化**: 参考Kafka的磁盘预分配设计模式

**性能特性：**
```java
// 批量写入配置
public static final int MIN_BATCH_SIZE = 16;          // 最小批处理大小
public static final int MAX_BATCH_SIZE = 50;          // 最大批处理大小
public static final int LARGE_COMMAND_THRESHOLD = 512 * 1024;  // 大命令阈值

// 背压控制
private static final int DEFAULT_BACKPRESSURE_THRESHOLD = 6 * 1024 * 1024;  // 6MB
private static final int DEFAULT_QUEUE_SIZE = 1000;    // 默认队列大小
```

**重写机制：**
- 后台线程执行，不阻塞正常操作
- 重写期间新命令缓冲到队列
- 原子文件替换，确保数据安全
- 支持重写失败后的自动恢复

## 🔄 主从复制

miniRedis 实现了完整的 Redis 主从复制机制，支持 PSYNC 协议、断线重连和命令传播，确保数据在多个节点间的一致性。

### 核心特性

- **5状态复制状态机**: DISCONNECTED → CONNECTING → SYNCING → STREAMING → ERROR
- **PSYNC协议**: 支持全量同步和部分重同步  
- **复制积压缓冲区**: 1MB环形缓冲区，支持断线重连时的增量同步
- **心跳监控**: 1秒间隔心跳检测，自动故障发现和重连
- **命令传播**: 实时传播写命令到所有从节点
- **偏移量管理**: 精确的复制偏移量跟踪和同步

### 架构设计

#### 核心组件

**节点管理：**
- `RedisNode` - 主从节点核心实现，管理连接和状态
- `NodeState` - 节点状态管理，维护主从关系和连接信息

**复制管理：**
- `ReplicationManager` - 复制协调器，处理全量/部分同步
- `ReplicationStateMachine` - 5状态复制状态机，管理状态转换
- `ReplicationHandler` - RESP协议复制消息处理器
- `ReplBackLog` - 1MB环形复制积压缓冲区

**心跳监控：**
- `HeartbeatManager` - 心跳管理器，监控连接健康状态
- `HeartbeatStats` - 心跳统计信息

### 复制流程

1. **连接建立**: 从节点连接主节点并发送 PSYNC 命令
2. **全量同步**: 主节点生成RDB快照并传输给从节点  
3. **增量同步**: 实时传播写命令到从节点
4. **部分重同步**: 断线重连时根据偏移量进行增量同步
5. **心跳监控**: 定期发送PING命令检测连接状态

### 配置示例

#### 启动主从集群
启动启动类 RedisMsLauncher
#### 断线重连测试

项目提供 `RedisMsLauncherWithReconnect` 用于测试断线重连功能：


### 技术特征

- **心跳间隔**: 1秒，失败3次触发重连
- **缓冲区大小**: 1MB环形缓冲区  
- **同步方式**: 全量RDB + 增量命令流
- **故障恢复**: 自动检测和重连

## ⚡ 性能基准

### AOF 持久化性能测试

> 📋 **测试说明**: 以下性能数据基于实际测试环境获得，测试代码位于 `AofExtremeCaseTest.java`

#### 高频写入性能测试

**测试场景**: 10万条SET命令连续写入
```
测试环境: 本地环境，SSD存储
命令类型: SET key{i} value{i} (i=0→100,000)
```

**性能指标**:
- **写入速度**: 414,938 命令/秒
- **总耗时**: 241ms
- **数据恢复**: 50ms恢复10万条记录
- **文件大小**: 4.3MB

#### 并发写入性能测试

**测试场景**: 10线程并发，每线程1000条命令
```
线程数: 10个并发线程
每线程命令数: 1,000条SET命令
总命令数: 10,000条
```

**性能指标**:
- **写入速度**: 86,957 命令/秒
- **总耗时**: 115ms
- **成功率**: 100% (10,000/10,000)
- **数据恢复**: 107ms恢复9,985条记录

#### 批量写入vs直接写入对比

**测试场景**: 同等数据量下的两种写入模式对比

| 写入模式 | 吞吐量 | 命令处理速率 | 写入时间 | 平均延迟 |
|---------|--------|--------------|----------|----------|
| **批量写入** | 52.42 MB/s | 1,041,667 命令/秒 | 96ms | 0.001ms |
| **直接写入** | 15.25 MB/s | 303,030 命令/秒 | 330ms | 0.003ms |

**性能提升**:
- **吞吐量提升**: 243.75% (批量写入相比直接写入)
- **延迟降低**: 75.49% (批量写入延迟更低)
- **平均批次大小**: 844条命令/批次

#### 崩溃恢复测试

**测试场景**: 模拟程序崩溃后的数据恢复能力
```
测试数据: 5,000条命令
崩溃模拟: 强制中断AOF写入过程
恢复测试: 重启后数据完整性验证
```

**恢复指标**:
- **恢复时间**: 8-14ms (5,000条命令)
- **数据完整性**: 99.86% (4,993/5,000条成功恢复)
- **恢复成功率**: 99.86%

#### 背压控制测试

**测试场景**: 高负载下的背压控制机制验证
```
背压触发阈值: 6MB
队列大小限制: 1,000条命令
测试负载: 持续高频写入
```

**背压指标**:
- **背压触发次数**: 38次
- **最大延迟**: 53.816ms (背压触发时)
- **平均延迟**: 0.613ms (背压状态下)
- **队列管理**: 自动调节写入速度，防止内存溢出

### AOF 优化机制

- **批处理**: 16-50条命令批量处理，减少系统调用
- **异步写入**: 专用写入线程，避免阻塞主线程
- **背压控制**: 6MB阈值，防止内存溢出
- **大命令优化**: 512KB以上命令直接写入，避免队列积压
- **定时刷盘**: 可配置刷盘间隔，平衡性能与安全性
## 👩‍💻 开发指南

### 项目结构
```
src/main/java/site/hnfy258/
├── aof/                    # AOF 持久化
│   ├── AofManager.java     # AOF 管理器
│   ├── AofLoader.java      # AOF 文件加载器
│   └── writer/             # 写入器组件
├── cluster/                # 主从复制
│   ├── RedisNode.java      # 节点管理
│   ├── ReplicationManager.java  # 复制管理器
│   └── heartbeat/          # 心跳监控
├── rdb/                    # RDB 快照
│   ├── RdbManager.java     # RDB 管理器
│   ├── RdbWriter.java      # RDB 写入器
│   └── RdbLoader.java      # RDB 加载器
├── command/                # 命令处理
├── dataStruct/             # 数据结构
├── server/                 # 服务器核心
└── util/                   # 工具类
```

### 构建与测试
```bash
# 编译项目
mvn clean compile

# 运行所有测试
mvn test

# 运行 AOF 性能测试
mvn test -Dtest=AofExtremeCaseTest

# 生成测试报告
mvn test -Dmaven.test.failure.ignore=true surefire-report:report
```

### 性能测试
项目包含完整的性能测试套件，位于 `src/test/java/site/hnfy258/aof/writer/AofExtremeCaseTest.java`：

- **高频写入测试**: 10万条命令连续写入
- **并发写入测试**: 多线程并发写入验证
- **批量优化测试**: 批量vs直接写入对比
- **崩溃恢复测试**: 数据完整性和恢复时间验证
- **背压控制测试**: 高负载下的背压机制验证

### 配置说明
```java
// AOF 配置参数
public static final int MIN_BATCH_SIZE = 16;          // 最小批量大小
public static final int MAX_BATCH_SIZE = 50;          // 最大批量大小
public static final int LARGE_COMMAND_THRESHOLD = 512 * 1024;  // 大命令阈值
public static final int DEFAULT_BACKPRESSURE_THRESHOLD = 6 * 1024 * 1024;  // 背压阈值

// 复制配置参数
public static final int HEARTBEAT_INTERVAL = 1000;    // 心跳间隔 (ms)
public static final int REPL_BACKLOG_SIZE = 1024 * 1024;  // 复制缓冲区大小
```

## 🛣️ 开发计划

### 近期目标
- [x] **完善 AOF 持久化** - 已实现批量写入、后台重写、故障恢复
- [x] **主从复制** - 已实现PSYNC协议、5状态机、断线重连机制
- [ ] 添加更多 Redis 命令
- [ ] 实现过期策略
- [ ] 性能测试和优化

### 长期目标
- [ ] 集群模式 (Redis Cluster)
- [ ] 哨兵模式
- [ ] 发布/订阅模式
- [ ] 流数据类型 (Stream)

## 🤝 贡献指南

我们欢迎所有形式的贡献：

1. **问题反馈**: 发现 bug 或有改进建议
2. **代码贡献**: 提交 Pull Request
3. **文档完善**: 改进文档和示例
4. **测试用例**: 添加更多测试场景


## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- Redis 官方团队提供的优秀设计理念
- Netty 团队提供的高性能网络框架
- 开源社区的宝贵建议和贡献

---

⭐ 如果这个项目对你有帮助，请给我一个 Star！

📧 联系方式: [2494946808@qq.com]

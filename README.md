# Redis Mini - 一个基于Java的Redis服务器实现

## 项目概览

Redis Mini 是一个用Java实现的Redis服务器，它忠实地复刻了Redis的核心功能和特性。是一个实现了五大基础数据结构，aof，rdb持久化，主从同步功能的Java版本redis服务端。

### 核心特性

* **完整的Redis命令支持**: 实现了Redis的主要命令，包括String、List、Hash、Set和Sorted Set等数据类型的操作。
* **持久化机制**: 支持RDB快照和AOF日志两种持久化方式，确保数据安全性。
* **主从复制**: 实现了Redis的主从复制协议，支持全量同步和增量同步。
* **高性能网络框架**: 基于Netty构建，支持高并发连接和请求处理。
* **模块化设计**: 项目被清晰地分为多个功能模块，每个模块都有其明确的职责和边界。

## 项目架构

Redis Mini 采用模块化架构，每个模块都专注于特定的功能领域：

* [**redis-common**](redis-common/redis-common.md): 提供核心数据结构和工具类，是整个项目的基石。
* [**redis-core**](redis-core/redis-core.md): 实现Redis的内存数据库模型和数据类型。
* [**redis-persistence**](redis-persistence/redis-persistence.md): 负责RDB和AOF两种持久化机制。
* [**redis-protocal**](redis-protocal/redis-protocal.md): 实现RESP（Redis序列化协议）的编解码。
* [**redis-replication**](redis-replication/redis-replication.md): 提供主从复制功能。
* [**redis-server**](redis-server/redis-server.md): 整合所有模块，提供完整的服务器实现。

## 快速开始

### 环境要求

* JDK 21


### 构建项目

```bash
# 克隆项目
git clone https://github.com/kachofugetsu09/redis-mini.git

# 进入项目目录
cd redis-mini

# 编译并打包
mvn clean package
```

### 运行服务器

```bash
# 进入server模块目录
cd redis-server/target

# 启动服务器
java -jar redis-server-1.0.0.jar
```

### 连接测试

使用标准的redis-cli或任何Redis客户端连接服务器：

```bash
redis-cli -p 6379
127.0.0.1:6379> SET mykey "Hello Redis Mini!"
OK
127.0.0.1:6379> GET mykey
"Hello Redis Mini!"
```

## 模块详解

每个模块都有其独立的文档，详细说明了该模块的设计理念、核心组件和实现细节：

* [**redis-common**](redis-common/redis-common.md): 核心数据结构模块
  * 高性能的动态字符串（SDS）实现
  * 线程安全的字典（Dict）实现
  * 高效的跳表（SkipList）实现

* [**redis-core**](redis-core/redis-core.md): 数据库核心模块
  * Redis数据类型的实现
  * 数据库管理和操作接口
  * 批量操作优化

* [**redis-persistence**](redis-persistence/redis-persistence.md): 持久化模块
  * RDB快照持久化
  * AOF日志持久化
  * 混合持久化支持

* [**redis-protocal**](redis-protocal/redis-protocal.md): 协议模块
  * RESP协议实现
  * 高效的编解码器
  * 命令解析和响应生成

* [**redis-replication**](redis-replication/redis-replication.md): 复制模块
  * 主从复制协议实现
  * 全量同步和增量同步
  * 复制积压缓冲区管理

* [**redis-server**](redis-server/redis-server.md): 服务器模块
  * 网络服务实现
  * 命令处理和执行
  * 服务器生命周期管理


## Benchmark测试
## Redis 性能百分比对比：Java MiniRedis vs. 官方 Redis 8.0

**（所有百分比表示：MiniRedis 性能是官方 Redis 的百分之多少）**
#### 测试命令：
```bash
./redis-benchmark.exe -h 127.0.0.1 -p 6379 -c 50 -n 50000 -t PING_INLINE,PING_BULK,SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP,ZADD,lpush:rand:new,LRANGE_1,LRANGE_3,LRANGE_5,LRANGE_10,MSET
```

#### 虚拟机参数
```
-Xms24G -Xmx24G -XX:+UseZGC  -XX:+ZProactive
```

| 命令        | 官方 Redis8 RPS (req/s) | MiniRedis RPS (req/s) | RPS 比例 (%) | 官方 Redis8 p50 (ms) | MiniRedis p50 (ms) | p50 比例 (%) | 官方 Redis8 p99 (ms) | MiniRedis p99 (ms) | p99 比例 (%) | 
| :---------- |:----------------------| :-------------------- | :----------- |:-------------------| :----------------- | :----------- |:-------------------| :----------------- | :----------- | 
| PING_INLINE | 68776                 | 48403                 | 70.4%        | 0.551              | 0.807              | 146.5%       | 0.895              | 1.295              | 144.7%       |
| SET         | 67204                 | 52138                 | 77.6%        | 0.551              | 0.695              | 126.1%       | 0.951              | 1.215              | 127.8%       | 
| GET         | 71531                 | 54585                 | 76.3%        | 0.471              | 0.639              | 135.7%       | 0.839              | 1.183              | 141.0%       | 
| INCR        | 72150                 | 45704                 | 63.3%        | 0.479              | 0.831              | 173.5%       | 0.871              | 1.559              | 179.0%       | 
| LPUSH       | 71839                 | 45413                 | 63.2%        | 0.519              | 0.871              | 167.8%       | 0.919              | 1.455              | 158.3%       | 
| RPUSH       | 73206                 | 45579                 | 62.3%        | 0.511              | 0.799              | 156.4%       | 0.839              | 1.455              | 173.4%       | 
| LPOP        | 72464                 | 50000                 | 69.0%        | 0.519              | 0.719              | 138.5%       | 0.879              | 1.263              | 143.7%       | 
| RPOP        | 63857                 | 52854                 | 82.8%        | 0.551              | 0.583              | 105.8%       | 1.271              | 1.095              | 86.1%        |
| SADD        | 71736                 | 41667                 | 58.1%        | 0.503              | 0.791              | 157.3%       | 0.895              | 1.527              | 170.6%       | 
| HSET        | 70822                 | 47393                 | 67.0%        | 0.527              | 0.783              | 148.6%       | 0.871              | 1.335              | 153.3%       | 
| SPOP        | 71942                 | 37707                 | 52.4%        | 0.471              | 0.671              | 142.5%       | 0.943              | 1.503              | 159.4%       | 
| ZADD        | 69541                 | 25164                 | 36.2%        | 0.535              | 0.647              | 120.9%       | 0.911              | 3.111              | 341.5%       | 
| MSET (10)   | 50607                 | 43745                 | 86.4%        | 0.791              | 0.567              | 71.7%        | 1.255              | 1.359              | 108.3%       | 
| **平均比例** |                       |                       | **67.4%**    |                    |                    | **139.3%**   |                    |                    | **168.2%**   | 


## 开发计划
- [ ] 重构和优化replication模块，具体存在的问题可以参考replication模块的文档
- [x] 升级到Java21，利用ZGC带来的性能提升解决benchmark中长尾延迟过长的问题
- [ ] 添加Redis Sentinel支持
- [ ] 实现Redis Cluster集群功能
- [ ] 实现更多的Redis命令


## 贡献指南

我们欢迎任何形式的贡献，包括但不限于：

* 提交bug报告
* 改进文档
* 提交新功能
* 优化性能

请确保在提交Pull Request之前：

1. 更新或添加相关的单元测试
2. 更新相关文档
3. 遵循项目的代码规范

## 许可证

本项目采用 [MIT 许可证](LICENSE) 进行许可。 

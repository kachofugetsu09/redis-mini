# Redis Mini - Protocol 协议模块

## 模块概览

`redis-protocal` 模块是 `redis-mini` 项目的通信神经中枢。它提供了对 Redis 序列化协议 (RESP, REdis Serialization Protocol) 的完整实现，负责将 Java 对象与网络字节流进行双向转换（即序列化与反序列化）。

本模块的设计目标是实现极高的性能、内存效率和对 RESP 规范的严格遵循，确保与任何标准 Redis 客户端的兼容性。

---

## 核心组件与特性

本模块的核心由两部分组成：RESP 数据类型的抽象封装，以及基于 Netty 的高效编解码器。

### 1. RESP 数据类型封装 (`protocal.*`)

通过一个抽象基类 `Resp`，派生出所有 RESP 数据类型的具体实现。

* **`SimpleString`**: 实现简单字符串 (`+OK\r\n`)。内部通过**享元模式 (Flyweight Pattern)** 缓存了如 `OK`, `PONG` 等常用响应，避免了不必要的对象创建。
* **`Errors`**: 实现错误类型 (`-ERR message\r\n`)，用于向客户端传递错误信息。
* **`RespInteger`**: 实现整数类型 (`:1000\r\n`)。同样运用了**享元模式**，缓存了从 -10 到 127 的常用整数，优化了性能。
* **`BulkString`**: 实现批量字符串 (`$6\r\nfoobar\r\n`)。这是最核心的数据类型之一，它基于 `redis-common` 模块的 `RedisBytes` 构建，支持**零拷贝**操作，并缓存了常用的命令字符串（如 `GET`, `SET`），极大地提升了性能。
* **`RespArray`**: 实现数组类型 (`*2\r\n...`)，用于表示客户端的命令和服务器的复合响应。它为 `NULL` 和 `EMPTY` 数组提供了静态常量实例，并能在编码时预估所需容量，减少 `ByteBuf` 的扩容开销。

### 2. Netty 编解码器 (`protocal.handler.*`)

* **`RespDecoder`**: Netty 的入站处理器 (Inbound Handler)，负责将 TCP 流中的字节解码成 `Resp` 对象。
    * **双协议支持**: 它不仅能处理标准的 RESP 格式，还兼容**内联命令 (Inline Commands)**（例如 `redis-cli` 中直接输入的 `PING` 或 `SET key value`）。这确保了服务器能与标准客户端无缝交互。
    * **流式处理**: 能够正确处理网络中数据包被分片（chunked）的情况，直到接收到完整的 RESP 消息才进行解码。

* **`RespEncoder`**: Netty 的出站处理器 (Outbound Handler)，负责将 `Resp` 对象编码成字节流写入网络。
    * **性能优化**: 通过预估消息大小来智能分配 `ByteBuf`，减少内存的动态扩展，从而提高网络写入效率。

## 设计原则

* **高性能与低开销**: 大量使用 `RedisBytes` 实现零拷贝；通过享元模式复用常用对象；利用 Netty 的 `ByteBuf` 池化能力减少内存分配和GC压力。
* **不可变性与线程安全**: 各种 `Resp` 对象在设计上是不可变的，这使得它们在 Netty 的多线程环境中可以被安全地传递和处理。
* **解耦**: 协议层完全独立，只负责字节与对象的转换。它不关心命令的业务逻辑，仅依赖 `redis-common` 模块获取 `RedisBytes` 等基础类型。

## 在项目中的角色

`redis-protocal` 模块是网络层和服务逻辑层之间的关键桥梁。

* 它被 `redis-server` 模块用于 Netty 的 `ChannelPipeline` 中，处理所有与客户端的通信。
* 它也被 `redis-persistence`（用于序列化AOF日志）和 `redis-replication`（用于同步命令）等模块使用，因为它们都需要以 RESP 格式来表示和处理命令。

## 如何构建与测试

该模块可以使用 Maven 独立进行构建和测试。

```bash
# 进入模块根目录
cd redis-protocal

# 运行测试并安装构建产物
mvn clean install
```
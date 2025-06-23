# Redis Mini - Server 服务模块

## 模块概览

`redis-server` 模块是 `redis-mini` 项目的顶层整合模块和应用的**主入口**。它将所有其他模块（`common`, `protocal`, `core`, `persistence`, `replication`）有机地整合在一起，构建出一个功能完整的、可运行的 Redis 服务器。

本模块的核心职责包括：
* **启动和管理服务器生命周期**：初始化所有服务，并监听网络端口。
* **网络服务**: 基于 Netty 构建高性能的网络通信层，负责接收客户端连接和数据。
* **命令处理**: 解析并分发客户端请求到具体的命令实现类。
* **功能协调**: 作为核心协调者，调用持久化和主从复制等高级功能。

---

## 核心组件与架构

`redis-server` 模块通过精巧的设计，将项目的各个部分粘合在一起。

### 1. 服务器启动与网络层 (`RedisMiniServer`)

* **`RedisMiniServer`**: 这是服务器的主实现类，它负责引导（Bootstrap）和启动 Netty 服务。
* **平台优化的网络模型**: 服务器能够智能检测操作系统环境，自动选择最高效的 I/O 模型，如在 Linux 环境下优先使用 `Epoll`，在 macOS 上使用 `KQueue`，在其他系统上则使用标准的 `NIO`。
* **Netty Pipeline**: `ChannelPipeline` 的配置清晰地展示了模块间的协作流程：
    1.  `RespDecoder`: （来自 `redis-protocal`）将入站的字节流解码为 `Resp` 对象。
    2.  `RespEncoder`: （来自 `redis-protocal`）将出站的 `Resp` 对象编码为字节流。
    3.  `RespCommandHandler`: （本模块核心）接收解码后的 `Resp` 对象，并执行相应的命令逻辑。

### 2. 命令处理引擎 (`command`包)

* **命令模式 (Command Pattern)**: 项目采用经典的命令模式来设计命令处理系统。每个 Redis 命令（如 `GET`, `SET`）都被实现为一个独立的类，并实现 `Command` 接口。
* **`CommandType` 枚举**: 这是一个命令工厂和注册中心。它通过 `findByBytes` 方法，能够高效地将客户端发来的命令字符串（如 "SET"）映射到具体的枚举类型，并能通过 `createCommand` 方法创建对应的命令对象实例。
* **`RespCommandHandler`**: 这是命令处理的入口。它接收 `RespArray` 对象，解析出命令和参数，通过 `CommandType` 工厂创建命令实例，最后调用命令的 `handle()` 方法来执行业务逻辑。对于写命令，它还会负责调用持久化和主从复制的相关接口。

### 3. 统一上下文 (`RedisContext`)

`RedisContext` 是整个 `redis-server` 模块架构的灵魂，也是解决模块间循环依赖的关键。

* **外观模式 (Facade Pattern)**: `RedisContext` 接口及其实现 `RedisContextImpl`，为所有上层组件（主要是命令实现类）提供了一个统一、稳定的服务接口。
* **依赖注入**: 所有的 `Command` 对象在创建时都会被注入一个 `RedisContext` 实例。这使得命令的实现变得非常简洁和清晰。
* **解耦**: `RedisContext` 极大地降低了模块间的耦合度。例如，`Set` 命令的实现类无需知道持久化的具体细节，它只需要在完成数据操作后，调用 `redisContext.writeAof()` 即可。上下文会自动处理后续的持久化逻辑。

## 在项目中的角色

`redis-server` 模块是整个项目的“组装者”和应用层。

* **依赖关系**: 它是唯一一个依赖所有其他模块的模块。
* **可执行产物**: 它通过 `maven-shade-plugin` 插件将所有模块和依赖打包成一个单一的可执行 JAR 文件，是项目的最终交付成果。
* **启动器**: 包含了 `RedisServerLauncher` 和 `RedisMsLauncher` 等多个启动类，用于以不同模式（单机、主从）启动服务器。

## 如何运行

1.  **构建项目**:
    在项目根目录下执行 Maven 命令，这将编译所有模块并生成一个可执行的 JAR 包到 `redis-server/target` 目录。

    ```bash
    mvn clean package
    ```

2.  **运行服务器**:
    进入 `redis-server` 模块的 `target` 目录，执行以下命令来启动服务器。

    ```bash
    java -jar redis-server-1.0.0.jar
    ```

    服务器将根据 `RedisServerLauncher` 中的默认配置启动。
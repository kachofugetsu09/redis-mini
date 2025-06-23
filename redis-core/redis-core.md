# Redis Mini - Core 核心模块

## 模块概览

`redis-core` 模块是 `redis-mini` 项目的引擎和逻辑核心。它负责实现 Redis 的内存数据库模型，管理所有数据类型的具体行为，并为上层服务（如命令执行、持久化、复制）提供统一、抽象的数据访问接口。

该模块承上启下，它基于 `redis-common` 提供的底层数据结构（如 `Dict`, `Sds`, `SkipList`），构建出面向用户的 Redis 数据类型（如 String, List, Hash, Set, ZSet），并由 `redis-server` 和 `redis-persistence` 等模块直接调用。

---

## 核心设计

`redis-core` 模块的架构设计清晰地分离了数据库管理和数据结构实现，确保了代码的高内聚和可扩展性。

### 1. 数据库层 (`database` & `core`)

这一层负责模拟 Redis 的多数据库（Multi-DB）模型。

* **`RedisCore` / `RedisCoreImpl`**: 这是数据库的顶层管理器。它内部持有一个 `RedisDB` 实例的数组，并管理当前操作的是哪个数据库。`SELECT` 命令的实现就依赖于此。为了保证线程安全，当前数据库索引 `currentDBIndex` 使用 `AtomicInteger` 来管理。
* **`RedisDB`**: 代表一个独立的数据库（如 `db0`, `db1`...）。其核心是一个 `Dict<RedisBytes, RedisData>` 实例，用于存储该数据库中所有的键值对。

### 2. 数据结构层 (`datastructure`)

这一层将 `redis-common` 中底层的、通用的数据结构，封装成符合 Redis 语义的高级数据类型。所有这些类型都实现了 `RedisData` 接口，具备超时管理和持久化转换的能力。

* **`RedisString`**: 封装了 `Sds`，提供了如 `incr` 等 Redis String 特有的操作。
* **`RedisList`**: 基于 `java.util.LinkedList` 实现，提供了 `lpush`, `rpop` 等双端队列操作。
* **`RedisHash`**: 内部封装了一个 `Dict`，实现了字段（field）到值（value）的映射。
* **`RedisSet`**: 内部同样封装 `Dict`，但所有值（value）都为一个固定的`PRESENT`对象，巧妙地利用 `Dict` 的键唯一性实现了集合（Set）的不重复特性。
* **`RedisZset` (有序集合)**: 这是最复杂的数据结构，它**同时使用** `Dict` 和 `SkipList` 两种结构来实现：
    * `Dict` 用于存储成员（member）到分数（score）的映射，这使得通过成员查找分数的复杂度为 O(1)。
    * `SkipList` 则按照分数对成员进行排序，保证了范围查询（如 `ZRANGE`）的效率为 O(log N)。

### 3. 批量操作优化

* **`RedisBatchOptimizer`**: 提供如 `batchSetStrings` 等批量操作的工具类。在处理 `MSET` 等命令时，它会预先创建所有 `RedisString` 对象，然后进行一次性的批量写入，这在高并发和大数据量场景下能有效减少锁竞争和上下文切换开销。

## 线程安全模型

本模块的设计严格遵循 Redis 的单线程命令处理模型。

* **写操作**: 所有的写命令（`put`, `delete` 等）都应由 `redis-server` 模块的单个命令执行线程来调用，从而天然地避免了写并发问题。
* **读操作**: 读操作（如 `get`、`keys`）以及后台任务（如RDB持久化）可以并发执行。线程安全性由 `redis-common` 模块中的 `Dict` 组件通过其**版本化快照**机制来保证。当后台线程需要读取数据时，它会获取一个数据的一致性快照，而不会影响主线程的正常写入。

## 在项目中的角色

`redis-core` 是连接上层服务与底层基础的桥梁。

* **上游**: `redis-server` 模块依赖它来执行具体的命令逻辑；`redis-persistence` 模块依赖它来获取全量数据以生成 RDB 或 AOF 重写文件。
* **下游**: 它依赖 `redis-common` 模块来获得高性能的 `Sds`, `Dict` 等基础构件，并依赖 `redis-protocal` 来定义 `convertToResp` 的返回格式。

## 如何构建与测试

该模块可以使用 Maven 独立进行构建和测试。

```bash
# 进入模块根目录
cd redis-core

# 运行测试并安装构建产物
mvn clean install
```
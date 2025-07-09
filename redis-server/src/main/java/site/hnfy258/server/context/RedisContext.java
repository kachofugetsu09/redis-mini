package site.hnfy258.server.context;

import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.raft.Raft;
import site.hnfy258.rdb.RdbManager;

import java.util.concurrent.CompletableFuture;

import java.util.Set;

/**
 * Redis服务器上下文接口，提供统一的系统功能访问入口。
 * 
 * <p>该接口作为Redis系统的核心抽象层，主要职责包括：
 * <ul>
 *   <li>提供数据存储和访问的统一接口
 *   <li>管理持久化操作（AOF和RDB）
 *   <li>处理主从复制和集群功能
 *   <li>维护服务器状态和配置
 *   <li>提供性能监控和统计
 *   <li>管理系统资源和内存
 * </ul>
 * 
 * <p>设计目标：
 * <ul>
 *   <li>解耦系统组件：通过上下文模式解决组件间的循环依赖
 *   <li>统一接口：为不同功能模块提供一致的访问方式
 *   <li>状态管理：集中管理服务器运行时状态
 *   <li>可扩展性：支持功能的灵活扩展
 *   <li>高性能：优化关键路径，减少锁竞争
 *   <li>可监控：提供丰富的监控指标
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用读写分离策略减少锁竞争
 *   <li>采用批量操作优化磁盘IO
 *   <li>实现智能的内存管理和回收
 *   <li>优化网络传输和序列化
 *   <li>支持异步操作和并行处理
 * </ul>
 * 
 * <p>并发控制：
 * <ul>
 *   <li>使用细粒度锁减少竞争
 *   <li>实现无锁数据结构
 *   <li>采用乐观并发控制
 *   <li>支持原子操作
 *   <li>保证事务隔离
 * </ul>
 * 
 * <p>使用建议：
 * <ul>
 *   <li>所有需要访问Redis功能的组件应该通过此接口而不是直接访问具体实现
 *   <li>实现类应该保证线程安全
 *   <li>避免在实现中产生新的循环依赖
 *   <li>合理使用异步操作提升性能
 *   <li>正确处理异常和资源清理
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
public interface RedisContext {
    
    // ========== 数据操作接口 ==========
    
    /**
     * 获取指定键的数据。
     * 
     * <p>该方法会：
     * <ul>
     *   <li>从当前选中的数据库中查找数据
     *   <li>检查数据是否过期
     *   <li>更新访问统计信息
     *   <li>触发键空间通知（如果启用）
     *   <li>更新LRU/LFU信息
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用无锁HashMap实现快速查找
     *   <li>采用延迟删除策略处理过期键
     *   <li>异步更新统计信息
     * </ul>
     * 
     * @param key 要查找的键
     * @return 对应的数据，如果键不存在或已过期则返回null
     * @throws IllegalArgumentException 如果key为null
     */
    RedisData get(RedisBytes key);
    
    /**
     * 存储键值对到当前数据库。
     * 
     * <p>该方法会：
     * <ul>
     *   <li>覆盖已存在的键
     *   <li>触发数据变更事件
     *   <li>同步到AOF（如果启用）
     *   <li>更新内存使用统计
     *   <li>检查内存限制
     * </ul>
     * 
     * <p>内存管理：
     * <ul>
     *   <li>根据maxmemory策略淘汰数据
     *   <li>更新内存使用统计
     *   <li>触发内存警告（如果配置）
     * </ul>
     * 
     * @param key 键
     * @param value 值
     * @throws IllegalArgumentException 如果key或value为null
     * @throws OutOfMemoryError 如果超出内存限制且无法淘汰
     */
    void put(RedisBytes key, RedisData value);
    
    /**
     * 获取当前数据库中的所有键。
     * 
     * <p>返回的集合：
     * <ul>
     *   <li>不包含已过期的键
     *   <li>是当前数据库的快照
     *   <li>不保证元素顺序
     * </ul>
     * 
     * <p>性能注意：
     * <ul>
     *   <li>该操作可能较慢，建议使用SCAN命令
     *   <li>返回结果占用额外内存
     *   <li>大数据量时可能阻塞服务器
     * </ul>
     * 
     * @return 当前数据库中所有有效键的集合
     */
    Set<RedisBytes> keys();
    
    /**
     * 切换到指定的数据库。
     * 
     * <p>注意事项：
     * <ul>
     *   <li>索引范围必须在[0, 数据库数量-1]之间
     *   <li>切换操作是线程安全的
     *   <li>切换后对数据的操作将在新数据库上进行
     *   <li>不影响其他客户端的选择
     * </ul>
     * 
     * <p>并发控制：
     * <ul>
     *   <li>使用读写锁保护数据库切换
     *   <li>确保原子性操作
     *   <li>维护每个客户端的数据库选择
     * </ul>
     * 
     * @param dbIndex 目标数据库索引
     * @throws IllegalArgumentException 如果索引超出范围
     */
    void selectDB(int dbIndex);
    
    /**
     * 获取当前数据库索引。
     * 
     * @return 当前选中的数据库索引（0-15）
     */
    int getCurrentDBIndex();
    
    /**
     * 获取系统配置的数据库总数。
     * 
     * @return 数据库总数
     */
    int getDBNum();
    
    /**
     * 清空所有数据库的数据。
     * 
     * <p>该操作会：
     * <ul>
     *   <li>删除所有数据库中的所有键值对
     *   <li>重置数据库状态
     *   <li>触发清空事件
     *   <li>同步到持久化系统
     * </ul>
     */
    void flushAll();
    
    // ========== 持久化接口 ==========
    
    /**
     * 将命令写入AOF日志。
     * 
     * <p>写入过程：
     * <ul>
     *   <li>追加到AOF缓冲区
     *   <li>根据同步策略决定是否立即刷盘
     *   <li>必要时触发AOF重写
     * </ul>
     * 
     * @param commandBytes 要记录的命令字节数组
     * @throws IllegalArgumentException 如果commandBytes为null
     */
    void writeAof(byte[] commandBytes);
    
    /**
     * 执行同步RDB保存操作。
     * 
     * <p>保存过程：
     * <ul>
     *   <li>创建RDB文件的临时副本
     *   <li>将当前数据集序列化到临时文件
     *   <li>原子性地替换旧的RDB文件
     * </ul>
     * 
     * @return 保存成功返回true，否则返回false
     */
    boolean saveRdb();
    
    /**
     * 异步执行RDB保存操作。
     * 
     * <p>该方法：
     * <ul>
     *   <li>在后台线程中执行保存操作
     *   <li>不会阻塞主线程
     *   <li>通过Future返回操作结果
     * </ul>
     * 
     * @return 异步操作的Future对象
     */
    CompletableFuture<Boolean> bgSaveRdb();
    
    /**
     * 从RDB文件加载数据。
     * 
     * <p>加载过程：
     * <ul>
     *   <li>验证RDB文件格式
     *   <li>清空现有数据
     *   <li>反序列化数据到内存
     *   <li>更新服务器状态
     * </ul>
     * 
     * @return 加载成功返回true，否则返回false
     */
    boolean loadRdb();
    
    /**
     * 创建用于主从复制的临时RDB快照。
     * 
     * <p>该方法：
     * <ul>
     *   <li>生成当前数据集的内存快照
     *   <li>不写入磁盘文件
     *   <li>用于主从复制的全量同步
     * </ul>
     * 
     * @return RDB格式的字节数组
     * @throws Exception 如果创建快照失败
     */
    byte[] createTempRdbForReplication();
    
    /**
     * 从字节数组加载RDB数据。
     * 
     * <p>主要用于：
     * <ul>
     *   <li>主从复制的全量同步
     *   <li>内存中的RDB导入
     *   <li>备份恢复
     * </ul>
     * 
     * @param rdbContent RDB格式的字节数组
     * @return 加载成功返回true，否则返回false
     * @throws IllegalArgumentException 如果rdbContent为null或格式无效
     */
    boolean loadRdbFromBytes(byte[] rdbContent);
    
    // ========== 集群复制接口 ==========
    
    /**
     * 将命令传播到从节点。
     * 
     * <p>传播过程：
     * <ul>
     *   <li>更新复制偏移量
     *   <li>写入复制积压缓冲区
     *   <li>异步发送到所有从节点
     * </ul>
     * 
     * @param commandBytes 要传播的命令字节数组
     */
    void propagateCommand(byte[] commandBytes);
    
    /**
     * 判断当前节点是否为主节点。
     * 
     * @return 如果是主节点返回true，从节点返回false
     */
    boolean isMaster();
    
    /**
     * 获取节点的唯一标识符。
     * 
     * @return 节点ID，未配置集群时返回null
     */
    String getNodeId();
    
    /**
     * 设置当前节点的集群信息。
     * 
     * <p>该操作会：
     * <ul>
     *   <li>更新节点角色
     *   <li>重置复制状态
     *   <li>触发必要的数据同步
     * </ul>
     * 
     * @param redisNode 新的节点信息
     */
    void setRedisNode(RedisNode redisNode);
    
    /**
     * 检查AOF持久化是否已启用。
     * 
     * @return AOF启用返回true，否则返回false
     */
    boolean isAofEnabled();
    
    /**
     * 检查RDB持久化是否已启用。
     * 
     * @return RDB启用返回true，否则返回false
     */
    boolean isRdbEnabled();
    
    // ========== 扩展功能接口 ==========
    
    /**
     * 执行AOF重写操作。
     * 
     * <p>重写过程：
     * <ul>
     *   <li>创建当前数据集的快照
     *   <li>生成最优命令序列
     *   <li>原子性地替换旧的AOF文件
     * </ul>
     * 
     * @return 重写成功返回true，否则返回false
     */
    boolean rewriteAof();
    
    /**
     * 获取当前节点的集群信息。
     * 
     * @return RedisNode实例，未配置集群时返回null
     */
    RedisNode getRedisNode();
    
    /**
     * 获取RDB管理器实例。
     * 
     * @return RDB管理器，未启用RDB时返回null
     */
    RdbManager getRdbManager();

    /**
     * 获取AOF管理器实例。
     * 
     * @return AOF管理器，未启用AOF时返回null
     */
    AofManager getAofManager();

    /**
     * 强制刷新AOF缓冲区到磁盘。
     * 
     * <p>该操作会：
     * <ul>
     *   <li>将缓冲区数据写入磁盘
     *   <li>执行fsync确保数据持久化
     *   <li>重置缓冲区状态
     * </ul>
     */
    void flushAof();
    
    /**
     * 获取服务器绑定的主机地址。
     * 
     * @return 主机地址（如"127.0.0.1"）
     */
    String getServerHost();
    
    /**
     * 获取服务器监听的端口号。
     * 
     * @return 端口号（如6379）
     */
    int getServerPort();

    // ========== 系统管理接口 ==========
    
    /**
     * 执行系统启动初始化。
     * 
     * <p>初始化过程：
     * <ul>
     *   <li>加载配置
     *   <li>初始化数据库
     *   <li>恢复持久化数据
     *   <li>启动后台任务
     * </ul>
     */
    void startup();
    
    /**
     * 执行系统关闭清理。
     * 
     * <p>清理过程：
     * <ul>
     *   <li>保存数据（如果配置要求）
     *   <li>关闭持久化系统
     *   <li>清理资源
     *   <li>停止后台任务
     * </ul>
     */
    void shutdown();
    
    /**
     * 检查系统是否正在运行。
     * 
     * @return 系统运行中返回true，否则返回false
     */
    boolean isRunning();

    /**
     * 获取所有数据库实例。
     * 
     * <p>注意：
     * <ul>
     *   <li>返回数组是只读的
     *   <li>不应直接修改返回的数据库实例
     *   <li>用于系统监控和调试
     * </ul>
     * 
     * @return 数据库实例数组
     */
    RedisDB[] getDataBases();

    /**
     * 获取Redis核心功能实现。
     * 
     * <p>该方法用于：
     * <ul>
     *   <li>访问底层数据结构操作
     *   <li>执行核心算法
     *   <li>系统监控和调试
     * </ul>
     * 
     * @return Redis核心实现实例
     */
    RedisCore getRedisCore();

    Raft getRaft();

    boolean isRaftEnabled();
}

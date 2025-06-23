package site.hnfy258.server;

import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.core.RedisCore;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.context.RedisContext;

/**
 * Redis服务器核心接口，定义了Redis服务器的基本行为和生命周期管理。
 * 
 * <p>该接口封装了Redis服务器的核心功能，包括：
 * <ul>
 *   <li>服务器的启动和停止
 *   <li>持久化管理（RDB和AOF）
 *   <li>集群节点管理
 *   <li>核心功能访问
 *   <li>服务器上下文管理
 * </ul>
 * 
 * <p>实现此接口的类需要确保：
 * <ul>
 *   <li>线程安全性：所有方法的实现都应该是线程安全的
 *   <li>资源管理：正确处理资源的分配和释放
 *   <li>状态一致性：维护服务器各组件间的状态一致
 *   <li>性能优化：关键操作应当采用异步处理和批量优化
 *   <li>监控指标：提供服务器运行状态的监控数据
 * </ul>
 * 
 * <p>性能考虑：
 * <ul>
 *   <li>命令处理应使用零拷贝和内存池优化
 *   <li>持久化操作应在后台线程执行
 *   <li>网络IO应采用高效的事件驱动模型
 *   <li>主从同步应支持断点续传
 *   <li>大量数据操作应支持渐进式处理
 * </ul>
 * 
 * <p>最佳实践：
 * <ul>
 *   <li>定期检查服务器状态和资源使用
 *   <li>实现优雅的服务降级机制
 *   <li>提供详细的错误日志和诊断信息
 *   <li>支持动态配置更新
 *   <li>实现数据一致性保护机制
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
public interface RedisServer {
    
    /**
     * 启动Redis服务器。
     * 
     * <p>该方法应该完成以下任务：
     * <ul>
     *   <li>初始化服务器配置
     *   <li>启动网络监听（支持IPv4/IPv6）
     *   <li>加载持久化数据（如果有）
     *   <li>初始化数据结构和内存池
     *   <li>启动后台任务（如过期键清理、AOF重写等）
     *   <li>初始化集群状态（如果启用）
     *   <li>设置信号处理器
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>采用多阶段并行启动
     *   <li>延迟加载非核心组件
     *   <li>预热关键数据结构
     * </ul>
     * 
     * @throws IllegalStateException 如果服务器已经在运行
     * @throws RuntimeException 如果启动过程中发生致命错误
     */
    void start();

    /**
     * 优雅停止Redis服务器。
     * 
     * <p>该方法应该完成以下任务：
     * <ul>
     *   <li>保存数据（如果配置要求）
     *   <li>停止接受新的连接
     *   <li>等待现有请求处理完成（带超时）
     *   <li>关闭网络连接
     *   <li>停止后台任务
     *   <li>释放资源（内存池、文件句柄等）
     *   <li>保存服务器状态
     * </ul>
     * 
     * <p>安全考虑：
     * <ul>
     *   <li>设置最大等待时间
     *   <li>保护数据一致性
     *   <li>记录关键操作日志
     * </ul>
     * 
     * @throws IllegalStateException 如果服务器未运行
     */
    void stop();

    /**
     * 获取RDB持久化管理器。
     * 
     * <p>RDB管理器负责：
     * <ul>
     *   <li>生成数据快照
     *   <li>压缩和优化存储格式
     *   <li>管理临时文件
     *   <li>处理快照加载
     *   <li>维护校验和
     * </ul>
     * 
     * @return RDB管理器实例，用于处理RDB格式的持久化操作
     * @throws IllegalStateException 如果RDB功能未启用
     */
    RdbManager getRdbManager();

    /**
     * 获取AOF持久化管理器。
     * 
     * <p>AOF管理器负责：
     * <ul>
     *   <li>追加命令日志
     *   <li>管理重写操作
     *   <li>处理fsync策略
     *   <li>恢复数据状态
     *   <li>优化文件大小
     * </ul>
     * 
     * @return AOF管理器实例，用于处理AOF格式的持久化操作
     * @throws IllegalStateException 如果AOF功能未启用
     */
    AofManager getAofManager();

    /**
     * 设置当前节点的主节点信息。
     * 
     * <p>在主从复制模式下，用于设置当前节点要同步的主节点。
     * 这个操作可能触发以下过程：
     * <ul>
     *   <li>断开与旧主节点的连接
     *   <li>建立与新主节点的连接
     *   <li>进行全量同步或增量同步
     *   <li>更新复制状态
     *   <li>调整内存使用
     * </ul>
     *
     * <p>性能优化：
     * <ul>
     *   <li>支持断点续传
     *   <li>增量同步优先
     *   <li>网络压缩传输
     *   <li>后台同步处理
     * </ul>
     *
     * @param masterNode 主节点信息，如果为null表示取消主从关系
     * @throws IllegalStateException 如果节点当前不支持复制操作
     */
    void setRedisNode(RedisNode masterNode);

    /**
     * 获取Redis核心功能实现。
     * 
     * <p>核心功能包括：
     * <ul>
     *   <li>数据结构操作
     *   <li>事务处理
     *   <li>键过期管理
     *   <li>内存回收
     *   <li>命令路由
     * </ul>
     * 
     * @return Redis核心功能实现实例，提供数据结构和命令的具体实现
     */
    RedisCore getRedisCore();

    /**
     * 获取当前Redis节点信息。
     * 
     * <p>节点信息包含：
     * <ul>
     *   <li>节点ID和角色
     *   <li>网络连接状态
     *   <li>复制进度
     *   <li>负载统计
     *   <li>性能指标
     * </ul>
     * 
     * @return 当前节点的信息，包含节点ID、角色等元数据
     */
    RedisNode getRedisNode();

    /**
     * 获取Redis服务器上下文。
     * 
     * <p>上下文信息包含：
     * <ul>
     *   <li>服务器配置
     *   <li>运行时状态
     *   <li>统计数据
     *   <li>客户端信息
     *   <li>系统资源使用
     * </ul>
     * 
     * @return Redis服务器上下文，包含服务器运行时的状态信息
     */
    RedisContext getRedisContext();
}

package site.hnfy258.server.config;

import lombok.Builder;
import lombok.Data;

/**
 * Redis服务器配置类，统一管理所有服务器配置参数。
 * 
 * <p>该类采用Builder模式设计，提供了灵活的配置创建方式。主要配置包括：
 * <ul>
 *   <li>网络配置：主机地址、端口、连接参数等
 *   <li>持久化配置：AOF/RDB开关、文件路径等
 *   <li>线程配置：各类线程池大小设置
 *   <li>数据库配置：数据库数量、内存限制等
 *   <li>复制配置：主从复制相关参数
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>使用Builder模式简化配置创建
 *   <li>提供合理的默认值
 *   <li>支持配置的动态更新
 *   <li>实现配置的持久化
 *   <li>提供预设配置模板
 * </ul>
 * 
 * <p>使用建议：
 * <ul>
 *   <li>优先使用预设配置模板
 *   <li>根据实际需求调整参数
 *   <li>注意参数之间的依赖关系
 *   <li>合理设置内存限制
 *   <li>定期检查配置有效性
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Data
@Builder
public class RedisServerConfig {
    
    // ========== 网络配置 ==========
    
    /**
     * 服务器监听地址。
     * 
     * <p>可选值：
     * <ul>
     *   <li>127.0.0.1：仅本机访问
     *   <li>0.0.0.0：允许所有网络访问
     *   <li>特定IP：限制特定网络访问
     * </ul>
     */
    @Builder.Default
    private String host = "127.0.0.1";
    
    /**
     * 服务器监听端口。
     * 
     * <p>注意事项：
     * <ul>
     *   <li>默认使用6379
     *   <li>建议使用1024以上的端口
     *   <li>避免使用系统保留端口
     *   <li>注意防火墙设置
     * </ul>
     */
    @Builder.Default
    private int port = 6379;
    
    /**
     * TCP连接队列大小。
     * 
     * <p>性能影响：
     * <ul>
     *   <li>影响并发连接处理能力
     *   <li>过小可能导致连接拒绝
     *   <li>过大可能浪费系统资源
     *   <li>建议根据并发量调整
     * </ul>
     */
    @Builder.Default
    private int backlogSize = 1024;
    
    /**
     * 接收缓冲区大小（字节）。
     * 
     * <p>调优建议：
     * <ul>
     *   <li>影响数据接收性能
     *   <li>考虑内存使用情况
     *   <li>根据数据包大小调整
     *   <li>注意系统限制
     * </ul>
     */
    @Builder.Default
    private int receiveBufferSize = 32 * 1024;
    
    /**
     * 发送缓冲区大小（字节）。
     * 
     * <p>调优建议：
     * <ul>
     *   <li>影响数据发送性能
     *   <li>考虑网络带宽情况
     *   <li>根据响应包大小调整
     *   <li>注意系统限制
     * </ul>
     */
    @Builder.Default
    private int sendBufferSize = 32 * 1024;
    
    // ========== 持久化配置 ==========
    
    /**
     * 是否启用AOF持久化。
     * 
     * <p>特点说明：
     * <ul>
     *   <li>提供更好的数据安全性
     *   <li>影响写入性能
     *   <li>占用更多磁盘空间
     *   <li>恢复速度较慢
     * </ul>
     */
    @Builder.Default
    private boolean aofEnabled = false;
    
    /**
     * AOF文件路径。
     * 
     * <p>配置建议：
     * <ul>
     *   <li>使用绝对路径
     *   <li>确保目录可写
     *   <li>考虑磁盘容量
     *   <li>定期备份文件
     * </ul>
     */
    @Builder.Default
    private String aofFileName = "redis.aof";
    
    /**
     * 是否启用RDB持久化。
     * 
     * <p>特点说明：
     * <ul>
     *   <li>提供数据快照功能
     *   <li>恢复速度较快
     *   <li>可能丢失部分数据
     *   <li>适合备份场景
     * </ul>
     */
    @Builder.Default
    private boolean rdbEnabled = false;
    
    /**
     * RDB文件路径。
     * 
     * <p>配置建议：
     * <ul>
     *   <li>使用绝对路径
     *   <li>确保目录可写
     *   <li>考虑磁盘容量
     *   <li>定期清理旧文件
     * </ul>
     */
    @Builder.Default
    private String rdbFileName = "dump.rdb";
    
    // ========== 线程配置 ==========
    
    /**
     * Boss线程组大小（接受连接）。
     * 
     * <p>调优建议：
     * <ul>
     *   <li>通常设置为1即可
     *   <li>多核系统可适当增加
     *   <li>注意避免资源浪费
     *   <li>监控线程使用情况
     * </ul>
     */
    @Builder.Default
    private int bossThreadCount = 1;
    
    /**
     * Worker线程组大小（处理I/O）。
     * 
     * <p>调优建议：
     * <ul>
     *   <li>建议为CPU核心数2倍
     *   <li>考虑业务复杂度
     *   <li>监控线程池状态
     *   <li>避免过度分配
     * </ul>
     */
    @Builder.Default
    private int workerThreadCount = Runtime.getRuntime().availableProcessors() * 2;
    
    /**
     * 命令执行器线程数。
     * 
     * <p>说明：
     * <ul>
     *   <li>保持Redis单线程模型
     *   <li>通常设置为1
     *   <li>特殊场景可增加
     *   <li>注意并发安全
     * </ul>
     */
    @Builder.Default
    private int commandExecutorThreadCount = 1;
    
    // ========== 数据库配置 ==========
    
    /**
     * 数据库数量。
     * 
     * <p>配置说明：
     * <ul>
     *   <li>默认16个数据库
     *   <li>按需合理配置
     *   <li>影响内存使用
     *   <li>注意性能影响
     * </ul>
     */
    @Builder.Default
    private int databaseCount = 16;
    
    /**
     * 最大内存限制（字节）。
     * 
     * <p>配置说明：
     * <ul>
     *   <li>0表示不限制
     *   <li>建议设置限制
     *   <li>预留系统内存
     *   <li>监控内存使用
     * </ul>
     */
    @Builder.Default
    private long maxMemory = 0L;
    
    // ========== 复制配置 ==========
    
    /**
     * 是否启用主从复制功能。
     * 
     * <p>功能说明：
     * <ul>
     *   <li>提供数据备份
     *   <li>支持读写分离
     *   <li>提高可用性
     *   <li>注意网络要求
     * </ul>
     */
    @Builder.Default
    private boolean replicationEnabled = true;
    
    /**
     * 复制缓冲区大小。
     * 
     * <p>配置说明：
     * <ul>
     *   <li>影响复制性能
     *   <li>考虑网络延迟
     *   <li>权衡内存使用
     *   <li>监控缓冲区状态
     * </ul>
     */
    @Builder.Default
    private int replicationBufferSize = 1024 * 1024; // 1MB
    
    // ========== 工厂方法 ==========
    
    /**
     * 创建默认配置。
     * 
     * <p>特点：
     * <ul>
     *   <li>使用默认参数
     *   <li>适合开发测试
     *   <li>最小化配置
     *   <li>快速启动
     * </ul>
     * 
     * @return 默认配置实例
     */
    public static RedisServerConfig defaultConfig() {
        return RedisServerConfig.builder().build();
    }
    
    /**
     * 创建开发环境配置。
     * 
     * <p>特点：
     * <ul>
     *   <li>本地访问限制
     *   <li>禁用AOF
     *   <li>启用RDB
     *   <li>适合开发调试
     * </ul>
     * 
     * @return 开发环境配置实例
     */
    public static RedisServerConfig developmentConfig() {
        return RedisServerConfig.builder()
                .host("127.0.0.1")
                .port(6379)
                .aofEnabled(false)
                .rdbEnabled(true)
                .build();
    }
    
    /**
     * 创建生产环境配置。
     * 
     * <p>特点：
     * <ul>
     *   <li>启用所有持久化
     *   <li>合理的内存限制
     *   <li>优化的线程配置
     *   <li>适合生产部署
     * </ul>
     * 
     * @return 生产环境配置实例
     */
    public static RedisServerConfig productionConfig() {
        return RedisServerConfig.builder()
                .host("0.0.0.0")
                .port(6379)
                .backlogSize(2048)
                .receiveBufferSize(64 * 1024)
                .sendBufferSize(64 * 1024)
                .aofEnabled(true)
                .rdbEnabled(true)
                .bossThreadCount(1)
                .workerThreadCount(Runtime.getRuntime().availableProcessors() * 2)
                .commandExecutorThreadCount(1)
                .databaseCount(16)
                .maxMemory(Runtime.getRuntime().maxMemory() * 3 / 4)  // 使用75%的JVM最大内存
                .replicationEnabled(true)
                .replicationBufferSize(8 * 1024 * 1024)  // 8MB
                .build();
    }
    
    /**
     * 验证配置参数的合法性
     * 
     * @throws IllegalArgumentException 如果配置参数无效
     */
    public void validate() {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("端口号必须在1-65535范围内");
        }
        
        if (databaseCount <= 0) {
            throw new IllegalArgumentException("数据库数量必须大于0");
        }
        
        if (bossThreadCount <= 0 || workerThreadCount <= 0 || commandExecutorThreadCount <= 0) {
            throw new IllegalArgumentException("线程数量必须大于0");
        }
        
        if (backlogSize <= 0 || receiveBufferSize <= 0 || sendBufferSize <= 0) {
            throw new IllegalArgumentException("缓冲区大小必须大于0");
        }
        
        if (aofEnabled && (aofFileName == null || aofFileName.trim().isEmpty())) {
            throw new IllegalArgumentException("启用AOF时必须指定AOF文件名");
        }
        
        if (rdbEnabled && (rdbFileName == null || rdbFileName.trim().isEmpty())) {
            throw new IllegalArgumentException("启用RDB时必须指定RDB文件名");
        }
    }
}

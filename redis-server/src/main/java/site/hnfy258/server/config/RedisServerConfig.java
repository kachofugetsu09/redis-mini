package site.hnfy258.server.config;

import lombok.Builder;
import lombok.Data;

/**
 * Redis服务器配置类
 * 
 * <p>统一管理Redis服务器的所有配置参数，包括网络设置、
 * 持久化配置、线程池配置等。支持Builder模式便于配置创建，
 * 提供合理的默认值确保开箱即用。
 * 
 * <p>主要配置分类：
 * <ul>
 *     <li>网络配置 - 主机地址、端口号、连接参数</li>
 *     <li>持久化配置 - AOF/RDB开关、文件路径</li>
 *     <li>线程配置 - 线程池大小、线程命名</li>
 *     <li>数据库配置 - 数据库数量、内存限制</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Data
@Builder
public class RedisServerConfig {
    
    // ========== 网络配置 ==========
    
    /** 服务器监听地址 */
    @Builder.Default
    private String host = "127.0.0.1";
    
    /** 服务器监听端口 */
    @Builder.Default
    private int port = 6379;
    
    /** TCP连接队列大小 */
    @Builder.Default
    private int backlogSize = 1024;
    
    /** 接收缓冲区大小（字节） */
    @Builder.Default
    private int receiveBufferSize = 32 * 1024;
    
    /** 发送缓冲区大小（字节） */
    @Builder.Default
    private int sendBufferSize = 32 * 1024;
    
    // ========== 持久化配置 ==========
    
    /** 是否启用AOF持久化 */
    @Builder.Default
    private boolean aofEnabled = true;
    
    /** AOF文件路径 */
    @Builder.Default
    private String aofFileName = "redis.aof";
    
    /** 是否启用RDB持久化 */
    @Builder.Default
    private boolean rdbEnabled = false;
    
    /** RDB文件路径 */
    @Builder.Default
    private String rdbFileName = "dump.rdb";
    
    // ========== 线程配置 ==========
    
    /** Boss线程组大小（接受连接） */
    @Builder.Default
    private int bossThreadCount = 1;
    
    /** Worker线程组大小（处理I/O） */
    @Builder.Default
    private int workerThreadCount = Runtime.getRuntime().availableProcessors() * 2;
    
    /** 命令执行器线程数（Redis单线程模型） */
    @Builder.Default
    private int commandExecutorThreadCount = 1;
    
    // ========== 数据库配置 ==========
    
    /** 数据库数量 */
    @Builder.Default
    private int databaseCount = 16;
    
    /** 最大内存限制（字节，0表示无限制） */
    @Builder.Default
    private long maxMemory = 0L;
    
    // ========== 复制配置 ==========
    
    /** 是否启用主从复制功能 */
    @Builder.Default
    private boolean replicationEnabled = true;
    
    /** 复制缓冲区大小 */
    @Builder.Default
    private int replicationBufferSize = 1024 * 1024; // 1MB
    
    // ========== 工厂方法 ==========
    
    /**
     * 创建默认配置
     * 
     * @return 默认配置实例
     */
    public static RedisServerConfig defaultConfig() {
        return RedisServerConfig.builder().build();
    }
    
    /**
     * 创建开发环境配置
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
     * 创建生产环境配置
     * 
     * @return 生产环境配置实例
     */
    public static RedisServerConfig productionConfig() {
        return RedisServerConfig.builder()
                .host("0.0.0.0")
                .port(6379)
                .aofEnabled(true)
                .rdbEnabled(true)
                .backlogSize(2048)
                .receiveBufferSize(64 * 1024)
                .sendBufferSize(64 * 1024)
                .workerThreadCount(Runtime.getRuntime().availableProcessors() * 4)
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

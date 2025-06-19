package site.hnfy258.rdb;

/**
 * RDB持久化相关常量定义
 * 
 * <p>定义了RDB文件格式、操作码、数据类型等常量，
 * 以及异步持久化相关的配置参数。统一管理所有RDB
 * 模块使用的常量，确保配置的一致性和可维护性。
 * 
 * <p>主要常量分类：
 * <ul>
 *     <li>RDB文件格式常量 - 文件名、操作码等</li>
 *     <li>数据类型标识常量 - 支持的Redis数据类型</li>
 *     <li>异步持久化配置 - 线程池和超时时间设置</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
public final class RdbConstants {
    
    // ========== RDB文件相关常量 ==========
    
    /** 默认RDB文件名 */
    public static final String RDB_FILE_NAME = "dump.rdb";
    
    /** RDB文件结束标记 */
    public static final byte RDB_OPCODE_EOF = (byte) 255;
    
    /** 数据库选择操作码 */
    public static final byte RDB_OPCODE_SELECTDB = (byte) 254;
    
    // ========== 数据类型常量 ==========
    
    /** 字符串类型标识 */
    public static final byte STRING_TYPE = (byte) 0;
    
    /** 列表类型标识 */
    public static final byte LIST_TYPE = (byte) 1;
      /** 集合类型标识 */
    public static final byte SET_TYPE = (byte) 2;
    
    /** 有序集合类型标识 */
    public static final byte ZSET_TYPE = (byte) 3;
    
    /** 哈希表类型标识 */
    public static final byte HASH_TYPE = (byte) 4;
    
    // ========== 异步持久化配置常量 ==========
    
    /** 后台保存线程池核心线程数 */
    public static final int BGSAVE_CORE_POOL_SIZE = 1;
      /** 后台保存线程池最大线程数 */
    public static final int BGSAVE_MAX_POOL_SIZE = 2;
    
    /** 后台保存任务队列大小 */
    public static final int BGSAVE_QUEUE_SIZE = 10;
    
    /** 后台保存线程空闲时间（秒） */
    public static final long BGSAVE_KEEP_ALIVE_TIME = 60L;
    
    /** 后台保存任务超时时间（毫秒） */
    public static final long BGSAVE_TIMEOUT_MS = 30000L;
      /** 快照创建超时时间（毫秒） */
    public static final long SNAPSHOT_TIMEOUT_MS = 30_000L;
    
    /** 文件写入超时时间（毫秒） */
    public static final long WRITE_TIMEOUT_MS = 300_000L;
    
    /**
     * 私有构造函数，防止工具类被实例化
     */
    private RdbConstants() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }
}

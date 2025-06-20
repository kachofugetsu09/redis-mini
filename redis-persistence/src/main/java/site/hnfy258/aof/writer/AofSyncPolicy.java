package site.hnfy258.aof.writer;

/**
 * AOF 刷盘策略枚举
 * 
 * <p>定义了 Redis AOF 持久化的三种刷盘策略，对应 Redis 的 appendfsync 配置选项。
 * 每种策略在性能和数据安全性之间提供不同的平衡。
 * 
 * <p>支持的策略：
 * <ul>
 *     <li>NO - 操作系统控制刷盘，性能最好但安全性最低</li>
 *     <li>ALWAYS - 每次写入立即刷盘，安全性最高但性能最低</li>
 *     <li>SMART - 智能批处理模式，平衡性能和安全性</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public enum AofSyncPolicy {
    /**
     * 不主动刷盘策略
     * 
     * <p>完全依赖操作系统的缓冲区刷新机制。
     * 性能最好，但在系统崩溃时可能丢失最近一段时间的数据。
     * 对应 Redis 的 appendfsync no 配置。
     */
    NO,
    
    /**
     * 立即刷盘策略
     * 
     * <p>每个写命令执行后立即将数据刷新到磁盘。
     * 提供最高级别的数据安全性，但会显著影响性能。
     * 对应 Redis 的 appendfsync always 配置。
     */
    ALWAYS,
    
    /**
     * 智能批处理策略
     * 
     * <p>基于主循环的批处理逻辑，动态决定刷盘时机：
     * <ul>
     *     <li>自动适应负载模式</li>
     *     <li>批量处理提高吞吐量</li>
     *     <li>智能平衡性能和数据安全</li>
     *     <li>避免传统定时刷盘的局限性</li>
     * </ul>
     */
    SMART
}

package site.hnfy258.aof.writer;


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
    

    EVERYSEC
}

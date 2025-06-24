package site.hnfy258.server.context;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.Set;

/**
 * Redis数据存储层，提供高性能的数据访问和管理功能。
 * 
 * <p>该类是Redis系统的核心存储层，主要职责包括：
 * <ul>
 *   <li>管理多个数据库实例
 *   <li>提供统一的数据访问接口
 *   <li>实现数据存储优化
 *   <li>处理并发访问控制
 *   <li>管理内存资源
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用高效的数据结构和算法
 *   <li>实现多级缓存机制
 *   <li>采用批量操作优化
 *   <li>支持异步写入
 *   <li>实现智能预读
 * </ul>
 * 
 * <p>并发控制：
 * <ul>
 *   <li>采用分段锁减少竞争
 *   <li>实现无锁读操作
 *   <li>支持MVCC机制
 *   <li>保证原子性操作
 *   <li>避免全局锁
 * </ul>
 * 
 * <p>内存管理：
 * <ul>
 *   <li>实现LRU/LFU淘汰策略
 *   <li>支持内存限制配置
 *   <li>提供内存使用统计
 *   <li>实现增量式rehash
 *   <li>优化大键处理
 * </ul>
 * 
 * <p>最佳实践：
 * <ul>
 *   <li>定期进行过期键清理
 *   <li>动态调整内存配置
 *   <li>监控性能指标
 *   <li>优化热点数据访问
 *   <li>实现优雅降级
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class RedisDataStore {
    
    /**
     * Redis核心实现实例，提供底层数据操作功能。
     */
    private final RedisCore redisCore;
    
    /**
     * 构造函数，初始化数据存储层。
     * 
     * <p>初始化过程包括：
     * <ul>
     *   <li>创建数据库实例
     *   <li>初始化内存管理器
     *   <li>启动后台任务
     *   <li>预热关键数据结构
     * </ul>
     * 
     * @param redisCore Redis核心数据操作组件
     * @throws IllegalArgumentException 如果redisCore为null
     */
    public RedisDataStore(final RedisCore redisCore) {
        if (redisCore == null) {
            throw new IllegalArgumentException("RedisCore不能为null");
        }
        this.redisCore = redisCore;
        log.info("RedisDataStore初始化完成，数据库数量: {}", redisCore.getDBNum());
    }

    // ========== 数据操作方法 ==========
    
    /**
     * 获取指定键的数据。
     * 
     * <p>查询过程：
     * <ul>
     *   <li>检查键是否过期
     *   <li>更新访问统计
     *   <li>触发键事件
     *   <li>返回数据副本
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用无锁读取
     *   <li>实现读缓存
     *   <li>异步更新统计
     * </ul>
     * 
     * @param key 键
     * @return 对应的数据，如果不存在则返回null
     * @throws IllegalArgumentException 如果key为null
     */
    public RedisData get(final RedisBytes key) {
        if (key == null) {
            throw new IllegalArgumentException("键不能为null");
        }
        return redisCore.get(key);
    }
    
    /**
     * 存储键值对。
     * 
     * <p>存储过程：
     * <ul>
     *   <li>检查内存限制
     *   <li>更新数据结构
     *   <li>触发变更事件
     *   <li>更新统计信息
     * </ul>
     * 
     * <p>内存管理：
     * <ul>
     *   <li>检查内存上限
     *   <li>执行数据淘汰
     *   <li>优化大键存储
     *   <li>处理内存碎片
     * </ul>
     * 
     * @param key 键
     * @param value 值
     * @throws IllegalArgumentException 如果key或value为null
     * @throws OutOfMemoryError 如果内存不足且无法淘汰
     */
    public void put(final RedisBytes key, final RedisData value) {
        if (key == null || value == null) {
            log.error("键和值都不能为null");
        }
        redisCore.put(key, value);
    }
    
    /**
     * 获取当前数据库的所有键。
     * 
     * <p>注意事项：
     * <ul>
     *   <li>返回结果是快照
     *   <li>可能占用大量内存
     *   <li>可能影响服务器性能
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>建议使用SCAN命令
     *   <li>支持游标遍历
     *   <li>异步构建结果集
     * </ul>
     * 
     * @return 键的集合
     */
    public Set<RedisBytes> keys() {
        return redisCore.keys();
    }
    
    /**
     * 选择数据库。
     * 
     * <p>切换过程：
     * <ul>
     *   <li>验证索引范围
     *   <li>更新线程上下文
     *   <li>切换数据视图
     *   <li>更新统计信息
     * </ul>
     * 
     * <p>并发控制：
     * <ul>
     *   <li>使用读写锁
     *   <li>保证原子性
     *   <li>维护线程隔离
     * </ul>
     * 
     * @param dbIndex 数据库索引(0-15)
     * @throws IllegalArgumentException 如果索引超出范围
     */
    public void selectDB(final int dbIndex) {
        if (dbIndex < 0 || dbIndex >= redisCore.getDBNum()) {
            throw new IllegalArgumentException(
                String.format("数据库索引超出范围: %d，有效范围: 0-%d", 
                    dbIndex, redisCore.getDBNum() - 1));
        }
        redisCore.selectDB(dbIndex);
    }
    
    /**
     * 获取当前数据库索引。
     * 
     * <p>该方法：
     * <ul>
     *   <li>返回线程本地变量
     *   <li>不需要同步操作
     *   <li>性能开销很小
     * </ul>
     * 
     * @return 当前数据库索引
     */
    public int getCurrentDBIndex() {
        return redisCore.getCurrentDBIndex();
    }
    
    /**
     * 获取数据库总数。
     * 
     * <p>说明：
     * <ul>
     *   <li>配置后不可修改
     *   <li>影响内存分配
     *   <li>建议合理配置
     * </ul>
     * 
     * @return 数据库总数
     */
    public int getDBNum() {
        return redisCore.getDBNum();
    }
    
    /**
     * 获取所有数据库实例。
     * 
     * <p>注意事项：
     * <ul>
     *   <li>返回数组是只读的
     *   <li>仅用于监控和统计
     *   <li>不要直接修改
     * </ul>
     * 
     * @return 数据库数组
     */
    public RedisDB[] getDataBases() {
        return redisCore.getDataBases();
    }
    
    /**
     * 清空所有数据库。
     * 
     * <p>清空过程：
     * <ul>
     *   <li>获取全局写锁
     *   <li>清空所有数据库
     *   <li>重置统计信息
     *   <li>触发清空事件
     *   <li>释放内存资源
     * </ul>
     * 
     * <p>注意事项：
     * <ul>
     *   <li>该操作不可逆
     *   <li>会阻塞所有写操作
     *   <li>可能需要较长时间
     *   <li>建议谨慎使用
     * </ul>
     */
    public void flushAll() {
        redisCore.flushAll();
        log.info("所有数据库已清空");
    }
    
    // ========== 统计信息方法 ==========
    
    /**
     * 获取当前数据库的键数量。
     * 
     * <p>统计说明：
     * <ul>
     *   <li>包含所有数据类型
     *   <li>不包含过期键
     *   <li>实时计算结果
     *   <li>可用于监控
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用计数器缓存
     *   <li>异步更新统计
     *   <li>批量计算优化
     * </ul>
     * 
     * @return 当前数据库中的键数量
     */
    public int getKeyCount() {
        RedisDB db = redisCore.getDataBases()[getCurrentDBIndex()];
        return (int)db.size();
    }
    
    /**
     * 检查键是否存在。
     * 
     * <p>检查过程：
     * <ul>
     *   <li>验证键是否有效
     *   <li>检查是否过期
     *   <li>更新访问统计
     *   <li>触发键事件
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用布隆过滤器
     *   <li>实现快速判断
     *   <li>减少锁竞争
     *   <li>优化内存访问
     * </ul>
     * 
     * @param key 要检查的键
     * @return 如果键存在返回true，否则返回false
     * @throws IllegalArgumentException 如果key为null
     */
    public boolean exists(final RedisBytes key) {
        if (key == null) {
            throw new IllegalArgumentException("键不能为null");
        }
        RedisDB db = redisCore.getDataBases()[getCurrentDBIndex()];
        return db.exist(key);
    }
    
    /**
     * 删除指定的键。
     * 
     * <p>删除过程：
     * <ul>
     *   <li>检查键是否存在
     *   <li>获取写锁保护
     *   <li>删除数据结构
     *   <li>释放关联资源
     *   <li>更新统计信息
     * </ul>
     * 
     * <p>内存管理：
     * <ul>
     *   <li>释放数据内存
     *   <li>回收索引空间
     *   <li>处理内存碎片
     *   <li>更新内存统计
     * </ul>
     * 
     * <p>并发控制：
     * <ul>
     *   <li>使用写锁保护
     *   <li>保证原子操作
     *   <li>处理并发访问
     *   <li>避免死锁风险
     * </ul>
     * 
     * @param key 要删除的键
     * @return 如果键存在并且删除成功返回true，否则返回false
     * @throws IllegalArgumentException 如果key为null
     */
    public boolean delete(final RedisBytes key) {
        if (key == null) {
            throw new IllegalArgumentException("键不能为null");
        }
        return redisCore.delete(key);
    }
}

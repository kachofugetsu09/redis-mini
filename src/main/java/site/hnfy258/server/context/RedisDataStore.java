package site.hnfy258.server.context;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.server.core.RedisCore;

import java.util.Set;

/**
 * Redis数据存储层
 * 
 * 负责管理数据库和数据操作，是解耦方案中的核心数据访问层。
 * 通过封装RedisCore的功能，提供统一的数据访问接口。
 * 
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class RedisDataStore {
    
    private final RedisCore redisCore;
    
    /**
     * 构造函数
     * 
     * @param redisCore Redis核心数据操作组件
     */
    public RedisDataStore(final RedisCore redisCore) {
        this.redisCore = redisCore;
        log.info("RedisDataStore初始化完成，数据库数量: {}", redisCore.getDBNum());
    }
    
    // ========== 数据操作方法 ==========
    
    /**
     * 获取指定键的数据
     * 
     * @param key 键
     * @return 对应的数据，如果不存在则返回null
     */
    public RedisData get(final RedisBytes key) {
        return redisCore.get(key);
    }
    
    /**
     * 存储键值对
     * 
     * @param key 键
     * @param value 值
     */
    public void put(final RedisBytes key, final RedisData value) {
        redisCore.put(key, value);
    }
    
    /**
     * 获取当前数据库的所有键
     * 
     * @return 键的集合
     */
    public Set<RedisBytes> keys() {
        return redisCore.keys();
    }
    
    /**
     * 选择数据库
     * 
     * @param dbIndex 数据库索引(0-15)
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
     * 获取当前数据库索引
     * 
     * @return 当前数据库索引
     */
    public int getCurrentDBIndex() {
        return redisCore.getCurrentDBIndex();
    }
    
    /**
     * 获取数据库总数
     * 
     * @return 数据库总数
     */
    public int getDBNum() {
        return redisCore.getDBNum();
    }
    
    /**
     * 获取所有数据库
     * 
     * @return 数据库数组
     */
    public RedisDB[] getDataBases() {
        return redisCore.getDataBases();
    }
    
    /**
     * 清空所有数据库
     */
    public void flushAll() {
        redisCore.flushAll();
        log.info("所有数据库已清空");
    }
    
    // ========== 统计信息方法 ==========
    
    /**
     * 获取当前数据库的键数量
     * 
     * @return 键的数量
     */
    public int getKeyCount() {
        return keys().size();
    }
    
    /**
     * 检查键是否存在
     * 
     * @param key 键
     * @return true如果键存在，false如果不存在
     */
    public boolean exists(final RedisBytes key) {
        return get(key) != null;
    }
    
    /**
     * 删除指定键
     * 
     * @param key 键
     * @return true如果删除成功，false如果键不存在
     */
    public boolean delete(final RedisBytes key) {
        if (exists(key)) {
            boolean success = redisCore.delete(key);
            return success;
        }
        return false;
    }
}

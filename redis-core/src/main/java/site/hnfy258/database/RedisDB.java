package site.hnfy258.database;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.internal.Dict;

import java.util.Set;

/**
 * Redis数据库实现类
 * 
 * <p>实现了单个Redis数据库的核心功能，每个Redis实例可以包含多个数据库。
 * 使用线程安全的Dict作为底层存储结构，支持高效的键值对操作。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>键值对的存储、获取和删除</li>
 *     <li>键的存在性检查和枚举</li>
 *     <li>数据库的清空操作</li>
 *     <li>数据库大小统计</li>
 *     <li>线程安全的并发访问</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
@Setter
public class RedisDB {

    /** 底层数据存储结构，使用线程安全的Dict */
    private final Dict<RedisBytes, RedisData> data;

    /** 数据库标识ID */
    private final int id;

    /**
     * 构造函数
     * 
     * @param id 数据库标识ID
     */
    public RedisDB(int id) {
        this.id = id;
        this.data = new Dict<>();
    }

    /**
     * 获取数据库中所有的键
     * 
     * @return 包含所有键的集合
     */
    public Set<RedisBytes> keys() {
        return data.keySet();
    }

    /**
     * 检查指定键是否存在
     * 
     * @param key 要检查的键
     * @return 如果键存在返回true，否则返回false
     */
    public boolean exist(RedisBytes key) {
        return data.containsKey(key);
    }

    /**
     * 存储键值对
     * 
     * @param key 键
     * @param value 值
     */
    public void put(RedisBytes key, RedisData value) {
        data.put(key, value);
    }

    /**
     * 获取指定键的值
     * 
     * @param key 要获取的键
     * @return 对应的值，如果键不存在则返回null
     */
    public RedisData get(RedisBytes key) {
        return data.get(key);
    }

    /**
     * 删除指定键的数据
     * 
     * @param key 要删除的键
     * @return 删除的值，如果键不存在则返回null
     */
    public RedisData delete(RedisBytes key) {
        return data.remove(key);
    }

    /**
     * 获取数据库中键值对的数量
     * 
     * @return 键值对数量
     */
    public long size() {
        return data.size();
    }

    /**
     * 清空数据库中的所有数据
     */
    public void clear() {
        data.clear();
    }
}

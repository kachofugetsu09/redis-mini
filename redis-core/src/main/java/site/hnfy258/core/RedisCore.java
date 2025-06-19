package site.hnfy258.core;

import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.Set;

/**
 * Redis核心操作接口
 * 
 * <p>定义了Redis核心数据操作的基本功能，包括多数据库管理、
 * 键值对操作和命令执行等核心能力。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>多数据库的选择和管理</li>
 *     <li>键值对的增删改查操作</li>
 *     <li>数据库级别的批量操作</li>
 *     <li>Redis命令的执行接口</li>
 *     <li>为持久化层提供数据访问能力</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public interface RedisCore {

    /**
     * 获取当前数据库中的所有键
     * 
     * @return 包含所有键的集合
     */
    Set<RedisBytes> keys();

    /**
     * 存储键值对到当前数据库
     * 
     * @param key 键
     * @param value 值
     */
    void put(RedisBytes key, RedisData value);

    /**
     * 从当前数据库获取指定键的值
     * 
     * @param key 要获取的键
     * @return 对应的值，如果键不存在则返回null
     */
    RedisData get(RedisBytes key);

    /**
     * 选择数据库
     * 
     * @param dbIndex 数据库索引
     */
    void selectDB(int dbIndex);

    /**
     * 获取数据库总数
     * 
     * @return 数据库数量
     */
    int getDBNum();

    /**
     * 获取当前选中的数据库索引
     * 
     * @return 当前数据库索引
     */
    int getCurrentDBIndex();

    /**
     * 获取所有数据库实例
     * 
     * @return 数据库实例数组
     */
    RedisDB[] getDataBases();

    /**
     * 清空所有数据库的数据
     */
    void flushAll();

    /**
     * 从当前数据库删除指定键
     * 
     * @param key 要删除的键
     * @return 如果键存在并被删除返回true，否则返回false
     */
    boolean delete(RedisBytes key);

    /**
     * 执行Redis命令
     * 
     * <p>用于AOF加载、RDB恢复等场景下的命令重放。
     * 
     * @param commandName 命令名称
     * @param args 命令参数
     * @return 如果命令执行成功返回true，否则返回false
     */
    boolean executeCommand(String commandName, String[] args);
}

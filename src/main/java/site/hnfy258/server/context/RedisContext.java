package site.hnfy258.server.context;

import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.core.RedisCore;

import java.util.Set;

/**
 * Redis系统统一上下文接口
 * 
 * 提供对数据存储、持久化、集群等功能的统一访问入口，
 * 解决RedisCore、RedisServer、RedisNode之间的循环依赖问题
 * 
 * @author hnfy258
 * @since 1.0
 */
public interface RedisContext {
    
    // ========== 数据操作接口 ==========
    
    /**
     * 获取指定键的数据
     * 
     * @param key 键
     * @return 对应的数据，如果不存在则返回null
     */
    RedisData get(RedisBytes key);
    
    /**
     * 存储键值对
     * 
     * @param key 键
     * @param value 值
     */
    void put(RedisBytes key, RedisData value);
    
    /**
     * 获取当前数据库的所有键
     * 
     * @return 键的集合
     */
    Set<RedisBytes> keys();
    
    /**
     * 选择数据库
     * 
     * @param dbIndex 数据库索引(0-15)
     */
    void selectDB(int dbIndex);
    
    /**
     * 获取当前数据库索引
     * 
     * @return 当前数据库索引
     */
    int getCurrentDBIndex();
    
    /**
     * 获取数据库总数
     * 
     * @return 数据库总数
     */
    int getDBNum();
    
    /**
     * 清空所有数据库
     */
    void flushAll();
    
    // ========== 持久化接口 ==========
    
    /**
     * 写入AOF日志
     * 
     * @param commandBytes 命令字节数组
     */
    void writeAof(byte[] commandBytes);
    
    /**
     * 执行RDB保存
     * 
     * @return 保存是否成功
     */
    boolean saveRdb();
    
    /**
     * 加载RDB文件
     * 
     * @return 加载是否成功
     */
    boolean loadRdb();
    
    /**
     * 创建用于复制的临时RDB数据
     * 
     * @return RDB字节数组
     */
    byte[] createTempRdbForReplication();
    
    /**
     * 从字节数组加载RDB数据
     * 
     * @param rdbContent RDB内容字节数组
     * @return 加载是否成功
     */
    boolean loadRdbFromBytes(byte[] rdbContent);
    
    // ========== 集群复制接口 ==========
    
    /**
     * 传播命令到从节点
     * 
     * @param commandBytes 命令字节数组
     */
    void propagateCommand(byte[] commandBytes);
    
    /**
     * 判断当前节点是否为主节点
     * 
     * @return true表示主节点，false表示从节点
     */
    boolean isMaster();
    
    /**
     * 获取节点ID
     * 
     * @return 节点ID，如果没有配置集群则返回null
     */
    String getNodeId();
    
    /**
     * 设置Redis节点（用于集群功能）
     * 
     * @param redisNode Redis节点实例
     */
    void setRedisNode(RedisNode redisNode);
    
    /**
     * 判断是否启用了AOF持久化
     * 
     * @return true表示启用AOF
     */
    boolean isAofEnabled();
    
    /**
     * 判断是否启用了RDB持久化
     * 
     * @return true表示启用RDB
     */
    boolean isRdbEnabled();
    
    // ========== 扩展功能接口 ==========
    
    /**
     * 执行AOF重写操作
     * 
     * @return 重写是否成功
     */
    boolean rewriteAof();
    
    /**
     * 获取关联的RedisNode
     * 
     * @return RedisNode实例，如果没有关联则返回null
     */
    RedisNode getRedisNode();

    
    /**
     * 获取底层的RdbManager实例（用于兼容性）
     * 
     * @return RdbManager实例，如果RDB未启用则返回null
     */
    RdbManager getRdbManager();

    /**
     * 获取底层的AofManager实例（用于兼容性）
     * 
     * @return AofManager实例，如果AOF未启用则返回null
     */
    AofManager getAofManager();

    /**
     * 刷新AOF缓冲区
     */
    void flushAof();
    
    /**
     * 获取服务器主机地址
     * 
     * @return 主机地址
     */
    String getServerHost();
    
    /**
     * 获取服务器端口
     * 
     * @return 端口号
     */
    int getServerPort();

    // ========== 系统管理接口 ==========
    
    /**
     * 系统启动初始化
     */
    void startup();
    
    /**
     * 系统关闭清理
     */
    void shutdown();
    
    /**
     * 获取系统运行状态
     * 
     * @return true表示系统正在运行
     */
    boolean isRunning();

    /**
     * 获取所有数据库
     * 
     * @return 数据库数组
     */
    RedisDB[] getDataBases();

    /**
     * 获取底层的RedisCore实例（用于兼容性）
     * 
     * @return RedisCore实例
     * @deprecated 请使用具体的数据操作方法代替直接访问RedisCore
     */
    @Deprecated
    RedisCore getRedisCore();
}

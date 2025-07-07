package site.hnfy258.core;

import site.hnfy258.core.command.CommandExecutor;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Redis核心数据操作实现类
 * 
 * <p>实现了RedisCore接口，提供完整的Redis数据库操作功能。
 * 支持多数据库管理、线程安全的数据操作和命令执行能力。
 * 
 * <h2>线程安全设计：</h2>
 * <ul>
 *     <li><strong>数据库选择</strong>：使用AtomicInteger确保currentDBIndex的线程安全</li>
 *     <li><strong>数据存储</strong>：依赖Dict的线程安全保证</li>
 *     <li><strong>多线程场景</strong>：支持AofLoader、RDB加载、正常客户端请求的并发操作</li>
 * </ul>
 * 
 * <h2>架构设计：</h2>
 * <ul>
 *     <li>支持命令执行器的依赖注入，实现模块间解耦</li>
 *     <li>提供统一的数据访问接口，屏蔽底层实现细节</li>
 *     <li>支持多数据库的动态切换和管理</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public class RedisCoreImpl implements RedisCore {

    /** 数据库列表，支持多数据库功能 */
    private final List<RedisDB> databases;
    
    /** 数据库总数量 */
    private final int dbNum;
    
    /** 当前选中的数据库索引，使用AtomicInteger保证线程安全 */
    private final AtomicInteger currentDBIndex = new AtomicInteger(0);
    
    /** 命令执行器，由上层模块注入，用于命令重放等场景 */
    private CommandExecutor commandExecutor;
    
    /** 快照锁，确保同一时间只有一个快照操作可以执行 */
    private final AtomicReference<String> snapshotLock = new AtomicReference<>(null);

    /**
     * 构造函数：初始化指定数量的数据库
     * 
     * @param dbNum 数据库数量，必须为正数
     */
    public RedisCoreImpl(final int dbNum) {
        this.dbNum = dbNum;
        this.databases = new java.util.ArrayList<>(dbNum);
        for (int i = 0; i < dbNum; i++) {
            databases.add(new RedisDB(i));
        }
    }

    /**
     * 获取当前数据库中的所有键
     * 
     * @return 当前数据库中所有键的集合
     */
    @Override
    public Set<RedisBytes> keys() {
        int dbIndex = getCurrentDBIndex();
        RedisDB db = databases.get(dbIndex);
        return db.keys();
    }

    /**
     * 向当前数据库存储键值对
     * 
     * @param key 键
     * @param value 值
     */
    @Override
    public void put(RedisBytes key, RedisData value) {
        RedisDB db = databases.get(getCurrentDBIndex());
        db.put(key, value);
    }

    /**
     * 从当前数据库获取指定键的值
     * 
     * @param key 要获取的键
     * @return 对应的值，如果键不存在则返回null
     */
    @Override
    public RedisData get(RedisBytes key) {
        RedisDB db = databases.get(getCurrentDBIndex());
        if (db.exist(key)) {
            return db.get(key);
        }
        return null;
    }

    /**
     * 线程安全的数据库选择方法
     * 
     * @param dbIndex 数据库索引
     * @throws RuntimeException 当数据库索引超出范围时抛出
     */
    @Override
    public void selectDB(int dbIndex) {
        if (dbIndex >= 0 && dbIndex < dbNum) {
            currentDBIndex.set(dbIndex);  // 原子设置
        } else {
            throw new RuntimeException("dbIndex out of range");
        }
    }

    /**
     * 获取数据库总数量
     * 
     * @return 数据库数量
     */
    @Override
    public int getDBNum() {
        return dbNum;
    }

    /**
     * 线程安全的获取当前数据库索引方法
     * 
     * @return 当前数据库索引
     */
    @Override
    public int getCurrentDBIndex() {
        return currentDBIndex.get();  // 原子读取
    }

    /**
     * 获取所有数据库实例
     * 
     * @return 包含所有数据库实例的数组
     */
    @Override
    public RedisDB[] getDataBases() {
        return databases.toArray(new RedisDB[0]);
    }

    /**
     * 清空所有数据库的数据
     */
    @Override
    public void flushAll() {
        for (RedisDB db : databases) {
            db.clear();
        }
    }

    /**
     * 从当前数据库删除指定键
     * 
     * @param key 要删除的键
     * @return 如果键存在并被删除返回true，否则返回false
     */
    @Override
    public boolean delete(RedisBytes key) {
        RedisDB db = databases.get(getCurrentDBIndex());
        if (db.exist(key)) {
            db.delete(key);
            return true;
        }
        return false;
    }

    /**
     * 执行Redis命令
     * 
     * <p>通过注入的命令执行器执行Redis命令，主要用于：
     * <ul>
     *     <li>AOF文件加载时的命令重放</li>
     *     <li>RDB恢复过程中的数据重建</li>
     *     <li>其他需要命令执行的场景</li>
     * </ul>
     * 
     * @param commandName 命令名称
     * @param args 命令参数
     * @return 如果命令执行成功返回true，否则返回false
     * @throws IllegalStateException 如果命令执行器未设置
     */
    @Override
    public boolean executeCommand(String commandName, String[] args) {
        if (commandExecutor == null) {
            throw new IllegalStateException("CommandExecutor not set. Please call setCommandExecutor first.");
        }
        return commandExecutor.executeCommand(commandName, args);
    }

    /**
     * 设置命令执行器
     * 
     * <p>由redis-server层注入具体的命令执行器实现，
     * 实现了依赖倒置原则。
     * 
     * @param commandExecutor 命令执行器实例
     */
    public void setCommandExecutor(CommandExecutor commandExecutor) {
        this.commandExecutor = commandExecutor;
    }
    
    /**
     * 尝试获取快照锁
     * 
     * <p>确保同一时间只有一个快照操作（AOF重写或RDB快照）可以执行。
     * 
     * @param snapshotType 快照类型（"AOF"或"RDB"）
     * @return 如果成功获取锁返回true，否则返回false
     */
    @Override
    public boolean tryAcquireSnapshotLock(String snapshotType) {
        if (snapshotType == null) {
            throw new IllegalArgumentException("快照类型不能为null");
        }
        return snapshotLock.compareAndSet(null, snapshotType);
    }
    
    /**
     * 释放快照锁
     * 
     * @param snapshotType 快照类型（"AOF"或"RDB"）
     */
    @Override
    public void releaseSnapshotLock(String snapshotType) {
        if (snapshotType == null) {
            return;
        }
        snapshotLock.compareAndSet(snapshotType, null);
    }
    
    /**
     * 检查当前是否有快照操作正在进行
     * 
     * @return 如果有快照操作正在进行返回true，否则返回false
     */
    @Override
    public boolean isSnapshotInProgress() {
        return snapshotLock.get() != null;
    }
    
    /**
     * 获取当前正在进行的快照类型
     * 
     * @return 当前快照类型，如果没有快照操作则返回null
     */
    @Override
    public String getCurrentSnapshotType() {
        return snapshotLock.get();
    }
}

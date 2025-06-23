package site.hnfy258.server.context;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.command.executor.CommandExecutorImpl;
import site.hnfy258.server.config.RedisServerConfig;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis系统统一上下文实现类
 * 
 * 通过组合模式聚合各个功能模块，提供统一的访问接口，
 * 解决组件间的循环依赖问题。集成了数据存储层和持久化层。
 * 
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class RedisContextImpl implements RedisContext {
    
    // ========== 分层组件 ==========
    private final RedisDataStore dataStore;
    private final RedisPersistence persistence;
    private RedisNode redisNode;
    
    // ========== 原有组件（用于兼容性） ==========
    private final RedisCore redisCore;
    private AofManager aofManager;
    private RdbManager rdbManager;
    
    // ========== 服务器配置 ==========
    private final String serverHost;
    private final int serverPort;
    private final RedisServerConfig config;
    
    // ========== 系统状态 ==========
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    /**
     * 新的构造函数，接收RedisServerConfig
     * 
     * @param redisCore Redis核心数据操作组件
     * @param serverHost 服务器主机地址
     * @param serverPort 服务器端口号
     * @param config Redis服务器配置
     */
    public RedisContextImpl(final RedisCore redisCore,
                          final String serverHost,
                          final int serverPort,
                          final RedisServerConfig config) {
        this.redisCore = redisCore;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.config = config;
        
        // 1. 初始化分层组件
        this.dataStore = new RedisDataStore(redisCore);
        
        // 2. 设置命令执行器（移到前面）
        if (redisCore instanceof RedisCoreImpl) {
            CommandExecutorImpl commandExecutor = new CommandExecutorImpl(this);
            ((RedisCoreImpl) redisCore).setCommandExecutor(commandExecutor);
        }
        
        // 3. 初始化持久化组件
        initializePersistence();
        
        // 4. 初始化持久化管理层
        this.persistence = new RedisPersistence(aofManager, rdbManager);
        
        log.info("RedisContext初始化完成 - AOF:{}, RDB:{}, 服务器:{}:{}", 
                aofManager != null ? "启用" : "禁用",
                rdbManager != null ? "启用" : "禁用",
                serverHost, serverPort);
    }
    
    /**
     * 初始化持久化组件
     */
    private void initializePersistence() {
        try {
            // 初始化AOF
            if (config.isAofEnabled()) {
                this.aofManager = new AofManager(config.getAofFileName(), redisCore);
                aofManager.load();
                Thread.sleep(500);
                log.info("AOF持久化组件初始化成功: {}", config.getAofFileName());
            }
            
            // 初始化RDB
            if (config.isRdbEnabled()) {
                this.rdbManager = new RdbManager(redisCore, config.getRdbFileName());
                boolean success = rdbManager.loadRdb();
                if (!success) {
                    log.warn("RDB文件加载失败，可能是文件不存在或格式错误: {}", config.getRdbFileName());
                } else {
                    log.info("RDB持久化组件初始化成功: {}", config.getRdbFileName());
                }
                Thread.sleep(500);
            }
        } catch (Exception e) {
            log.error("持久化组件初始化失败", e);
            throw new RuntimeException("持久化组件初始化失败", e);
        }
    }
    
    /**
     * 设置Redis节点（用于集群功能）
     * 
     * @param redisNode Redis节点实例
     */
    public void setRedisNode(RedisNode redisNode) {
        this.redisNode = redisNode;
        log.info("RedisContext关联节点: {}", 
                redisNode != null ? redisNode.getNodeId() : "null");
    }
      // ========== 数据操作实现 ==========
    
    @Override
    public RedisData get(final RedisBytes key) {
        return dataStore.get(key);
    }
    
    @Override
    public void put(final RedisBytes key, final RedisData value) {
        dataStore.put(key, value);
    }
    
    @Override
    public Set<RedisBytes> keys() {
        return dataStore.keys();
    }
    
    @Override
    public void selectDB(final int dbIndex) {
        dataStore.selectDB(dbIndex);
    }
    
    @Override
    public int getCurrentDBIndex() {
        return dataStore.getCurrentDBIndex();
    }    
    @Override
    public int getDBNum() {
        return dataStore.getDBNum();
    }
    
    @Override
    public void flushAll() {
        dataStore.flushAll();
    }
    
    // ========== 持久化实现 ==========
    
    @Override
    public void writeAof(final byte[] commandBytes) {
        persistence.writeAof(commandBytes);
    }
      @Override
    public boolean saveRdb() {
        return persistence.saveRdb();
    }
    
    @Override
    public CompletableFuture<Boolean> bgSaveRdb() {
        return persistence.bgSaveRdb();
    }
    @Override
    public boolean loadRdb() {
        return persistence.loadRdb();
    }
    
    @Override
    public byte[] createTempRdbForReplication() {
        return persistence.createTempRdbForReplication();
    }
    
    @Override
    public boolean loadRdbFromBytes(final byte[] rdbContent) {
        return persistence.loadRdbFromBytes(rdbContent);
    }
    
    // ========== 集群复制实现 ==========
    
    @Override
    public void propagateCommand(byte[] commandBytes) {
        if (redisNode != null && redisNode.isMaster()) {
            try {
                redisNode.propagateCommand(commandBytes);
            } catch (Exception e) {
                log.error("命令传播失败", e);
            }
        }
    }
    
    @Override
    public boolean isMaster() {
        return redisNode != null && redisNode.isMaster();
    }
    
    @Override
    public String getNodeId() {
        return redisNode != null ? redisNode.getNodeId() : null;
    }
      @Override
    public boolean isAofEnabled() {
        return persistence.isAofEnabled();
    }
    
    @Override
    public boolean isRdbEnabled() {
        return persistence.isRdbEnabled();
    }
    
    // ========== 扩展功能实现 ==========    @Override
    public boolean rewriteAof() {
        if (persistence.isAofEnabled()) {
            // 1. 通过AofManager的Writer执行真正的AOF重写
            try {
                if (aofManager != null && aofManager.getAofWriter() != null) {
                    log.info("开始执行AOF重写操作");
                    return aofManager.getAofWriter().bgrewrite();
                } else {
                    log.warn("AOF重写失败：AofManager或AofWriter不可用");
                    return false;
                }
            } catch (Exception e) {
                log.error("AOF重写失败: {}", e.getMessage(), e);
                return false;
            }
        }
        
        log.warn("AOF重写失败：AOF未启用");
        return false;
    }
    
    @Override
    public RedisNode getRedisNode() {
        return redisNode;
    }
    
    @Override
    public RedisCore getRedisCore() {
        // 1. 直接返回底层RedisCore，避免反射
        return redisCore;
    }
    
    @Override
    public void flushAof() {
        if (persistence.isAofEnabled()) {
            persistence.flushAof();
        }
    }
      @Override
    public String getServerHost() {
        return serverHost;
    }

    @Override
    public int getServerPort() {
        return serverPort;
    }

    // ========== 系统管理实现 ==========
      @Override
    public void startup() {
        if (running.compareAndSet(false, true)) {
            log.info("RedisContext启动中...");
            
            // 1. 加载持久化数据
            boolean rdbLoaded = loadRdb();
            log.info("RDB加载结果: {}", rdbLoaded ? "成功" : "失败");
            
            // 2. 加载AOF数据（需要直接调用aofManager，因为persistence层没有load接口）
            if (aofManager != null) {
                try {
                    aofManager.load();
                    log.info("AOF加载完成");
                } catch (Exception e) {
                    log.error("AOF加载失败", e);
                }
            }
            
            log.info("RedisContext启动完成");
        }
    }
      @Override
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            log.info("RedisContext关闭中...");
            
            // 1. 关闭持久化组件
            persistence.shutdown();
            
            log.info("RedisContext关闭完成");
        }
    }
    
    @Override
    public boolean isRunning() {
        return running.get();
    }    @Override
    public RedisDB[] getDataBases() {
        if (redisCore != null) {
            return redisCore.getDataBases();
        }
        return new RedisDB[0];  // 返回空数组而不是 null，避免空指针异常
    }

    @Override
    public RdbManager getRdbManager() {
        return rdbManager;
    }

    @Override
    public AofManager getAofManager() {
        return aofManager;
    }    /**
     * 检查AofManager是否可用
     * 
     * @return true表示AofManager可用
     */
    public boolean isAofManagerAvailable() {
        return aofManager != null;
    }

    /**
     * 检查RdbManager是否可用
     * 
     * @return true表示RdbManager可用
     */
    public boolean isRdbManagerAvailable() {
        return rdbManager != null;
    }
}

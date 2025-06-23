package site.hnfy258.server.context;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.rdb.RdbManager;

import java.util.concurrent.CompletableFuture;

/**
 * Redis持久化管理器，提供统一的数据持久化服务。
 * 
 * <p>该类的主要职责：
 * <ul>
 *   <li>统一管理AOF和RDB两种持久化方式
 *   <li>提供数据持久化的高层抽象
 *   <li>处理持久化过程中的异常情况
 *   <li>管理持久化组件的生命周期
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>组件解耦：AOF和RDB功能可以独立开启或关闭
 *   <li>异常处理：统一的异常处理和日志记录
 *   <li>性能优化：支持同步和异步操作
 *   <li>可靠性：保证数据持久化的原子性和一致性
 * </ul>
 * 
 * <p>使用场景：
 * <ul>
 *   <li>定期数据快照（RDB）
 *   <li>实时命令同步（AOF）
 *   <li>数据恢复和迁移
 *   <li>主从复制数据传输
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class RedisPersistence {
    
    /** AOF持久化管理器 */
    private final AofManager aofManager;
    
    /** RDB持久化管理器 */
    private final RdbManager rdbManager;
    
    /** AOF功能启用状态 */
    private final boolean aofEnabled;
    
    /** RDB功能启用状态 */
    private final boolean rdbEnabled;
    
    /**
     * 创建持久化管理器实例。
     * 
     * <p>初始化过程：
     * <ul>
     *   <li>设置AOF和RDB管理器
     *   <li>确定功能启用状态
     *   <li>记录初始化日志
     * </ul>
     * 
     * @param aofManager AOF管理器，可以为null表示未启用AOF
     * @param rdbManager RDB管理器，可以为null表示未启用RDB
     */
    public RedisPersistence(final AofManager aofManager, final RdbManager rdbManager) {
        this.aofManager = aofManager;
        this.rdbManager = rdbManager;
        this.aofEnabled = (aofManager != null);
        this.rdbEnabled = (rdbManager != null);
        
        log.info("RedisPersistence初始化完成 - AOF: {}, RDB: {}", 
                aofEnabled ? "启用" : "禁用", 
                rdbEnabled ? "启用" : "禁用");
    }
    
    // ========== AOF持久化方法 ==========
    
    /**
     * 将命令写入AOF日志。
     * 
     * <p>写入过程：
     * <ul>
     *   <li>检查AOF功能是否启用
     *   <li>追加命令到AOF缓冲区
     *   <li>根据同步策略决定是否立即刷盘
     * </ul>
     * 
     * @param commandBytes 要记录的命令字节数组
     * @throws IllegalArgumentException 如果commandBytes为null
     */
    public void writeAof(final byte[] commandBytes) {
        if (!aofEnabled) {
            log.debug("AOF未启用，跳过写入操作");
            return;
        }
        try {
            aofManager.appendBytes(commandBytes);
            log.debug("AOF写入命令成功，字节数: {}", commandBytes.length);
        } catch (Exception e) {
            log.error("AOF写入失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 强制刷新AOF缓冲区到磁盘。
     * 
     * <p>刷新过程：
     * <ul>
     *   <li>检查AOF功能是否启用
     *   <li>将缓冲区数据写入磁盘
     *   <li>执行fsync确保持久化
     * </ul>
     */
    public void flushAof() {
        if (!aofEnabled) {
            return;
        }
        try {
            aofManager.flushBuffer();
            log.debug("AOF缓冲区已刷新");
        } catch (Exception e) {
            log.error("AOF刷新失败: {}", e.getMessage(), e);
        }
    }
    
    // ========== RDB持久化方法 ==========
    
    /**
     * 执行同步RDB保存操作。
     * 
     * <p>保存过程：
     * <ul>
     *   <li>检查RDB功能是否启用
     *   <li>创建临时RDB文件
     *   <li>序列化当前数据集
     *   <li>原子性替换旧文件
     * </ul>
     * 
     * @return 保存成功返回true，否则返回false
     */
    public boolean saveRdb() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过保存操作");
            return false;
        }
        
        try {
            rdbManager.saveRdb();
            log.info("RDB保存成功");
            return true;
        } catch (Exception e) {
            log.error("RDB保存失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 异步执行RDB保存操作（BGSAVE）。
     * 
     * <p>异步保存特点：
     * <ul>
     *   <li>在后台线程执行保存
     *   <li>不阻塞主线程
     *   <li>通过Future返回结果
     *   <li>支持取消操作
     * </ul>
     * 
     * @return 异步操作的Future对象
     */
    public CompletableFuture<Boolean> bgSaveRdb() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过BGSAVE操作");
            return CompletableFuture.completedFuture(false);
        }
        
        try {
            return rdbManager.bgSaveRdb();
        } catch (Exception e) {
            log.error("BGSAVE启动失败: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(false);
        }
    }
    
    /**
     * 从RDB文件加载数据。
     * 
     * <p>加载过程：
     * <ul>
     *   <li>检查RDB功能是否启用
     *   <li>验证RDB文件格式
     *   <li>清空现有数据
     *   <li>反序列化数据到内存
     * </ul>
     * 
     * @return 加载成功返回true，否则返回false
     */
    public boolean loadRdb() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过加载操作");
            return false;
        }
        
        try {
            final boolean success = rdbManager.loadRdb();
            if (success) {
                log.info("RDB加载成功");
            } else {
                log.warn("RDB加载失败，可能是文件不存在或格式错误");
            }
            return success;
        } catch (Exception e) {
            log.error("RDB加载失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 创建用于主从复制的临时RDB快照。
     * 
     * <p>快照生成过程：
     * <ul>
     *   <li>检查RDB功能是否启用
     *   <li>创建内存中的数据快照
     *   <li>序列化为RDB格式
     *   <li>不写入磁盘文件
     * </ul>
     * 
     * @return RDB格式的字节数组，失败返回null
     */
    public byte[] createTempRdbForReplication() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，无法创建复制数据");
            return null;
        }
        try {
            return rdbManager.createTempRdbForReplication();
        } catch (Exception e) {
            log.error("创建临时RDB失败: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 从字节数组加载RDB数据。
     * 
     * <p>主要用于：
     * <ul>
     *   <li>主从复制的全量同步
     *   <li>内存中的RDB导入
     *   <li>在线数据迁移
     * </ul>
     * 
     * @param rdbContent RDB格式的字节数组
     * @return 加载成功返回true，否则返回false
     * @throws IllegalArgumentException 如果rdbContent为null或空
     */
    public boolean loadRdbFromBytes(final byte[] rdbContent) {
        if (!rdbEnabled || rdbContent == null || rdbContent.length == 0) {
            log.debug("RDB未启用或数据为空，跳过加载操作");
            return false;
        }
        
        try {
            rdbManager.loadRdbFromBytes(rdbContent);
            log.info("从字节数组加载RDB成功，大小: {} bytes", rdbContent.length);
            return true;
        } catch (Exception e) {
            log.error("从字节数组加载RDB失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    // ========== 状态查询方法 ==========
    
    /**
     * 检查AOF持久化是否已启用。
     * 
     * @return AOF功能启用返回true，否则返回false
     */
    public boolean isAofEnabled() {
        return aofEnabled;
    }
    
    /**
     * 检查RDB持久化是否已启用。
     * 
     * @return RDB功能启用返回true，否则返回false
     */
    public boolean isRdbEnabled() {
        return rdbEnabled;
    }
    
    /**
     * 获取AOF管理器实例。
     * 
     * @return AOF管理器，未启用时返回null
     */
    public AofManager getAofManager() {
        return aofManager;
    }
    
    /**
     * 获取RDB管理器实例。
     * 
     * @return RDB管理器，未启用时返回null
     */
    public RdbManager getRdbManager() {
        return rdbManager;
    }
    
    // ========== 生命周期管理 ==========
    
    /**
     * 关闭持久化组件，释放相关资源。
     * 
     * <p>关闭过程：
     * <ul>
     *   <li>刷新并关闭AOF管理器
     *   <li>完成当前RDB操作
     *   <li>关闭文件句柄
     *   <li>释放系统资源
     * </ul>
     */
    public void shutdown() {
        log.info("开始关闭持久化组件");
        
        // 1. 关闭AOF管理器
        if (aofEnabled && aofManager != null) {
            try {
                aofManager.close();
                log.info("AOF管理器已关闭");
            } catch (Exception e) {
                log.error("关闭AOF管理器失败: {}", e.getMessage(), e);
            }
        }
        
        // 2. 关闭RDB管理器
        if (rdbEnabled && rdbManager != null) {
            try {
                rdbManager.close();
                log.info("RDB管理器已关闭");
            } catch (Exception e) {
                log.error("关闭RDB管理器失败: {}", e.getMessage(), e);
            }
        }
    }
}

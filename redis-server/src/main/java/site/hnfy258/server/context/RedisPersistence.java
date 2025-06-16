package site.hnfy258.server.context;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.rdb.RdbManager;

/**
 * Redis持久化层
 * 
 * 统一管理AOF和RDB持久化功能，是解耦方案中的持久化抽象层。
 * 提供统一的持久化接口，屏蔽底层AOF和RDB的具体实现细节。
 * 
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class RedisPersistence {
    
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final boolean aofEnabled;
    private final boolean rdbEnabled;
    
    /**
     * 构造函数
     * 
     * @param aofManager AOF管理器，可以为null表示未启用
     * @param rdbManager RDB管理器，可以为null表示未启用
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
     * 写入AOF日志
     * 
     * @param commandBytes 命令字节数组
     */
    public void writeAof(final byte[] commandBytes) {
        if (!aofEnabled) {
            log.debug("AOF未启用，跳过写入操作");
            return;
        }
          try {
            // 1. 直接使用AofManager的字节数组写入接口
            aofManager.appendBytes(commandBytes);
            log.debug("AOF写入命令成功，字节数: {}", commandBytes.length);
        } catch (Exception e) {
            log.error("AOF写入失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 强制刷新AOF缓冲区
     */
    public void flushAof() {
        if (!aofEnabled) {
            return;
        }
          try {
            // 直接使用AofManager的缓冲区刷新接口
            aofManager.flushBuffer();
            log.debug("AOF缓冲区已刷新");
        } catch (Exception e) {
            log.error("AOF刷新失败: {}", e.getMessage(), e);
        }
    }
    
    // ========== RDB持久化方法 ==========
    
    /**
     * 执行RDB保存
     * 
     * @return 保存是否成功
     */
    public boolean saveRdb() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过保存操作");
            return false;
        }
        
        try {
            // 1. 执行RDB保存操作
            rdbManager.saveRdb();
            log.info("RDB保存成功");
            return true;
        } catch (Exception e) {
            log.error("RDB保存失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 加载RDB文件
     * 
     * @return 加载是否成功
     */
    public boolean loadRdb() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过加载操作");
            return false;
        }
        
        try {
            // 1. 执行RDB加载操作
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
     * 创建用于复制的临时RDB数据
     * 
     * @return RDB字节数组，如果失败返回null
     */
    public byte[] createTempRdbForReplication() {
        if (!rdbEnabled) {
            log.debug("RDB未启用，无法创建复制数据");
            return null;
        }
          try {
            // 1. 创建临时RDB数据用于主从复制
            return rdbManager.createTempRdbForReplication();
        } catch (Exception e) {
            log.error("创建临时RDB失败: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 从字节数组加载RDB数据
     * 
     * @param rdbContent RDB内容字节数组
     * @return 加载是否成功
     */
    public boolean loadRdbFromBytes(final byte[] rdbContent) {
        if (!rdbEnabled || rdbContent == null || rdbContent.length == 0) {
            log.debug("RDB未启用或数据为空，跳过加载操作");
            return false;
        }
        
        try {
            // 1. 从字节数组加载RDB数据
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
     * 判断是否启用了AOF持久化
     * 
     * @return true表示启用AOF
     */
    public boolean isAofEnabled() {
        return aofEnabled;
    }
    
    /**
     * 判断是否启用了RDB持久化
     * 
     * @return true表示启用RDB
     */
    public boolean isRdbEnabled() {
        return rdbEnabled;
    }
    
    /**
     * 获取AOF管理器
     * 
     * @return AOF管理器实例，可能为null
     */
    public AofManager getAofManager() {
        return aofManager;
    }
    
    /**
     * 获取RDB管理器
     * 
     * @return RDB管理器实例，可能为null
     */
    public RdbManager getRdbManager() {
        return rdbManager;
    }
    
    // ========== 生命周期管理 ==========
    
    /**
     * 关闭持久化组件
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
        
        log.info("持久化组件关闭完成");
    }
}

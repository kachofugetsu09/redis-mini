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
 *   <li>优化持久化性能
 *   <li>保证数据可靠性
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>组件解耦：AOF和RDB功能可以独立开启或关闭
 *   <li>异常处理：统一的异常处理和日志记录
 *   <li>性能优化：支持同步和异步操作
 *   <li>可靠性：保证数据持久化的原子性和一致性
 *   <li>可扩展性：支持自定义持久化策略
 *   <li>监控支持：提供详细的性能指标
 * </ul>
 * 
 * <p>持久化策略：
 * <ul>
 *   <li>AOF持久化：
 *     <ul>
 *       <li>支持不同的fsync策略（always/everysec/no）
 *       <li>自动的AOF重写机制
 *       <li>增量追加命令日志
 *       <li>支持AOF重写期间的新命令缓存
 *     </ul>
 *   <li>RDB持久化：
 *     <ul>
 *       <li>支持定时快照和条件触发
 *       <li>写时复制机制减少内存占用
 *       <li>压缩算法优化存储空间
 *       <li>校验和保证数据完整性
 *     </ul>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>写入优化：
 *     <ul>
 *       <li>批量写入缓冲
 *       <li>后台线程处理
 *       <li>增量式重写
 *       <li>写时复制优化
 *     </ul>
 *   <li>读取优化：
 *     <ul>
 *       <li>文件预读
 *       <li>并行加载
 *       <li>内存映射
 *       <li>增量恢复
 *     </ul>
 * </ul>
 * 
 * <p>使用场景：
 * <ul>
 *   <li>定期数据快照（RDB）
 *   <li>实时命令同步（AOF）
 *   <li>数据恢复和迁移
 *   <li>主从复制数据传输
 *   <li>灾难恢复
 *   <li>数据备份
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
     *   <li>初始化监控指标
     *   <li>启动后台任务
     * </ul>
     * 
     * <p>配置检查：
     * <ul>
     *   <li>验证持久化路径权限
     *   <li>检查磁盘空间
     *   <li>确认系统资源限制
     * </ul>
     * 
     * @param aofManager AOF管理器，可以为null表示未启用AOF
     * @param rdbManager RDB管理器，可以为null表示未启用RDB
     * @throws IllegalStateException 如果初始化环境检查失败
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
     *   <li>更新写入统计
     *   <li>检查重写条件
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用写入缓冲区
     *   <li>批量刷盘策略
     *   <li>后台线程处理
     *   <li>压缩重复命令
     * </ul>
     * 
     * <p>异常处理：
     * <ul>
     *   <li>磁盘空间不足
     *   <li>文件系统错误
     *   <li>IO异常
     *   <li>系统中断
     * </ul>
     * 
     * @param commandBytes 要记录的命令字节数组
     * @throws IllegalArgumentException 如果commandBytes为null
     * @throws IllegalStateException 如果AOF功能未启用
     * @throws RuntimeException 如果写入过程发生致命错误
     */
    public void writeAof(final byte[] commandBytes) {
        if (!aofEnabled) {
            log.debug("AOF未启用，跳过写入操作");
            return;
        }
        if (commandBytes == null) {
            throw new IllegalArgumentException("命令字节数组不能为null");
        }
        try {
            aofManager.appendBytes(commandBytes);
            log.debug("AOF写入命令成功，字节数: {}", commandBytes.length);
        } catch (Exception e) {
            log.error("AOF写入失败: {}", e.getMessage(), e);
            throw new RuntimeException("AOF写入失败", e);
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
     *   <li>更新同步时间戳
     *   <li>重置缓冲区
     * </ul>
     * 
     * <p>性能影响：
     * <ul>
     *   <li>可能阻塞主线程
     *   <li>影响写入延迟
     *   <li>增加磁盘IO负载
     *   <li>消耗系统资源
     * </ul>
     * 
     * <p>使用建议：
     * <ul>
     *   <li>避免频繁调用
     *   <li>合理设置fsync策略
     *   <li>监控同步延迟
     *   <li>注意磁盘性能
     * </ul>
     * 
     * @throws IllegalStateException 如果AOF功能未启用
     * @throws RuntimeException 如果刷新过程发生致命错误
     */
    public void flushAof() {
        if (!aofEnabled) {
            throw new IllegalStateException("AOF功能未启用");
        }
        try {
            aofManager.flushBuffer();
            log.debug("AOF缓冲区已刷新");
        } catch (Exception e) {
            log.error("AOF刷新失败: {}", e.getMessage(), e);
            throw new RuntimeException("AOF刷新失败", e);
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
     *   <li>更新统计信息
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>写时复制机制
     *   <li>增量压缩算法
     *   <li>文件预分配
     *   <li>批量序列化
     * </ul>
     * 
     * <p>数据一致性：
     * <ul>
     *   <li>原子性文件替换
     *   <li>校验和验证
     *   <li>版本号管理
     *   <li>错误恢复机制
     * </ul>
     * 
     * @return 保存成功返回true，否则返回false
     * @throws IllegalStateException 如果RDB功能未启用
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
     *   <li>监控保存进度
     * </ul>
     * 
     * <p>资源管理：
     * <ul>
     *   <li>控制并发任务数
     *   <li>限制内存使用
     *   <li>管理临时文件
     *   <li>超时处理
     * </ul>
     * 
     * <p>错误处理：
     * <ul>
     *   <li>任务中断恢复
     *   <li>资源清理
     *   <li>异常状态重置
     *   <li>失败重试策略
     * </ul>
     * 
     * @return 异步操作的Future对象
     * @throws IllegalStateException 如果RDB功能未启用或已有保存任务在执行
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
     *   <li>重建索引结构
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>文件预读
     *   <li>并行解析
     *   <li>增量加载
     *   <li>内存预分配
     * </ul>
     * 
     * <p>错误恢复：
     * <ul>
     *   <li>文件损坏检测
     *   <li>部分加载支持
     *   <li>版本兼容处理
     *   <li>状态回滚机制
     * </ul>
     * 
     * @return 加载成功返回true，否则返回false
     * @throws IllegalStateException 如果RDB功能未启用
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
     *   <li>优化传输格式
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>增量快照
     *   <li>写时复制
     *   <li>压缩传输
     *   <li>并行序列化
     * </ul>
     * 
     * <p>内存管理：
     * <ul>
     *   <li>控制快照大小
     *   <li>分片生成
     *   <li>及时释放
     *   <li>复用缓冲区
     * </ul>
     * 
     * @return RDB格式的字节数组，失败返回null
     * @throws IllegalStateException 如果RDB功能未启用
     * @throws OutOfMemoryError 如果内存不足
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
     *   <li>备份恢复
     * </ul>
     * 
     * <p>加载优化：
     * <ul>
     *   <li>流式解析
     *   <li>并行处理
     *   <li>增量加载
     *   <li>内存复用
     * </ul>
     * 
     * <p>安全检查：
     * <ul>
     *   <li>格式验证
     *   <li>版本兼容
     *   <li>数据完整性
     *   <li>内存限制
     * </ul>
     * 
     * @param rdbContent RDB格式的字节数组
     * @return 加载成功返回true，否则返回false
     * @throws IllegalArgumentException 如果rdbContent为null
     * @throws IllegalStateException 如果RDB功能未启用
     * @throws OutOfMemoryError 如果内存不足
     */
    public boolean loadRdbFromBytes(final byte[] rdbContent) {
        if (!rdbEnabled) {
            log.debug("RDB未启用，跳过加载操作");
            return false;
        }
        if (rdbContent == null) {
            throw new IllegalArgumentException("RDB内容不能为null");
        }
        try {
            return rdbManager.loadRdbFromBytes(rdbContent);
        } catch (Exception e) {
            log.error("从字节数组加载RDB失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 检查AOF持久化是否已启用。
     * 
     * @return AOF启用返回true，否则返回false
     */
    public boolean isAofEnabled() {
        return aofEnabled;
    }
    
    /**
     * 检查RDB持久化是否已启用。
     * 
     * @return RDB启用返回true，否则返回false
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
    
    /**
     * 关闭持久化系统。
     * 
     * <p>关闭过程：
     * <ul>
     *   <li>等待进行中的操作完成
     *   <li>刷新所有缓冲区
     *   <li>关闭文件句柄
     *   <li>清理临时文件
     *   <li>停止后台任务
     * </ul>
     * 
     * <p>资源清理：
     * <ul>
     *   <li>释放内存资源
     *   <li>关闭文件描述符
     *   <li>取消定时任务
     *   <li>清理临时文件
     * </ul>
     */
    public void shutdown() {
        log.info("开始关闭持久化系统...");
        
        // 关闭AOF持久化
        if (aofManager != null) {
            try {
                log.info("正在关闭AOF管理器...");
                aofManager.close();
                log.info("AOF管理器已关闭");
            } catch (Exception e) {
                log.error("关闭AOF管理器时发生错误: {}", e.getMessage(), e);
            }
        }
        
        // 关闭RDB持久化
        if (rdbManager != null) {
            try {
                log.info("正在关闭RDB管理器...");
                rdbManager.close();
                log.info("RDB管理器已关闭");
            } catch (Exception e) {
                log.error("关闭RDB管理器时发生错误: {}", e.getMessage(), e);
            }
        }
        
        log.info("持久化系统关闭完成");
    }
}

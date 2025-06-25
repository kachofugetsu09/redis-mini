package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.loader.AofLoader;
import site.hnfy258.aof.writer.AofBatchWriter;
import site.hnfy258.aof.writer.AofSyncPolicy;
import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AOF 持久化管理器
 * 
 * <p>负责管理 Redis AOF 持久化的核心组件，包括命令追加、文件加载、
 * 以及资源管理等功能。基于 RedisCore 接口设计，实现与具体 Redis 实现的解耦。
 * 
 * <p>核心功能：
 * <ul>
 *     <li>命令追加 - 将 Redis 命令持久化到 AOF 文件</li>
 *     <li>文件加载 - 启动时加载 AOF 文件恢复数据</li>
 *     <li>批量写入 - 通过批处理提升写入性能</li>
 *     <li>资源管理 - 安全地管理文件和内存资源</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>批量处理 - 使用 AofBatchWriter 提升写入性能</li>
 *     <li>内存池化 - 使用 Netty 的 ByteBuf 池化分配器</li>
 *     <li>线程安全 - 使用锁机制保证并发安全</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
@Getter
@Setter
public class AofManager {
    /** AOF 文件写入器 */
    private Writer aofWriter;
    
    /** AOF 批量写入器 */
    private AofBatchWriter batchWriter;
    
    /** AOF 文件加载器 */
    private AofLoader aofLoader;
    
    /** AOF 文件名 */
    private String fileName;
    
    /** 文件是否存在标志 */
    private boolean fileExists;
    
    /** Redis 核心接口 */
    private final RedisCore redisCore;

    /** 写入锁，保证并发安全 */
    private final ReentrantLock writeLock = new ReentrantLock();

    /** ByteBuf 分配器，使用 Netty 的池化分配器 */
    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    /** 默认配置参数 */
    private static final int DEFAULT_FLUSH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_PREALLOCATE = true;

    /**
     * 新增构造函数：支持RedisCore的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param fileName AOF文件名
     * @param redisCore Redis核心接口，提供所有核心功能的访问接口
     * @throws Exception 初始化异常
     */
    public AofManager(final String fileName, final RedisCore redisCore) 
            throws Exception {
        this(fileName, new File(fileName).exists(), DEFAULT_PREALLOCATE, 
             DEFAULT_FLUSH_INTERVAL_MS, redisCore);
    }    /**
     * 新增构造函数：支持RedisCore的完整版本
     * 
     * @param fileName AOF文件名
     * @param fileExists 文件是否存在
     * @param preallocated 是否预分配
     * @param flushInterval 刷新间隔
     * @param redisCore Redis核心接口
     * @throws Exception 初始化异常
     */
    public AofManager(final String fileName, final boolean fileExists, 
                     final boolean preallocated, final int flushInterval,
                     final RedisCore redisCore) throws Exception {
        this.fileName = fileName;
        this.fileExists = fileExists;
        this.redisCore = redisCore;
        
        // 1. 初始化AOF相关组件
        this.aofWriter = new AofWriter(new File(fileName), preallocated, 
                                      flushInterval, null, this.redisCore);
        this.batchWriter = new AofBatchWriter(aofWriter,flushInterval);
        // 使用RedisCore创建AofLoader，确保数据加载到正确的数据库
        this.aofLoader = new AofLoader(fileName, this.redisCore);
        log.info("AofManager使用RedisCore接口模式初始化完成，文件: {}", fileName);
    }    /**
     * 向AOF文件写入字节数组命令
     * 用于Redis持久化层调用
     * 
     * @param commandBytes 命令字节数组
     * @throws IOException 写入失败时抛出
     * @throws IllegalArgumentException 参数无效时抛出
     */
    public void appendBytes(final byte[] commandBytes) throws IOException {
        // 1. 参数验证
        validateCommandBytes(commandBytes);
        
        // 2. 分配ByteBuf并写入数据
        final ByteBuf byteBuf = allocator.buffer(commandBytes.length);
        boolean committedToBatchWriter = false;

        try {
            byteBuf.writeBytes(commandBytes);
            
            // 3. 线程安全的写入操作
            writeLock.lock();
            try {
                batchWriter.write(byteBuf);
                committedToBatchWriter = true;
            } finally {
                writeLock.unlock();
            }
        } catch (final Exception e) {
            throw new IOException("Failed to write bytes to AOF file", e);
        } finally {
            // 4. 确保资源释放
            if (!committedToBatchWriter) {
                ReferenceCountUtil.release(byteBuf);
            }
        }
    }
    
    /**
     * 验证命令字节数组的有效性
     * 
     * @param commandBytes 待验证的命令字节数组
     * @throws IllegalArgumentException 参数无效时抛出
     */
    private void validateCommandBytes(final byte[] commandBytes) {
        if (commandBytes == null) {
            throw new IllegalArgumentException("Command bytes cannot be null");
        }
        if (commandBytes.length == 0) {
            throw new IllegalArgumentException("Command bytes cannot be empty");
        }
    }    
    
    /**
     * 强制刷新AOF缓冲区到磁盘
     * 用于Redis持久化层调用
     * 
     * @throws IOException 刷新失败时抛出
     */
    public void flushBuffer() throws IOException {
        writeLock.lock();
        try {
            if (batchWriter != null) {
                batchWriter.flush();
            }
        } catch (final Exception e) {
            throw new IOException("Failed to flush AOF buffer", e);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 向AOF文件写入RESP数组命令
     * 
     * @param respArray RESP数组对象
     * @throws IOException 写入失败时抛出
     * @throws IllegalArgumentException 参数无效时抛出
     */
    public void append(final RespArray respArray) throws IOException {
        // 1. 参数验证
        if (respArray == null) {
            throw new IllegalArgumentException("RespArray cannot be null");
        }
        
        // 2. 分配ByteBuf并编码
        final ByteBuf byteBuf = allocator.buffer();
        boolean committedToBatchWriter = false;

        try {
            respArray.encode(respArray, byteBuf);
            
            // 3. 线程安全的写入操作
            writeLock.lock();
            try {
                batchWriter.write(byteBuf);
                committedToBatchWriter = true;
            } finally {
                writeLock.unlock();
            }
        } catch (final Exception e) {
            throw new IOException("Failed to write RESP array to AOF file", e);
        } finally {
            // 4. 确保资源释放
            if (!committedToBatchWriter) {
                ReferenceCountUtil.release(byteBuf);
            }
        }
    }

    /**
     * 加载AOF文件中的数据到Redis核心
     * 
     * @throws RuntimeException AOF文件加载失败时抛出
     */
    public void load() {
        if (aofLoader == null) {
            throw new IllegalStateException("AOF loader is not initialized");
        }
        
        try {
            aofLoader.load();
            log.info("AOF文件加载完成: {}", fileName);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to load AOF file: " + fileName, e);
        }
    }

    /**
     * 关闭AOF管理器及其所有组件
     * 按照正确的顺序关闭各个组件，确保资源完全释放
     */
    public void close() throws Exception {
        log.debug("开始关闭AOF管理器...");
        
        Exception firstException = null;
        
        try {
            // 1. 先关闭批量写入器，停止接收新的写入请求
            if (batchWriter != null) {
                try {
                    log.info("正在关闭批量写入器...");
                    batchWriter.close();
                } catch (Exception e) {
                    log.error("关闭批量写入器时发生错误", e);
                    if (firstException == null) {
                        firstException = e;
                    }
                } finally {
                    batchWriter = null;
                }
            }
            
            // 2. 再关闭AOF写入器
            if (aofWriter != null) {
                try {
                    log.info("正在关闭AOF写入器...");
                    aofWriter.close();
                } catch (Exception e) {
                    log.error("关闭AOF写入器时发生错误", e);
                    if (firstException == null) {
                        firstException = e;
                    }
                } finally {
                    aofWriter = null;
                }
            }
            
            // 3. 最后关闭AOF加载器
            if (aofLoader != null) {
                try {
                    log.info("正在关闭AOF加载器...");
                    aofLoader.close();
                } catch (Exception e) {
                    log.error("关闭AOF加载器时发生错误", e);
                    if (firstException == null) {
                        firstException = e;
                    }
                } finally {
                    aofLoader = null;
                }
            }
            
            log.info("AOF管理器关闭完成");
            
        } catch (Exception e) {
            log.error("关闭AOF管理器时发生意外错误", e);
            if (firstException == null) {
                firstException = e;
            }
        }
        
        // 如果有异常发生，抛出第一个异常
        if (firstException != null) {
            throw new RuntimeException("关闭AOF管理器时出错", firstException);
        }
    }

    /**
     * 刷新AOF缓冲区到磁盘
     * 这是flushBuffer()方法的简化版本，保持向后兼容性
     * 
     * @throws IOException 刷新失败时抛出
     */
    public void flush() throws IOException {
        flushBuffer();
    }

}

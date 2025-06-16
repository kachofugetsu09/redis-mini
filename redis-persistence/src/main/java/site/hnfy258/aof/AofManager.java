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
import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
@Setter
public class AofManager {
    private Writer aofWriter;
    private AofBatchWriter batchWriter;
    private AofLoader aofLoader;
    private String fileName;
    private boolean fileExists;
      // 核心依赖：统一使用RedisCore接口
    private final RedisCore redisCore;

    ReentrantLock writeLock = new ReentrantLock();

    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    private static final int DEFAULT_FLUSH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_PREALLOCATE = true;    /**
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
        this.batchWriter = new AofBatchWriter(aofWriter, flushInterval);
        // 使用RedisCore创建AofLoader，确保数据加载到正确的数据库
        this.aofLoader = new AofLoader(fileName, this.redisCore);
        log.info("AofManager使用RedisCore接口模式初始化完成，文件: {}", fileName);
    }    /**
     * 向AOF文件写入字节数组命令
     * 用于Redis持久化层调用
     * 
     * @param commandBytes 命令字节数组
     * @throws IOException 写入失败时抛出
     */
    public void appendBytes(final byte[] commandBytes) throws IOException {
        if (commandBytes == null || commandBytes.length == 0) {
            return;
        }
        
        ByteBuf byteBuf = allocator.buffer(commandBytes.length);
        boolean committedToBatchWriter = false;

        try {
            byteBuf.writeBytes(commandBytes);
            writeLock.lock();
            try {
                batchWriter.write(byteBuf);
                committedToBatchWriter = true;
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            throw new IOException("Failed to write bytes to AOF file", e);
        } finally {
            if (!committedToBatchWriter) {
                ReferenceCountUtil.release(byteBuf);
            }
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
        } catch (Exception e) {
            throw new IOException("Failed to flush AOF buffer", e);
        } finally {
            writeLock.unlock();
        }
    }

    public void append(RespArray respArray) throws IOException {
        ByteBuf byteBuf = allocator.buffer();
        boolean committedToBatchWriter = false;

        try{
            respArray.encode(respArray, byteBuf);
            writeLock.lock();
            try{
                batchWriter.write(byteBuf);
                committedToBatchWriter = true;
            }finally {
                writeLock.unlock();
            }
        }catch(Exception e){
            throw new RuntimeException("Failed to write batch to AOF file", e);
        }finally {
            if(!committedToBatchWriter){
                ReferenceCountUtil.release(byteBuf);
            }
        }
    }

    public void load(){
        aofLoader.load();
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

    public void flush() throws Exception {
        if(batchWriter != null){
            batchWriter.flush();
        }
    }

}

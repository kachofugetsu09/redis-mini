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
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

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
    private RedisCore redisCore;

    ReentrantLock writeLock = new ReentrantLock();

    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    private static final int DEFAULT_FLUSH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_PREALLOCATE = true;


    public AofManager(String fileName,RedisCore redisCore) throws Exception {
        this(fileName,new File(fileName).exists(),DEFAULT_PREALLOCATE,DEFAULT_FLUSH_INTERVAL_MS,redisCore);
    }

    public AofManager(String fileName,boolean fileExists,boolean preallocated, int flushInterval, RedisCore redisCore) throws Exception {
        this.fileName = fileName;
        this.fileExists = fileExists;
        this.aofWriter = new AofWriter(new File(fileName), preallocated,flushInterval, null,redisCore);
        this.batchWriter = new AofBatchWriter(aofWriter,flushInterval);
        this.redisCore = redisCore;
        this.aofLoader = new AofLoader(fileName,redisCore);
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

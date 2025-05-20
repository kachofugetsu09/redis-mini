package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import site.hnfy258.aof.loader.AofLoader;
import site.hnfy258.aof.writer.AofBatchWriter;
import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
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
        writeLock.lock();
        try{
            ByteBuf byteBuf = Unpooled.buffer();
            try{
                respArray.encode(respArray, byteBuf);
                batchWriter.write(byteBuf);
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            }finally {
            writeLock.unlock();
        }
    }

    public void load(){
        aofLoader.load();
    }

    public void close() throws Exception {

        if(batchWriter != null){
            batchWriter.close();
        }
        if(aofWriter != null){
            aofWriter.close();
        }
        if(aofLoader != null){
            aofLoader.close();
        }
    }

    public void flush() throws Exception {
        if(batchWriter != null){
            batchWriter.flush();
        }
    }

}

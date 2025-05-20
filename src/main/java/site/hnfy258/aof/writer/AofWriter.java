package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.utils.AofUtils;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.internal.Dict;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
@Slf4j
public class AofWriter implements Writer{
    private File file;
    private FileChannel channel;
    private RandomAccessFile raf;
    private AtomicLong size;
    private boolean isPreallocated;
    private final AtomicLong realSize = new AtomicLong(0);
    private final AtomicBoolean rewriting = new AtomicBoolean(false);

    private RedisCore redisCore;

    private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;
    public AofWriter(File file, boolean preallocated, int flushInterval, FileChannel channel,RedisCore redisCore) throws FileNotFoundException {
        this.file = file;
        this.isPreallocated = preallocated;
        this.redisCore = redisCore;

        if(channel == null){
            this.raf = new RandomAccessFile(file,"rw");
            this.channel = raf.getChannel();
            channel = this.channel;
        }else{
            this.channel = channel;
        }

        try{
            this.size = new AtomicLong(channel.size());

            if(isPreallocated){
                preAllocated(DEFAULT_PREALLOCATE_SIZE);
            }

            this.channel.position(this.size.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void preAllocated(int defaultPreallocateSize) throws IOException {
        long currentSize = 0;
        try{
            currentSize = this.channel.size();
        }catch(IOException e){
            log.error("获取文件长度时发生错误",e);
        }
        long newSize = currentSize + defaultPreallocateSize;
        if(this.raf != null){
            this.raf.setLength(newSize);
        }
        else if(this.channel != null){
            this.channel.truncate(newSize);
        }

        this.channel.position(currentSize);
        this.realSize.set(currentSize);
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        int written = writtenFullyTo(channel,buffer);
        size.addAndGet(written);
        realSize.addAndGet(written);
        return written;
    }

    private int writtenFullyTo(FileChannel channel, ByteBuffer buffer) {
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();
        int totalBytes = buffer.remaining();

        try{
            int written = 0;
            while(written < totalBytes){
                written += channel.write(buffer);
            }
            return written;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        finally {
            buffer.position(originalPosition);
            buffer.limit(originalLimit);
        }
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
       try{
           if(channel !=null && channel.isOpen()){
               log.info("执行关闭前最后一次刷盘");
               flush();
               long currentSize = realSize.get();
               if(currentSize > 0){
                   channel.truncate(realSize.get());
                   log.info("AOF文件已截断到长度{}",realSize.get());
               }
           }

           channel.close();
           if(raf != null) raf.close();
       }catch(IOException e){
           log.error("关闭AOF文件时发生错误",e);
       }
    }

    @Override
    public boolean bgrewrite() throws IOException {
        if(rewriting.get()){
            log.warn("正在进行AOF重写，无法再次执行");
            return false;
        }
        rewriting.set(true);
        Thread rewriteThread = new Thread(this::rewriteTask);
        rewriteThread.start();
        return true;
    }

    private void rewriteTask() {
        try{
            log.info("开始重写aof");
            File rewriteFile = new File("rewrite.aof");
            FileChannel channel = new RandomAccessFile(rewriteFile,"rw").getChannel();
            channel.position(0);
            RedisDB[] dataBases = redisCore.getDataBases();
            for(int i=0; i<dataBases.length; i++){
                RedisDB db = dataBases[i];
                if(db.size()>0){
                    log.info("正在重写数据库{}",i);
                    writeSelectCommand(i,channel);
                    writeDatabaseToAof(db,channel);
                }
            }
            channel.force(true);
            channel.close();
        }catch(IOException e) {
            log.error("重写AOF文件时发生错误", e);
        }
        finally {
            rewriting.compareAndSet(true, false);
        }
    }

    private void writeDatabaseToAof(RedisDB db, FileChannel channel) {
        Dict<RedisBytes, RedisData> data = db.getData();
        for(Map.Entry<Object,Object> entry: data.entrySet()){
            RedisBytes key = (RedisBytes) entry.getKey();
            RedisData value  = (RedisData) entry.getValue();
            log.info("正在重写key:{}",key.getString());
            AofUtils.writeDataToAof(key,value,channel);
        }
    }

    private void writeSelectCommand(int i, FileChannel channel) {
        List<Resp> selectCommand = new ArrayList<>();
        selectCommand.add(new BulkString("SELECT".getBytes()));
        selectCommand.add(new BulkString(String.valueOf(i).getBytes()));
        writeCommandToChannel(Collections.singletonList(new RespArray(selectCommand.toArray(new Resp[0]))), channel);
    }

    private void writeCommandToChannel(List<Resp> command, FileChannel channel) {
        if(!command.isEmpty()){
            for(Resp cmd: command){
                ByteBuf buf = Unpooled.buffer();
                cmd.encode(cmd,buf);
                ByteBuffer byteBuffer = buf.nioBuffer();
                int written = 0;
                while(written < byteBuffer.remaining()){
                    try{
                        written += channel.write(byteBuffer);
                    }catch(IOException e){
                        log.error("写入AOF文件时发生错误",e);
                    }
                }
                buf.release();
            }
        }
    }
}

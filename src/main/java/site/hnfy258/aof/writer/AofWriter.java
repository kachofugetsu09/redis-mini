package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.utils.AofUtils;
import site.hnfy258.aof.utils.FileUtils;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
@Slf4j
public class AofWriter implements Writer{
    private File file;
    private FileChannel channel;
    private RandomAccessFile raf;
    private boolean isPreallocated;
    private AtomicLong realSize = new AtomicLong(0);
    private final AtomicBoolean rewriting = new AtomicBoolean(false);

    public static final int  DEFAULT_REWRITE_BUFFER_SIZE = 100000;
    BlockingQueue<ByteBuffer> rewriteBufferQueue;

    private RedisCore redisCore;

    private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;
    public AofWriter(File file, boolean preallocated, int flushInterval, FileChannel channel,RedisCore redisCore) throws FileNotFoundException {
        this.file = file;
        this.isPreallocated = preallocated;
        this.redisCore = redisCore;
        this.rewriteBufferQueue = new LinkedBlockingDeque<>(DEFAULT_REWRITE_BUFFER_SIZE);

        if(channel == null){
            this.raf = new RandomAccessFile(file,"rw");
            this.channel = raf.getChannel();
            channel = this.channel;
        }else{
            this.channel = channel;
        }

        try{
            this.realSize = new AtomicLong(channel.size());

            if(isPreallocated){
                preAllocated(DEFAULT_PREALLOCATE_SIZE);
            }

            this.channel.position(this.realSize.get());
        } catch (IOException e) {
            try{
                if(this.channel !=null){
                    this.channel.close();
                }
                if(this.raf != null){
                    this.raf.close();
                }
            }catch(IOException ex){
                log.error("初始化关闭文件时发生错误",ex);
            }
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
        //1.创建一个buffer的副本
        ByteBuffer bufferCopy = ByteBuffer.allocate(buffer.remaining());
        bufferCopy.put(buffer.duplicate());
        bufferCopy.flip();

        int written = writtenFullyTo(channel,buffer);
        realSize.addAndGet(written);

        if(isRewriting()&& rewriteBufferQueue !=null){
            try{
                if(!rewriteBufferQueue.offer(bufferCopy,100, TimeUnit.MILLISECONDS)){
                    log.warn("重写AOF文件的缓冲区已满，丢弃数据");
                }
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                log.error("重写AOF文件的缓冲区已满，丢弃数据",e);
            }catch(Exception e){
                log.error("重写AOF文件的缓冲区已满，丢弃数据",e);
            }
        }
        return written;
    }

    boolean isRewriting() {
        return rewriting.get();
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
           if(channel !=null){
               channel =null;
           }
           if(raf != null) {
               raf.close();
                raf = null;
           }

           System.gc();
           Thread.sleep(1000);
       }catch(IOException e){
           log.error("关闭AOF文件时发生错误",e);
       }catch (InterruptedException e){
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
            File rewriteFile = File.createTempFile("redis_aof_temp",".aof",file.getParentFile());
            FileChannel rewriteChannel = new RandomAccessFile(rewriteFile,"rw").getChannel();
            rewriteChannel.position(0);
            //1.先进行数据库的重写
            RedisDB[] dataBases = redisCore.getDataBases();
            for(int i=0; i<dataBases.length; i++){
                RedisDB db = dataBases[i];
                if(db.size()>0){
                    log.info("正在重写数据库{}",i);
                    writeSelectCommand(i,rewriteChannel);
                    writeDatabaseToAof(db,rewriteChannel);
                }
            }
            log.info("开始缓冲区的重写");
            applyRewriteBuffer(rewriteFile);
            rewriteChannel.force(true);
            rewriteChannel.close();

            replaceAofFile(rewriteFile);
            log.info("重写AOF文件完成");
        }catch(IOException e) {
            log.error("重写AOF文件时发生错误", e);
        }
        finally {
            rewriteBufferQueue.clear();
            rewriting.compareAndSet(true, false);
        }
    }

    private void replaceAofFile(File rewriteFile) {
        try{
            //关闭当前文件通道
            if(channel != null){
                channel.close();
            }
            if(raf != null){
                raf.close();
            }
            File backupFile = null;

            try{
                //1.创建备份
                backupFile = FileUtils.createBackupFile(file,".bak");
                if(backupFile != null){
                    log.info("创建备份文件{}",backupFile.getAbsolutePath());
                }
                //2.将重写的新文件移动到原文件位置
                FileUtils.safeRenameFile(rewriteFile,file);
                log.info("重写AOF文件完成，替换原文件");
                //3.成功后删除备份
                if(backupFile != null && backupFile.exists()){
                    Files.delete(backupFile.toPath());
                }
            }catch(Exception e){
                log.error("重命名文件时发生错误", e);
                if(!file.exists()&& backupFile != null && backupFile.exists()){
                    try{
                        FileUtils.safeRenameFile(backupFile,file);
                        log.info("重命名文件失败，恢复备份文件");
                    }catch(Exception ex){
                        log.error("恢复备份文件时发生错误", ex);
                    }
                }
            }
            this.raf = new RandomAccessFile(file,"rw");
            this.channel = raf.getChannel();
            this.channel.position(realSize.get());
            log.info("重写AOF文件完成，替换原文件");
        }catch(IOException e){
            log.error("重命名文件时发生错误", e);
        }
    }

    private void applyRewriteBuffer(File rewriteFile) {
        int appliedCommands = 0;
        int totalBytes = 0;
        try{
            int batchSize = 1000;
            List<ByteBuffer> buffers = new ArrayList<>(batchSize);

            while(rewriteBufferQueue.drainTo(buffers,batchSize)>0){
                for(ByteBuffer buffer: buffers){
                    int written = writtenFullyTo(channel,buffer);
                    totalBytes += written;
                    appliedCommands++;
                }
                buffers.clear();
            }
            log.info("重写AOF文件的缓冲区已应用，应用了{}条命令",appliedCommands);
        }catch(Exception e) {
            log.error("重写AOF文件的缓冲区应用时发生错误", e);
        }
    }

    private void writeDatabaseToAof(RedisDB db, FileChannel channel) {
        Dict<RedisBytes, RedisData> data = db.getData();
        List<Map.Entry<Object, Object>> batch = new ArrayList<>(1000);
        int batchSize =1000;
        for(Map.Entry<Object, Object> entry: data.entrySet()){
            batch.add(entry);
            if(batch.size() >= batchSize){
                writeBatchToAof(batch,channel);
                batch.clear();
            }
        }
        if(!batch.isEmpty()){
            writeBatchToAof(batch,channel);
            batch.clear();
        }
    }

    private void writeBatchToAof(List<Map.Entry<Object, Object>> batch, FileChannel channel) {
        for(Map.Entry<Object,Object> entry: batch){
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

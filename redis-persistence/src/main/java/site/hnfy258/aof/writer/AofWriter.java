package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.utils.AofUtils;
import site.hnfy258.aof.utils.FileUtils;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.internal.Dict;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

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

/** * AOF写入器 - 负责将Redis命令持久化到AOF文件
 * 
 * <p>重构说明：原依赖RedisCore，现改为依赖RedisCore接口，实现彻底解耦</p>
 */
@Slf4j
public class AofWriter implements Writer {
    private File file;
    private FileChannel channel;
    private RandomAccessFile raf;
    private boolean isPreallocated;
    private AtomicLong realSize = new AtomicLong(0);
    private final AtomicBoolean rewriting = new AtomicBoolean(false);

    public static final int DEFAULT_REWRITE_BUFFER_SIZE = 100000;
    BlockingQueue<ByteBuffer> rewriteBufferQueue;

    // ========== 核心依赖：使用RedisCore接口实现解耦 ==========
    private final RedisCore redisCore;

    private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;    /**
     * 构造函数：基于RedisCore接口的解耦架构
     * 
     * @param file AOF文件
     * @param preallocated 是否预分配磁盘空间
     * @param flushInterval 刷盘间隔
     * @param channel 文件通道
     * @param redisCore Redis核心接口
     * @throws FileNotFoundException 文件未找到异常
     */
    public AofWriter(File file, boolean preallocated, int flushInterval, 
                     FileChannel channel, RedisCore redisCore) throws FileNotFoundException {
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
        try {
            // 1. 等待重写任务完成
            if (rewriting.get()) {
                log.info("等待AOF重写任务完成...");
                int waitCount = 0;
                while (rewriting.get() && waitCount < 100) {
                    Thread.sleep(100);
                    waitCount++;
                }
                if (rewriting.get()) {
                    log.warn("AOF重写任务未在10秒内完成，强制关闭");
                }
            }
            
            // 2. 清理重写缓冲区
            if (rewriteBufferQueue != null) {
                rewriteBufferQueue.clear();
            }
            
            // 3. 关闭文件资源
            closeFileResources(this.channel, this.raf);
            this.channel = null;
            this.raf = null;
            
            log.debug("AOF Writer 已成功关闭");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("关闭AOF文件时被中断", e);
            throw new IOException("关闭AOF文件时被中断", e);
        } catch (Exception e) {
            log.error("关闭AOF文件时发生错误", e);
            throw new IOException("关闭AOF文件时发生错误", e);
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
    }    private void rewriteTask() {
        File rewriteFile = null;
        RandomAccessFile rewriteRaf = null;
        FileChannel rewriteChannel = null;
        
        try {
            log.info("开始重写aof");
            // 1. 创建临时文件和FileChannel
            rewriteFile = File.createTempFile("redis_aof_temp", ".aof", file.getParentFile());
            rewriteRaf = new RandomAccessFile(rewriteFile, "rw");
            rewriteChannel = rewriteRaf.getChannel();
            rewriteChannel.position(0);            // 2. 进行数据库的重写
            final RedisDB[] dataBases = redisCore.getDataBases();
            for (int i = 0; i < dataBases.length; i++) {
                final RedisDB db = dataBases[i];
                if (db.size() > 0) {
                    log.info("正在重写数据库{}", i);
                    writeSelectCommand(i, rewriteChannel);
                    writeDatabaseToAof(db, rewriteChannel);
                }
            }
            
            // 3. 应用重写缓冲区
            log.info("开始缓冲区的重写");
            applyRewriteBuffer(rewriteChannel);
            
            // 4. 强制刷盘并关闭临时文件
            rewriteChannel.force(true);
            closeRewriteResources(rewriteChannel, rewriteRaf);
            rewriteChannel = null;
            rewriteRaf = null;
            
            // 5. 替换原文件
            replaceAofFile(rewriteFile);
            log.info("重写AOF文件完成");
            
        } catch (IOException e) {
            log.error("重写AOF文件时发生错误", e);
            // 删除临时文件
            if (rewriteFile != null && rewriteFile.exists()) {
                try {
                    Files.delete(rewriteFile.toPath());
                    log.info("已删除临时重写文件: {}", rewriteFile.getAbsolutePath());
                } catch (IOException deleteEx) {
                    log.warn("删除临时重写文件失败: {}", deleteEx.getMessage());
                }
            }
        } finally {
            // 6. 确保资源释放
            closeRewriteResources(rewriteChannel, rewriteRaf);
            rewriteBufferQueue.clear();
            rewriting.compareAndSet(true, false);
        }
    }
    
    /**
     * 安全关闭重写相关资源
     */
    private void closeRewriteResources(final FileChannel rewriteChannel, 
                                     final RandomAccessFile rewriteRaf) {
        if (rewriteChannel != null) {
            try {
                rewriteChannel.close();
            } catch (IOException e) {
                log.warn("关闭重写FileChannel时发生错误: {}", e.getMessage());
            }
        }
        
        if (rewriteRaf != null) {
            try {
                rewriteRaf.close();
            } catch (IOException e) {
                log.warn("关闭重写RandomAccessFile时发生错误: {}", e.getMessage());
            }
        }
    }    private void replaceAofFile(final File rewriteFile) {
        RandomAccessFile oldRaf = null;
        FileChannel oldChannel = null;
        
        try {
            // 1. 保存当前资源引用
            oldRaf = this.raf;
            oldChannel = this.channel;
            
            // 2. 先设置为null，避免close方法中重复关闭
            this.raf = null;
            this.channel = null;
            
            // 3. 关闭当前文件通道
            closeFileResources(oldChannel, oldRaf);
            
            File backupFile = null;
            try {
                // 4. 创建备份
                backupFile = FileUtils.createBackupFile(file, ".bak");
                if (backupFile != null) {
                    log.info("创建备份文件{}", backupFile.getAbsolutePath());
                }
                
                // 5. 将重写的新文件移动到原文件位置
                FileUtils.safeRenameFile(rewriteFile, file);
                log.info("重写AOF文件完成，替换原文件");
                
                // 6. 成功后删除备份
                if (backupFile != null && backupFile.exists()) {
                    Files.delete(backupFile.toPath());
                    log.info("已删除备份文件: {}", backupFile.getAbsolutePath());
                }
                
            } catch (Exception e) {
                log.error("重命名文件时发生错误", e);
                
                // 7. 失败时恢复备份
                if (!file.exists() && backupFile != null && backupFile.exists()) {
                    try {
                        FileUtils.safeRenameFile(backupFile, file);
                        log.info("重命名文件失败，已恢复备份文件");
                    } catch (Exception ex) {
                        log.error("恢复备份文件时发生错误", ex);
                        throw new RuntimeException("文件替换失败且无法恢复备份", ex);
                    }
                }
            }
            
            // 8. 重新打开文件
            reopenFile();
            log.info("文件重新打开完成，当前位置: {}", this.channel.position());
            
        } catch (IOException e) {
            log.error("替换AOF文件时发生错误", e);
            // 如果重新打开失败，尝试恢复
            try {
                reopenFile();
            } catch (IOException reopenEx) {
                log.error("重新打开文件失败", reopenEx);
                throw new RuntimeException("AOF文件替换失败且无法重新打开", reopenEx);
            }
        }
    }
      /**
     * 安全关闭文件资源
     */
    private void closeFileResources(final FileChannel fileChannel, 
                                  final RandomAccessFile randomAccessFile) {
        if (fileChannel != null && fileChannel.isOpen()) {
            try {
                // 1. 执行关闭前最后一次刷盘
                log.debug("执行关闭前最后一次刷盘");
                fileChannel.force(true);
                
                // 2. 截断文件到实际大小
                final long currentSize = realSize.get();
                if (currentSize > 0) {
                    fileChannel.truncate(currentSize);
                    log.debug("AOF文件已截断到长度{}", currentSize);
                }
                
                // 3. 关闭FileChannel
                fileChannel.close();
                
            } catch (IOException e) {
                log.warn("关闭FileChannel时发生错误: {}", e.getMessage());
            }
        }
        
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                log.warn("关闭RandomAccessFile时发生错误: {}", e.getMessage());
            }
        }
    }
    
    /**
     * 重新打开文件
     */
    private void reopenFile() throws IOException {
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        this.channel.position(realSize.get());
    }private void applyRewriteBuffer(final FileChannel rewriteChannel) {
        int appliedCommands = 0;
        int totalBytes = 0;
        
        try {
            final int batchSize = 1000;
            final List<ByteBuffer> buffers = new ArrayList<>(batchSize);

            while (rewriteBufferQueue.drainTo(buffers, batchSize) > 0) {
                for (final ByteBuffer buffer : buffers) {
                    final int written = writtenFullyTo(rewriteChannel, buffer);
                    totalBytes += written;
                    appliedCommands++;
                }
                buffers.clear();
            }
            log.info("重写AOF文件的缓冲区已应用，应用了{}条命令，总字节数: {}", 
                    appliedCommands, totalBytes);
        } catch (Exception e) {
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
    }    private void writeSelectCommand(int i, FileChannel channel) {
        List<Resp> selectCommand = new ArrayList<>();
        // 🚀 优化：使用 RedisBytes 缓存 SELECT 命令
        selectCommand.add(new BulkString(RedisBytes.fromString("SELECT")));
        selectCommand.add(new BulkString(RedisBytes.fromString(String.valueOf(i))));
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

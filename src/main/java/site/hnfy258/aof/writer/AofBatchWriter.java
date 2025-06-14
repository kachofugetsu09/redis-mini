package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AofBatchWriter {
    private final Writer writer;

    //异步刷盘
    private final ScheduledExecutorService flushScheduler;
    private final AtomicBoolean forceFlush = new AtomicBoolean(false);
    private final int flushInterval;
    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private final BlockingQueue<ByteBuf> writeQueue;
    private final Thread writeThread;
    private final AtomicBoolean running = new AtomicBoolean(true);    //批处理参数
    public static final int MIN_BATCH_SIZE = 16;
    public static final int MAX_BATCH_SIZE = 50;

    public static final int  MIN_BATCH_TIMEOUT_MS = 2;
    public static final int  MAX_BATCH_TIMEOUT_MS = 10;

    //大命令的参数
    private static final int LARGE_COMMAND_THRESHOLD = 512*1024;
    
    // 刷盘超时配置
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 500; // 默认500ms
    private static final long FAST_FLUSH_TIMEOUT_MS = 200;    // 快速模式200ms  
    private static final long SLOW_FLUSH_TIMEOUT_MS = 1000;   // 慢速模式1秒


    private AtomicLong batchCount = new AtomicLong(0);
    private AtomicLong totalBatchedCommands = new AtomicLong(0);

    public AofBatchWriter(Writer fileWriter, int  flushInterval){
        this.writer = fileWriter;
        this.flushInterval = flushInterval;



        this.writeQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE);
        this.writeThread = new Thread(this::processWriteQueue);
        this.writeThread.setName("AOF-Writer-Thread");
        this.writeThread.setDaemon(true);
        this.writeThread.start();

        this.flushScheduler = new ScheduledThreadPoolExecutor(1, r->{
            Thread thread = new Thread(r);
            thread.setName("AOF-Flush-Thread");
            thread.setDaemon(true);
            return thread;
        });

        if (flushInterval > 0) {
            this.flushScheduler.scheduleAtFixedRate(() -> {
                try {
                    if (forceFlush.compareAndSet(true, false)) {
                        writer.flush();
                    }
                } catch (Exception e) {
                    log.error("Failed to flush AOF file", e);
                }
            }, flushInterval, flushInterval, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    public void processWriteQueue(){
        ByteBuf[] batch = new ByteBuf[MAX_BATCH_SIZE];
        int batchSize = 0;

        while(running.get() || !writeQueue.isEmpty()){
            try{
                int currentBatchSize = calculateBatchSize(batchSize);
                long timeout = calculateTimeout(currentBatchSize);

                long deadline = System.currentTimeMillis() + timeout;

                while(batchSize < currentBatchSize && System.currentTimeMillis() < deadline){
                    ByteBuf item = writeQueue.poll(Math.max(1,  deadline - System.currentTimeMillis()),
                            TimeUnit.MILLISECONDS);

                    if(item != null){
                        batch[batchSize++] = item;
                    }else if(batchSize == 0){
                        Thread.yield();
                    }else{
                        break;
                    }
                }

                if(batchSize > 0){
                    writeBatch(batch, batchSize);
                    for(int i = 0; i < batchSize; i++){
                        batch[i].release();
                        batch[i] = null;
                    }
                    batchSize = 0;
                }
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch
            (Exception e){
                log.error("Failed to process write queue", e);
                for(int i = 0; i < batchSize; i++){
                    batch[i].release();
                    batch[i] = null;
                }
                batchSize = 0;
            }
        }
    }

    private void writeBatch(ByteBuf[] batch, int batchSize) {
        if(batchSize <= 0) return;
        try{
            int totalBytes = 0;
            for(int i = 0; i < batchSize; i++){
                totalBytes+=batch[i].readableBytes();
            }

            ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

            for(int i = 0; i < batchSize; i++){
                buffer.put(batch[i].nioBuffer());
            }
            buffer.flip();
            writer.write(buffer);
            batchCount.incrementAndGet();
            totalBatchedCommands.addAndGet(batchSize);
        }catch(Exception e){
            log.error("Failed to write batch to AOF file", e);
        }
    }    public void write(ByteBuf byteBuf) throws IOException {
        int byteSize = byteBuf.readableBytes();
        
        if (byteSize > LARGE_COMMAND_THRESHOLD) {
            try {
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);
                flush();
            } catch (Exception e) {
                throw new IOException("Failed to write large command", e);
            } finally {
                byteBuf.release();
            }
            return;
        }

        try {
            boolean success = writeQueue.offer(byteBuf, 3, TimeUnit.SECONDS);
            if (!success) {
                // 队列满时直接同步写入
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);
                byteBuf.release();
                log.warn("AOF队列满，直接同步写入 - 队列大小: {}", writeQueue.size());
            }
        } catch (Exception e) {
            byteBuf.release();
            Thread.currentThread().interrupt();
        }    }

    /**
     * 执行刷盘操作，使用默认超时时间
     * 
     * @throws Exception 刷盘失败或超时
     */
    public void flush() throws Exception {
        flush(DEFAULT_FLUSH_TIMEOUT_MS);
    }

    /**
     * 快速刷盘模式（用于性能要求极高的场景）
     * 
     * @throws Exception 刷盘失败或超时
     */
    public void fastFlush() throws Exception {
        flush(FAST_FLUSH_TIMEOUT_MS);
    }

    /**
     * 慢速刷盘模式（用于I/O较慢的环境）
     * 
     * @throws Exception 刷盘失败或超时
     */
    public void slowFlush() throws Exception {
        flush(SLOW_FLUSH_TIMEOUT_MS);
    }
    
    /**
     * 执行刷盘操作，指定超时时间
     * 
     * @param timeoutMs 超时时间(毫秒)
     * @throws Exception 刷盘失败或超时
     */
    public void flush(final long timeoutMs) throws Exception {
        final long startTime = System.currentTimeMillis();
        final int maxRetries = 3;
        
        // 验证超时参数
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("超时时间必须大于0: " + timeoutMs);
        }
        
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                // 1. 触发强制刷盘标志，让写入线程尽快处理
                forceFlush.set(true);
                
                // 2. 等待队列清空，带超时控制
                final long remainingTime = timeoutMs - (System.currentTimeMillis() - startTime);
                if (remainingTime <= 0) {
                    throw new IOException(String.format(
                        "AOF刷盘超时 - 耗时超过%dms，队列剩余: %d", 
                        timeoutMs, writeQueue.size()));
                }
                
                // 3. 分段等待，避免长时间阻塞
                final long waitStartTime = System.currentTimeMillis();
                while (!writeQueue.isEmpty() && 
                       (System.currentTimeMillis() - waitStartTime) < Math.min(remainingTime, 1000)) {
                    Thread.sleep(Math.min(50, remainingTime / 10)); // 动态调整等待时间
                }
                
                // 4. 如果队列已清空，执行最终刷盘
                if (writeQueue.isEmpty()) {
                    writer.flush();
                    log.debug("AOF刷盘完成，耗时: {}ms", System.currentTimeMillis() - startTime);
                    return;
                }
                
                log.warn("AOF刷盘重试 {}/{}, 队列剩余: {}", retry + 1, maxRetries, writeQueue.size());
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("AOF刷盘被中断", e);
            } catch (IOException e) {
                if (retry == maxRetries - 1) {
                    throw e; // 最后一次重试失败，抛出异常
                }
                log.warn("AOF刷盘重试 {}/{} 失败: {}", retry + 1, maxRetries, e.getMessage());
            }
        }
        
        // 所有重试都失败
        throw new IOException(String.format(
            "AOF刷盘失败 - %d次重试后仍有数据未刷盘，队列剩余: %d", 
            maxRetries, writeQueue.size()));
    }

    public void close() throws Exception {
        log.info("开始关闭 AofBatchWriter...");
        
        try {
            // 1. 停止接收新的写入请求
            running.set(false);
              // 2. 等待写入线程完成处理
            if (writeThread != null && writeThread.isAlive()) {
                writeThread.interrupt();
                try {
                    writeThread.join(3000);
                    if (writeThread.isAlive()) {
                        log.warn("写入线程未在3秒内终止");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("等待写入线程终止时被中断");
                }
            }
            
            // 3. 关闭刷盘调度器
            if (flushScheduler != null) {
                try {
                    flushScheduler.shutdown();
                    final boolean terminated = flushScheduler.awaitTermination(3, TimeUnit.SECONDS);
                    if (!terminated) {
                        log.warn("刷盘调度器未在3秒内终止，强制关闭");
                        flushScheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    flushScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                    log.warn("刷盘调度器关闭过程被中断");
                }
            }
            
            // 4. 清理队列中剩余的ByteBuf，避免内存泄漏
            cleanupWriteQueue();
            
            // 5. 执行最后的刷盘
            try {
                if (writer != null) {
                    writer.flush();
                    log.info("执行最后的刷盘完成");
                }
            } catch (Exception e) {
                log.error("最后刷盘时发生错误", e);
            }
            
            log.info("AofBatchWriter 关闭完成");
            
        } catch (Exception e) {
            log.error("关闭 AofBatchWriter 时发生错误", e);
            throw e;
        }
    }
    
    /**
     * 清理写入队列中的ByteBuf资源
     */
    private void cleanupWriteQueue() {
        int releasedCount = 0;
        
        try {
            ByteBuf byteBuf;
            while ((byteBuf = writeQueue.poll()) != null) {
                try {
                    if (byteBuf.refCnt() > 0) {
                        byteBuf.release();
                        releasedCount++;
                    }
                } catch (Exception e) {
                    log.warn("释放队列中的ByteBuf时发生错误: {}", e.getMessage());
                }
            }
              if (releasedCount > 0) {
                log.info("已释放队列中的 {} 个 ByteBuf 资源", releasedCount);
            }
              } catch (Exception e) {
            log.error("清理写入队列时发生错误", e);
        }
    }

    private long calculateTimeout(int currentBatchSize) {
        int queueSize = writeQueue.size();
        if (queueSize > DEFAULT_QUEUE_SIZE / 2) {
            return MIN_BATCH_TIMEOUT_MS;
        }
        return MAX_BATCH_TIMEOUT_MS;
    }

    private int calculateBatchSize(int batchSize) {
        int queueSize = writeQueue.size();
        int result = Math.min(MAX_BATCH_SIZE, Math.min(MIN_BATCH_SIZE, MIN_BATCH_SIZE + queueSize / 20));
        return result;
    }

    // ==================== 监控指标方法 ====================
    
    /**
     * 获取已处理的批次数量
     */
    public long getBatchCount() {
        return batchCount.get();
    }
    
    /**
     * 获取总的批处理命令数
     */
    public long getTotalBatchedCommands() {
        return totalBatchedCommands.get();
    }
    
    /**
     * 获取队列大小
     */
    public int getQueueSize() {
        return writeQueue.size();
    }
    
    
    /**
     * 检查是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }

}

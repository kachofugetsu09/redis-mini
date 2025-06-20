package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * AOF 批量写入器
 * 
 * <p>提供高性能的 AOF 命令批量写入功能。通过智能批处理和异步写入机制，
 * 在保证数据安全性的同时显著提升写入性能。支持多种刷盘策略，适应不同的
 * 性能和可靠性需求。
 * 
 * <p>核心特性：
 * <ul>
 *     <li>智能批处理 - 动态调整批次大小，优化写入性能</li>
 *     <li>异步写入 - 非阻塞的命令处理，提高响应速度</li>
 *     <li>多级刷盘 - 支持多种刷盘策略，平衡性能和可靠性</li>
 *     <li>资源管理 - 安全的资源分配和释放机制</li>
 * </ul>
 * 
 * <p>刷盘策略：
 * <ul>
 *     <li>ALWAYS - 每次写入后立即刷盘，安全性最高</li>
 *     <li>NO - 由操作系统控制刷盘，性能最好</li>
 *     <li>SMART - 智能批处理模式，平衡性能和安全性</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class AofBatchWriter implements AutoCloseable {
    /** 底层文件写入器 */
    private final Writer writer;
    
    /** 异步刷盘调度器（仅用于资源管理，SMART模式不使用定时刷盘） */
    private final ScheduledExecutorService flushScheduler;
    
    /** 默认写入队列大小 */
    private static final int DEFAULT_QUEUE_SIZE = 1000;
    
    /** 命令写入队列 */
    private final BlockingQueue<ByteBuf> writeQueue;
    
    /** 写入处理线程 */
    private final Thread writeThread;
    
    /** 运行状态标志 */
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    /** 基于 Future 的精确刷盘同步机制 */
    private volatile CompletableFuture<Void> lastBatchFuture = CompletableFuture.completedFuture(null);
    private final Object batchFutureLock = new Object();
    
    /** 批处理参数 */
    public static final int MAX_BATCH_SIZE = 50;

    /** 大命令阈值（字节） */
    private static final int LARGE_COMMAND_THRESHOLD = 512*1024;
    
    /** 刷盘超时配置（毫秒） */
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 500;  // 默认500ms
    private static final long FAST_FLUSH_TIMEOUT_MS = 200;     // 快速模式200ms
    private static final long SLOW_FLUSH_TIMEOUT_MS = 1000;    // 慢速模式1秒
    
    /** 监控指标 */
    private AtomicLong batchCount = new AtomicLong(0);
    private AtomicLong totalBatchedCommands = new AtomicLong(0);
    
    /** AOF 刷盘策略 */
    private final AofSyncPolicy syncPolicy;

    /**
     * 创建 AofBatchWriter（传统构造函数，默认使用 SMART 模式）
     */
    public AofBatchWriter(Writer fileWriter, int flushInterval) {
        this(fileWriter, AofSyncPolicy.SMART, flushInterval);
    }
      /**
     * 创建 AofBatchWriter（支持指定 AOF 刷盘策略）
     * 
     * @param fileWriter 底层文件写入器
     * @param syncPolicy AOF 刷盘策略
     * @param flushInterval 刷盘间隔（已废弃，SMART模式不需要定时器）
     */
    public AofBatchWriter(Writer fileWriter, AofSyncPolicy syncPolicy, int flushInterval) {
        this.writer = fileWriter;
        this.syncPolicy = syncPolicy;
        this.writeQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE);
        this.writeThread = new Thread(this::processWriteQueue);
        this.writeThread.setName("AOF-Writer-Thread");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        
        // SMART 模式不需要定时刷盘调度器，完全依赖主循环的批处理逻辑
        // ALWAYS 和 NO 模式也不需要定时器
        this.flushScheduler = new ScheduledThreadPoolExecutor(1, r->{
            Thread thread = new Thread(r);
            thread.setName("AOF-Flush-Thread");
            thread.setDaemon(true);
            return thread;
        });
    }    
    
    /**
     * AOF 批处理主循环 - SMART 模式的核心实现
     * 
     * 相比 Redis 的 everysec 定时刷盘，SMART 模式的优势：
     * 1. 自适应负载：根据实际命令到达速度动态调整批次大小
     * 2. 更低延迟：无需等待定时器，批次满了立即处理
     * 3. 更高吞吐：紧凑的批处理循环，减少系统调用次数
     * 4. 智能等待：使用 LockSupport.parkNanos 避免忙等待
     * 
     * 核心策略：
     * 1. 固定批量大小 + 短超时，避免复杂动态计算
     * 2. 先无阻塞 poll 一个元素，队列为空则 park 避免忙等待
     * 3. 取到第一个元素后，紧凑循环填满批次
     * 4. 立即批量写入磁盘，最大化吞吐量
     */    public void processWriteQueue() {
        ByteBuf[] batch = new ByteBuf[MAX_BATCH_SIZE];
        
        while (running.get() || !writeQueue.isEmpty()) {
            int batchSize = 0; // 将batchSize声明移到外层，确保finally块能访问
            
            try {
                // 第一步：无阻塞尝试获取第一个元素
                ByteBuf firstItem = writeQueue.poll();
                if (firstItem == null) {
                    // 队列为空，让出 CPU 避免忙等待
                    if (running.get()) {
                        LockSupport.parkNanos(1000);
                    }
                    continue;
                }
                
                // 第二步：已有第一个元素，开始批量收集
                batch[0] = firstItem;
                batchSize = 1;
                
                // 紧凑循环：尽可能填满批次（无阻塞）
                while (batchSize < MAX_BATCH_SIZE) {
                    ByteBuf item = writeQueue.poll();
                    if (item == null) {
                        break; // 队列暂时为空，立即处理当前批次
                    }
                    batch[batchSize++] = item;
                }
                
                // 第三步：批量写入磁盘
                writeBatch(batch, batchSize);
                
            } catch (Exception e) {
                log.error("AOF 批处理过程中发生异常", e);
                
                // 如果当前有未完成的批次Future，需要异常完成它
                CompletableFuture<Void> currentBatch;
                synchronized (batchFutureLock) {
                    currentBatch = lastBatchFuture;
                }
                if (currentBatch != null && !currentBatch.isDone()) {
                    currentBatch.completeExceptionally(e);
                }
            } finally {
                // 无论成功还是异常，都要清理当前批次的资源
                // 使用实际的batchSize而不是数组长度，提高效率
                for (int i = 0; i < batchSize; i++) {
                    if (batch[i] != null) {
                        try {
                            batch[i].release();
                        } catch (Exception releaseException) {
                            log.warn("释放ByteBuf时发生异常", releaseException);
                        }
                        batch[i] = null;
                    }
                }
            }
        }
        
        log.info("AOF 批处理主循环已退出");
    }

    private void writeBatch(ByteBuf[] batch, int batchSize) {
        if(batchSize <= 0) return;
        
        // 获取当前的lastBatchFuture，如果它是占位符，我们将完成它
        // 如果不是，我们创建一个新的Future
        CompletableFuture<Void> currentBatchFuture;
        synchronized (batchFutureLock) {
            if (lastBatchFuture.isDone()) {
                // 如果已经完成，创建新的Future
                currentBatchFuture = new CompletableFuture<>();
                lastBatchFuture = currentBatchFuture;
            } else {
                // 如果未完成，说明这是一个占位符，我们将使用并完成它
                currentBatchFuture = lastBatchFuture;
            }
        }
        
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
            
            // 根据 AOF 刷盘策略决定是否立即刷盘
            if (syncPolicy == AofSyncPolicy.ALWAYS) {
                writer.flush();
            } else if (syncPolicy == AofSyncPolicy.SMART) {
                // SMART 模式：让主循环的批处理逻辑自己决定何时刷盘
                // 在批次写入完成后适时刷盘，平衡性能和数据安全性
                writer.flush();
            }
            // NO 模式下不主动刷盘
            
            // 批次写入成功，标记完成
            currentBatchFuture.complete(null);
            
        }catch(Exception e){
            log.error("Failed to write batch to AOF file", e);
            // 批次写入失败，标记异常完成
            currentBatchFuture.completeExceptionally(e);
        }
    }
    
    public void write(ByteBuf byteBuf) throws IOException {
        int byteSize = byteBuf.readableBytes();
        
        if (byteSize > LARGE_COMMAND_THRESHOLD) {
            // 大命令直接同步写入，需要创建Future来保证flush的精确性
            CompletableFuture<Void> largeCmdFuture = new CompletableFuture<>();
            
            // 更新lastBatchFuture，确保flush能等待到这个大命令
            synchronized (batchFutureLock) {
                lastBatchFuture = largeCmdFuture;
            }
            
            try {
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);
                
                // 根据 AOF 刷盘策略决定是否立即刷盘（大命令通常需要立即刷盘）
                if (syncPolicy == AofSyncPolicy.ALWAYS || syncPolicy == AofSyncPolicy.SMART) {
                    writer.flush();
                }
                // NO 模式下即使是大命令也不主动刷盘
                
                // 标记大命令写入完成
                largeCmdFuture.complete(null);
                
                log.debug("大命令直接写入完成，大小: {}KB", byteSize / 1024);
                
            } catch (Exception e) {
                largeCmdFuture.completeExceptionally(e);
                throw new IOException("大命令写入失败，大小: " + byteSize + " bytes", e);
            } finally {
                byteBuf.release();
            }
            return;
        }
        
        try {
            boolean success = writeQueue.offer(byteBuf, 3, TimeUnit.SECONDS);
            if (!success) {
                // 队列满时直接同步写入，也需要Future同步
                CompletableFuture<Void> syncWriteFuture = new CompletableFuture<>();
                
                synchronized (batchFutureLock) {
                    lastBatchFuture = syncWriteFuture;
                }
                
                try {
                    ByteBuffer byteBuffer = byteBuf.nioBuffer();
                    writer.write(byteBuffer);
                    syncWriteFuture.complete(null);
                    log.warn("AOF队列满，直接同步写入 - 队列大小: {}", writeQueue.size());
                } catch (Exception e) {
                    syncWriteFuture.completeExceptionally(e);
                    throw e;
                } finally {
                    byteBuf.release();
                }
            } else {
                // 数据成功加入队列，创建一个Future占位符，确保flush能正确等待
                // 这个Future将在后台线程处理批次时被实际的批次Future替换或完成
                synchronized (batchFutureLock) {
                    // 如果当前没有未完成的Future，创建一个新的占位符
                    if (lastBatchFuture.isDone()) {
                        lastBatchFuture = new CompletableFuture<>();
                    }
                    // 如果已经有未完成的Future，保持不变，让它继续等待
                }
            }
        } catch (InterruptedException e) {
            // 如果被中断，需要释放资源并重新设置中断状态
            byteBuf.release();
            Thread.currentThread().interrupt();
            throw new IOException("写入过程被中断", e);
        } catch (Exception e) {
            // 其他异常直接抛出，已在上面处理了资源释放
            throw new IOException("写入AOF失败", e);
        }
    }

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
     * 新的精确刷盘机制：
     * 1. 获取当前最后一个批次的Future
     * 2. 等待该Future完成，确保所有已入队数据都已写入
     * 3. 执行底层Writer的flush，确保数据真正落盘
     * 
     * @param timeoutMs 超时时间(毫秒)
     * @throws Exception 刷盘失败或超时
     */
    public void flush(final long timeoutMs) throws Exception {
        final long startTime = System.currentTimeMillis();
        
        // 验证超时参数
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("超时时间必须大于0: " + timeoutMs);
        }
        
        try {
            // 1. 获取当前最后一个批次的Future（快照）
            CompletableFuture<Void> batchToWait;
            synchronized (batchFutureLock) {
                batchToWait = lastBatchFuture;
            }
            
            // 2. 等待该批次完成，带超时控制
            try {
                batchToWait.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new IOException(String.format(
                    "AOF刷盘超时 - 等待批次完成超过%dms，队列剩余: %d", 
                    timeoutMs, writeQueue.size()));
            } catch (ExecutionException e) {
                // 批次写入过程中发生异常
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new IOException("批次写入过程中发生异常", cause);
                }
            }
            
            // 3. 执行底层Writer的flush，确保数据真正落盘
            writer.flush();
            
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.debug("AOF刷盘完成，耗时: {}ms", elapsedTime);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("AOF刷盘被中断", e);
        }
    }    public void close() throws Exception {
        log.info("开始关闭 AofBatchWriter...");
        
        try {
            // 1. 停止接收新的写入请求
            running.set(false);
            
            // 2. 等待当前所有批次完成
            try {
                CompletableFuture<Void> finalBatch;
                synchronized (batchFutureLock) {
                    finalBatch = lastBatchFuture;
                }
                // 等待最后一个批次完成，超时5秒
                finalBatch.get(5, TimeUnit.SECONDS);
                log.info("所有待处理批次已完成");
            } catch (TimeoutException e) {
                log.warn("等待批次完成超时，可能有数据丢失");
            } catch (Exception e) {
                log.warn("等待批次完成时发生异常: {}", e.getMessage());
            }
            
            // 3. 等待写入线程完成处理
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
    }    // ==================== 刷盘状态查询方法 ====================
    
    /**
     * 检查是否有批次正在等待完成
     * @return true 如果有未完成的批次
     */
    public boolean hasPendingBatches() {
        synchronized (batchFutureLock) {
            return !lastBatchFuture.isDone();
        }
    }
    
    /**
     * 获取当前最后一个批次的Future，用于外部监控
     * @return 当前最后一个批次的Future
     */
    public CompletableFuture<Void> getLastBatchFuture() {
        synchronized (batchFutureLock) {
            return lastBatchFuture;
        }
    }

    /**
     * 获取当前的 AOF 刷盘策略
     */
    public AofSyncPolicy getSyncPolicy() {
        return syncPolicy;
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

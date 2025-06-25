package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
    /**
     * 底层文件写入器
     */
    private final Writer writer;

    /** 异步刷盘调度器（仅用于资源管理，SMART模式不使用定时刷盘） */

    /**
     * 默认写入队列大小
     */
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    /**
     * 命令写入队列
     */
    private final BlockingQueue<ByteBuf> writeQueue;

    /**
     * 写入处理线程
     */
    private final Thread writeThread;


    /**
     * 兜底刷盘线程（看门狗线程）
     */
    private Thread watchdogThread;

    /**
     * 控制兜底线程运行的标志
     */
    private volatile boolean watchdogRunning = true;

    /**
     * 上次刷盘的volatile时间戳
     */
    private volatile long lastFlushTimestamp;

    /**
     * 兜底刷盘的间隔（ 1 秒，对应 Redis 的 everysec）
     */
    private static final long WATCHDOG_INTERVAL_MS = 1000;

    /**
     * I/O 操作锁，确保 writer 的 write 和 flush 操作的线程安全
     * 避免 writeThread 和 watchdogThread 同时操作 writer
     */
    private final Object ioLock = new Object();


    /**
     * 运行状态标志
     */
    private final AtomicBoolean running = new AtomicBoolean(true);


    /**
     * 批处理参数
     */
    public static final int MAX_BATCH_SIZE = 50;

    /**
     * 大命令阈值（字节）
     */
    private static final int LARGE_COMMAND_THRESHOLD = 512 * 1024;

    /**
     * 刷盘超时配置（毫秒）
     */
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 500;  // 默认500ms

    /**
     * 监控指标
     */
    private AtomicLong batchCount = new AtomicLong(0);
    private AtomicLong totalBatchedCommands = new AtomicLong(0);

    /**
     * AOF 刷盘策略
     */
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
     * @param fileWriter    底层文件写入器
     * @param syncPolicy    AOF 刷盘策略
     */
    public AofBatchWriter(Writer fileWriter, AofSyncPolicy syncPolicy, int flushInterval) {
        this.writer = fileWriter;
        this.syncPolicy = syncPolicy;
        this.writeQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE);
        this.writeThread = new Thread(this::processWriteQueue);
        this.writeThread.setName("AOF-Writer-Thread");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        this.lastFlushTimestamp = System.currentTimeMillis();

        // 仅在 SMART 模式下启动兜底刷盘线程
        if (syncPolicy == AofSyncPolicy.SMART) {
            this.watchdogThread = new Thread(this::watchdogLoop);
            this.watchdogThread.setName("AOF-Watchdog-Thread");
            this.watchdogThread.setDaemon(true);
            this.watchdogThread.start();
        }


    }

    /**
     * AOF 批处理主循环 - SMART 模式的核心实现
     * <p>
     * 相比 Redis 的 everysec 定时刷盘，SMART 模式的优势：
     * 1. 自适应负载：根据实际命令到达速度动态调整批次大小
     * 2. 更低延迟：无需等待定时器，批次满了立即处理
     * 3. 更高吞吐：紧凑的批处理循环，减少系统调用次数
     * 4. 智能等待：使用 LockSupport.parkNanos 避免忙等待
     * <p>
     * 核心策略：
     * 1. 固定批量大小 + 短超时，避免复杂动态计算
     * 2. 先无阻塞 poll 一个元素，队列为空则 park 避免忙等待
     * 3. 取到第一个元素后，紧凑循环填满批次
     * 4. 立即批量写入磁盘，最大化吞吐量
     */
    public void processWriteQueue() {
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
        if (batchSize <= 0) return;

        try {
            // 写入逻辑
            int totalBytes = 0;
            for (int i = 0; i < batchSize; i++) {
                totalBytes += batch[i].readableBytes();
            }

            ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
            for (int i = 0; i < batchSize; i++) {
                buffer.put(batch[i].nioBuffer());
            }

            buffer.flip();

            synchronized (ioLock) {
                writer.write(buffer);
                // SMART模式每次写入后都同步刷盘
                if (syncPolicy == AofSyncPolicy.SMART) {
                    writer.flush();
                    lastFlushTimestamp = System.currentTimeMillis();
                }
            }

            batchCount.incrementAndGet();
            totalBatchedCommands.addAndGet(batchSize);

        } catch (Exception e) {
            log.error("Failed to write batch to AOF file", e);
        }
    }


    public void write(ByteBuf byteBuf) throws IOException {
        if (!running.get()) {
            byteBuf.release();
            throw new IOException("AofBatchWriter已关闭");
        }
        if (syncPolicy == AofSyncPolicy.ALWAYS) {
            try {
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                synchronized (ioLock) {
                    writer.write(byteBuffer);
                    writer.flush();
                }
                log.debug("[ALWAYS] 命令已立即写入并刷盘，大小: {} bytes", byteBuf.readableBytes());
            } finally {
                byteBuf.release();
            }
            return;
        }

        int byteSize = byteBuf.readableBytes();

        if (byteSize > LARGE_COMMAND_THRESHOLD) {
            // 大命令直接同步写入
            try {
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                synchronized (ioLock) {
                    writer.write(byteBuffer);
                    if (syncPolicy == AofSyncPolicy.SMART) {
                        writer.flush();
                        lastFlushTimestamp = System.currentTimeMillis();
                    }
                }
                log.debug("大命令直接写入完成，大小: {}KB", byteSize / 1024);
            } finally {
                byteBuf.release();
            }
            return;
        }

        try {
            boolean success = writeQueue.offer(byteBuf, 3, TimeUnit.SECONDS);
            if (!success) {
                // 队列满时直接同步写入
                try {
                    ByteBuffer byteBuffer = byteBuf.nioBuffer();
                    synchronized (ioLock) {
                        writer.write(byteBuffer);
                        if (syncPolicy == AofSyncPolicy.SMART) {
                            writer.flush();
                            lastFlushTimestamp = System.currentTimeMillis();
                        }
                    }
                    log.warn("AOF队列满，直接同步写入 - 队列大小: {}", writeQueue.size());
                } finally {
                    byteBuf.release();
                }
            }
            // 成功入队，后台线程会处理
        } catch (InterruptedException e) {
            byteBuf.release();
            Thread.currentThread().interrupt();
            throw new IOException("写入过程被中断", e);
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



    public void flush(final long timeoutMs) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + timeoutMs;

        // 等待队列清空，使用更智能的等待策略
        while (!writeQueue.isEmpty() && System.currentTimeMillis() < endTime) {
            long remaining = endTime - System.currentTimeMillis();
            if (remaining <= 0) break;

            Thread.sleep(Math.min(remaining, 5)); // 最多等待5ms
        }

        if (!writeQueue.isEmpty()) {
            throw new IOException(String.format(
                    "AOF刷盘超时 - 等待队列清空超过%dms，队列剩余: %d",
                    timeoutMs, writeQueue.size()));
        }

        // 执行兜底刷盘
        synchronized (ioLock) {
            writer.flush();
            if (syncPolicy == AofSyncPolicy.SMART) {
                lastFlushTimestamp = System.currentTimeMillis();
            }
        }

        log.debug("AOF刷盘完成，耗时: {}ms", System.currentTimeMillis() - startTime);
    }

    private void watchdogLoop() {
        log.info("AOF Watchdog thread started.");
        while (watchdogRunning && running.get()) { // 同时检查两个状态
            try {
                Thread.sleep(WATCHDOG_INTERVAL_MS);

                if (System.currentTimeMillis() - lastFlushTimestamp > WATCHDOG_INTERVAL_MS) {
                    synchronized (ioLock) {
                        // 双重检查，避免竞争条件
                        if (System.currentTimeMillis() - lastFlushTimestamp > WATCHDOG_INTERVAL_MS) {
                            log.debug("Watchdog performing fallback flush.");
                            writer.flush();
                            lastFlushTimestamp = System.currentTimeMillis();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("AOF Watchdog thread encountered an error during flush.", e);
            }
        }
        log.info("AOF Watchdog thread has stopped.");
    }


    public void close() throws Exception {
        log.info("开始关闭 AofBatchWriter...");
        
        try {
            // 1. 停止接收新的写入请求
            running.set(false);
            watchdogRunning = false; // 停止兜底线程循环


            
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

            // 3.中断并等待兜底线程
            if (watchdogThread != null && watchdogThread.isAlive()) {
                watchdogThread.interrupt(); // 中断其 sleep
                try {
                    watchdogThread.join(2000); // 等待其终止
                    if (watchdogThread.isAlive()) {
                        log.warn("Watchdog 线程未在2秒内终止");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("等待 Watchdog 线程终止时被中断");
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

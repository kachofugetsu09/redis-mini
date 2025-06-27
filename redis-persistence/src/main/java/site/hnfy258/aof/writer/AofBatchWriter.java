package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport; // 虽然保留，但使用场景大幅减少

@Slf4j
public class AofBatchWriter implements AutoCloseable {
    /** 底层文件写入器 */
    private final Writer writer;

    /** 异步刷盘调度器 */
    private final ScheduledExecutorService flushScheduler;

    /** 默认写入队列大小 */
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    /** 命令写入队列 */
    private final BlockingQueue<ByteBuf> writeQueue;

    /** 写入处理线程 */
    private final Thread writeThread;

    /** 运行状态标志 */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /** 批处理参数 */
    public static final int MAX_BATCH_SIZE = 50;

    /** 批处理超时时间（纳秒） - 低延迟优先策略 */
    private static final long BATCH_TIMEOUT_NANOS = 5_000_000L; // 5毫秒

    /** 大命令阈值（字节） */
    private static final int LARGE_COMMAND_THRESHOLD = 512 * 1024;

    /** 监控指标 */
    private final AtomicLong batchCount = new AtomicLong(0);
    private final AtomicLong totalBatchedCommands = new AtomicLong(0);

    /** AOF 刷盘策略 */
    private final AofSyncPolicy syncPolicy;

    // ==================== EVERYSEC 模式刷盘机制  ====================

    /** 是否有未刷盘的数据 */
    private final AtomicBoolean hasPendingFlush = new AtomicBoolean(false);

    /** 上次刷盘时间 */
    private volatile long lastFlushTime = System.currentTimeMillis();

    /** 刷盘间隔（毫秒），在everysec模式下通常为1000ms */
    private final long flushIntervalMs;

    // 彻底移除 currentFlushLatch，因为 everysec 模式不需要外部等待刷盘完成

    /** 刷盘执行线程 (负责实际执行 flush 操作，由调度器触发) */
    private final Thread flushExecuteThread; //

    /**
     * 创建 AofBatchWriter（传统构造函数，默认使用 EVERYSEC 模式）
     */
    public AofBatchWriter(Writer fileWriter, int flushInterval) {
        this(fileWriter, AofSyncPolicy.EVERYSEC, flushInterval); // 默认策略改为 EVERYSEC
    }

    /**
     * 创建 AofBatchWriter（支持指定 AOF 刷盘策略）
     *
     * @param fileWriter 底层文件写入器
     * @param syncPolicy AOF 刷盘策略
     * @param flushInterval 刷盘间隔（毫秒），在EVERYSEC模式下通常设为1000ms
     */
    public AofBatchWriter(Writer fileWriter, AofSyncPolicy syncPolicy, int flushInterval) {
        this.writer = fileWriter;
        this.syncPolicy = syncPolicy;
        this.flushIntervalMs = flushInterval;
        this.writeQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUE_SIZE);

        // 启动写入线程
        this.writeThread = new Thread(this::processWriteQueue);
        this.writeThread.setName("AOF-Writer-Thread");
        this.writeThread.setDaemon(true);
        this.writeThread.start();

        // 创建刷盘调度器，用于定时触发刷盘任务
        this.flushScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setName("AOF-Flush-Scheduler");
            thread.setDaemon(true);
            return thread;
        });

        // 如果是 EVERYSEC 模式，启动定时刷盘任务
        if (syncPolicy == AofSyncPolicy.EVERYSEC) {
            this.flushExecuteThread = null;

            // 安排定时刷盘任务，周期性地执行 performFlush 逻辑
            this.flushScheduler.scheduleAtFixedRate(this::scheduledFlushTask,
                    flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
            log.info("EVERYSEC刷盘模式已启动，刷盘间隔: {}ms", flushIntervalMs);
        } else {
            this.flushExecuteThread = null; // NO 和 ALWAYS 模式下也不需要这个线程
        }
    }


    /**
     * EVERYSEC 模式下定时刷盘任务的执行逻辑
     */
    private void scheduledFlushTask() {
        if (!running.get()) {
            log.debug("AOFBatchWriter 已停止，取消定时刷盘任务。");
            return;
        }

        if (hasPendingFlush.compareAndSet(true, false)) { // 只有有待刷盘数据才执行
            try {
                writer.flush();
                lastFlushTime = System.currentTimeMillis();
                log.debug("EVERYSEC定时刷盘完成");
            } catch (Exception e) {
                log.error("EVERYSEC定时刷盘失败", e);
                // 刷盘失败，重新设置待刷盘标志，等待下次重试
                hasPendingFlush.set(true);
            }
        } else {
            log.trace("EVERYSEC定时检查，无待刷盘数据");
        }
    }


    // performFlush 不再需要参数，因为它只是一个内部的刷盘动作
    /**
     * 执行实际的刷盘操作
     */
    private void performFlush() {
        try {
            writer.flush();
            lastFlushTime = System.currentTimeMillis();
            log.debug("AOF刷盘完成");
        } catch (Exception e) {
            log.error("AOF刷盘失败", e);
            throw new RuntimeException("刷盘失败", e);
        }
    }


    /**
     * AOF 批处理主循环
     */
    public void processWriteQueue() {
        ByteBuf[] batch = new ByteBuf[MAX_BATCH_SIZE];

        while (running.get() || !writeQueue.isEmpty()) {
            int batchSize = 0;

            try {
                // 收集批次数据
                batchSize = collectBatch(batch);
                if (batchSize > 0) {
                    // 批量写入磁盘
                    writeBatch(batch, batchSize);
                }
            } catch (Exception e) {
                log.error("AOF 批处理过程中发生异常", e);
            } finally {
                // 清理当前批次的资源
                cleanupBatch(batch, batchSize);
            }
        }

        log.info("AOF 批处理主循环已退出");
    }

    /**
     * 收集批次数据
     * @param batch 批次数组
     * @return 实际收集到的元素数量
     */
    private int collectBatch(ByteBuf[] batch) {
        // 第一步：等待第一个元素（阻塞等待）
        ByteBuf firstItem = waitForFirstItem();
        if (firstItem == null) {
            return 0; // 被中断或停止运行
        }

        // 第二步：将第一个元素加入批次
        batch[0] = firstItem;
        int batchSize = 1;

        // 第三步：在超时时间内收集更多元素
        long batchStartTime = System.nanoTime();
        batchSize += collectAdditionalItems(batch, batchSize, batchStartTime);

        return batchSize;
    }

    /**
     * 等待队列中的第一个元素
     * @return 第一个元素，如果被中断或停止运行则返回null
     */
    private ByteBuf waitForFirstItem() {
        try {
            return writeQueue.take();
        } catch (InterruptedException e) {
            if (!running.get()) {
                return null;
            }
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * 收集批次中的额外元素
     * @param batch 批次数组
     * @param currentSize 当前批次大小
     * @param batchStartTime 批次开始时间
     * @return 新增的元素数量
     */
    private int collectAdditionalItems(ByteBuf[] batch, int currentSize, long batchStartTime) {
        int additionalCount = 0;

        while (currentSize + additionalCount < MAX_BATCH_SIZE) {
            long elapsed = System.nanoTime() - batchStartTime;
            if (elapsed >= BATCH_TIMEOUT_NANOS) {
                break; // 超时，立即处理当前批次
            }

            // 计算剩余超时时间
            long remainingTimeout = BATCH_TIMEOUT_NANOS - elapsed;

            ByteBuf item = pollWithTimeout(remainingTimeout);
            if (item == null) {
                break; // 超时或队列为空，处理当前批次
            }

            batch[currentSize + additionalCount] = item;
            additionalCount++;
        }

        return additionalCount;
    }

    /**
     * 在指定超时时间内从队列中获取元素
     * @param timeoutNanos 超时时间（纳秒）
     * @return 元素，如果超时或被中断则返回null
     */
    private ByteBuf pollWithTimeout(long timeoutNanos) {
        try {
            return writeQueue.poll(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            if (!running.get()) {
                return null;
            }
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * 清理批次中的资源
     * @param batch 批次数组
     * @param batchSize 批次大小
     */
    private void cleanupBatch(ByteBuf[] batch, int batchSize) {
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


    private void writeBatch(ByteBuf[] batch, int batchSize) {
        if (batchSize <= 0) return;

        try {
            int totalBytes = 0;
            for (int i = 0; i < batchSize; i++) {
                totalBytes += batch[i].readableBytes();
            }

            ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

            for (int i = 0; i < batchSize; i++) {
                buffer.put(batch[i].nioBuffer());
            }

            buffer.flip();
            writer.write(buffer);
            batchCount.incrementAndGet();
            totalBatchedCommands.addAndGet(batchSize);

            // 根据 AOF 刷盘策略决定是否立即刷盘或标记待刷盘
            if (syncPolicy == AofSyncPolicy.ALWAYS) {
                performFlush(); // ALWAYS 模式直接刷盘
            } else if (syncPolicy == AofSyncPolicy.EVERYSEC) {
                hasPendingFlush.set(true); // EVERYSEC模式：标记有待刷盘数据
            }
            // NO 模式下不主动刷盘
        } catch (Exception e) {
            log.error("Failed to write batch to AOF file", e);
            throw new RuntimeException("批次写入失败", e);
        }
    }

    public void write(ByteBuf byteBuf) throws IOException {
        int byteSize = byteBuf.readableBytes();

        if (byteSize > LARGE_COMMAND_THRESHOLD) {
            writeLargeCommand(byteBuf, byteSize);
        } else {
            writeToQueue(byteBuf);
        }
    }

    private void writeLargeCommand(ByteBuf byteBuf, int byteSize) throws IOException {
        log.debug("处理大命令，大小: {}KB", byteSize / 1024);

        try {
            ByteBuffer byteBuffer = byteBuf.nioBuffer();
            writer.write(byteBuffer);
            handleSyncPolicy();

            log.debug("大命令直接写入完成，大小: {}KB", byteSize / 1024);
        } catch (Exception e) {
            throw new IOException("大命令写入失败，大小: " + byteSize + " bytes", e);
        } finally {
            byteBuf.release();
        }
    }

    private void writeToQueue(ByteBuf byteBuf) throws IOException {
        try {
            boolean success = writeQueue.offer(byteBuf);
            if (!success) {
                handleQueueFull(byteBuf);
            }
        } catch (Exception e) {
            throw new IOException("写入AOF失败", e);
        }
    }

    private void handleQueueFull(ByteBuf byteBuf) throws IOException {
        log.warn("AOF队列满，直接同步写入 - 队列大小: {}", writeQueue.size());

        try {
            ByteBuffer byteBuffer = byteBuf.nioBuffer();
            writer.write(byteBuffer);
            handleSyncPolicy();
        } catch (Exception e) {
            throw e;
        } finally {
            byteBuf.release();
        }
    }

    private void handleSyncPolicy() throws IOException {
        if (syncPolicy == AofSyncPolicy.ALWAYS) {
            performFlush();
        } else if (syncPolicy == AofSyncPolicy.EVERYSEC) {
            hasPendingFlush.set(true);
        }
    }


    public void flush() throws IOException {
        // 在 EVERYSEC 模式下，只需要标记有待刷盘数据
        if (syncPolicy == AofSyncPolicy.EVERYSEC) {
            hasPendingFlush.set(true);
        } else {
            // 在 ALWAYS 模式下，立即执行刷盘
            performFlush();
        }
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

            // 3. 关闭刷盘调度器 (确保不再有定时任务触发)
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

            // 4. 清理队列中剩余的ByteBuf
            cleanupWriteQueue();

            // 5. 执行最后的刷盘
            try {
                if (writer != null) {
                    log.info("执行最后一次刷盘...");
                    writer.flush(); // 强制刷盘，确保所有数据都落盘
                    log.info("最后一次刷盘完成");
                }
            } catch (Exception e) {
                log.error("最后刷盘时发生错误", e);
                throw e;
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

    /**
     * 获取是否有待刷盘数据（仅EVERYSEC模式有效）
     */
    public boolean hasPendingFlush() {
        return hasPendingFlush.get();
    }
}
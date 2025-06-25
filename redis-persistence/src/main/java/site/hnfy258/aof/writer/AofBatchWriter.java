package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


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

    // ==================== SMART 模式刷盘机制 ====================

    /** 是否有未刷盘的数据 */
    private final AtomicBoolean hasPendingFlush = new AtomicBoolean(false);

    /** 上次刷盘时间 */
    private volatile long lastFlushTime = System.currentTimeMillis();

    /** 刷盘间隔（毫秒） */
    private final long flushIntervalMs;

    private volatile CountDownLatch currentFlushLatch = new CountDownLatch(0); // 初始为0，表示当前没有等待中的刷盘任务

    /** 刷盘线程 */
    private final Thread flushThread;

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
     * @param flushInterval 刷盘间隔（毫秒）
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

        // 创建刷盘调度器
        this.flushScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setName("AOF-Flush-Scheduler");
            thread.setDaemon(true);
            return thread;
        });

        // 如果是SMART模式，启动智能刷盘线程
        if (syncPolicy == AofSyncPolicy.SMART) {
            this.flushThread = new Thread(this::smartFlushLoop);
            this.flushThread.setName("AOF-Smart-Flush-Thread");
            this.flushThread.setDaemon(true);
            this.flushThread.start();
            log.info("SMART刷盘模式已启动，刷盘间隔: {}ms", flushIntervalMs);
        } else {
            this.flushThread = null;
        }
    }

    /**
     * SMART模式的智能刷盘循环
     *
     * 机制：
     * 1. 定时刷盘：每隔flushIntervalMs时间检查并刷盘
     * 2. 通知刷盘：批次完成后可以通知立即刷盘
     * 3. 避免重复刷盘：使用hasPendingFlush标志位避免无效刷盘
     */
    private void smartFlushLoop() {
        log.info("SMART刷盘线程已启动");

        while (running.get()) {
            try {
                // 等待通知或定时超时
                LockSupport.parkNanos(flushIntervalMs * 1_000_000L); // 将毫秒转换为纳秒

                // 检查中断状态，并清除中断标记
                if (Thread.interrupted()) {
                    if (!running.get()) {
                        log.info("SMART刷盘线程收到关闭信号");
                        break; // 收到关闭信号则退出循环
                    }
                    log.warn("SMART刷盘线程被意外中断，但系统仍在运行");
                    // 这里不直接 continue，继续检查是否需要刷盘
                }

                // 检查是否需要刷盘
                boolean shouldFlush = false;
                if (hasPendingFlush.get()) {
                    long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;
                    // 满足定时刷盘间隔，或者有外部（flush方法）通知的强制刷盘请求（通过latch判断）
                    if (timeSinceLastFlush >= flushIntervalMs || currentFlushLatch.getCount() > 0) {
                        shouldFlush = true;
                        log.debug("刷盘触发，距上次刷盘: {}ms", timeSinceLastFlush);
                    }
                }

                if (shouldFlush) {
                    performFlush();
                }

            } catch (Exception e) { // 捕获其他运行时异常
                log.error("SMART刷盘过程中发生异常", e);
                // 继续运行，避免因异常导致刷盘停止
            }
        }
        log.info("SMART刷盘线程已退出");
    }

    /**
     * 执行实际的刷盘操作
     */
    private void performFlush() {
        // 使用 CAS 确保只有一个线程能执行实际的刷盘逻辑
        if (hasPendingFlush.compareAndSet(true, false)) {
            try {
                writer.flush();
                lastFlushTime = System.currentTimeMillis();
                log.debug("SMART刷盘完成");
            } catch (Exception e) {
                log.error("SMART刷盘失败", e);
                // 刷盘失败，重新设置待刷盘标志，等待下次重试
                hasPendingFlush.set(true);
            } finally {
                // 无论刷盘成功或失败，都通知等待的线程，latch 计数减一
                // 确保即使失败，等待的 flush() 调用也不会永远阻塞
                currentFlushLatch.countDown();
            }
        } else {
            // 如果 hasPendingFlush 已经是 false（说明已经被其他地方刷盘了），也应该将 latch 计数减一，避免等待线程永远阻塞。
            // 这种情况通常发生在多个 flush() 调用几乎同时触发时。
            currentFlushLatch.countDown();
        }
    }

    /**
     * 通知刷盘线程执行刷盘（非阻塞）
     */
    private void notifyFlush() {
        if (syncPolicy == AofSyncPolicy.SMART) {
            // 唤醒刷盘线程。即使线程当前不在 park 状态，unpark 也会生效一次。
            LockSupport.unpark(flushThread);
        }
    }

    /**
     * AOF 批处理主循环 - 低延迟优先策略
     *
     * 新的低延迟策略：
     * 1. 获取第一个元素后立即开始计时
     * 2. 在超时时间内尽可能收集更多元素
     * 3. 达到批次大小上限或超时则立即写入
     * 4. SMART模式下写入后通知刷盘线程
     */
    public void processWriteQueue() {
        ByteBuf[] batch = new ByteBuf[MAX_BATCH_SIZE];

        while (running.get() || !writeQueue.isEmpty()) {
            int batchSize = 0;

            try {
                // 第一步：等待第一个元素（阻塞等待）
                ByteBuf firstItem;
                try {
                    firstItem = writeQueue.take();
                } catch (InterruptedException e) {
                    if (!running.get()) {
                        break;
                    }
                    Thread.currentThread().interrupt();
                    continue;
                }

                // 记录批次开始时间
                long batchStartTime = System.nanoTime();

                // 第二步：将第一个元素加入批次
                batch[0] = firstItem;
                batchSize = 1;

                // 第三步：在超时时间内收集更多元素
                while (batchSize < MAX_BATCH_SIZE) {
                    long elapsed = System.nanoTime() - batchStartTime;
                    if (elapsed >= BATCH_TIMEOUT_NANOS) {
                        break; // 超时，立即处理当前批次
                    }

                    // 计算剩余超时时间
                    long remainingTimeout = BATCH_TIMEOUT_NANOS - elapsed;

                    ByteBuf item;
                    try {
                        item = writeQueue.poll(remainingTimeout, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        if (!running.get()) {
                            break;
                        }
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (item == null) {
                        break; // 超时或队列为空，处理当前批次
                    }

                    batch[batchSize++] = item;
                }

                // 第四步：批量写入磁盘
                writeBatch(batch, batchSize);

            } catch (Exception e) {
                log.error("AOF 批处理过程中发生异常", e);
            } finally {
                // 清理当前批次的资源
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

            // 根据 AOF 刷盘策略决定是否立即刷盘
            if (syncPolicy == AofSyncPolicy.ALWAYS) {
                writer.flush();
                lastFlushTime = System.currentTimeMillis();
            } else if (syncPolicy == AofSyncPolicy.SMART) {
                // SMART模式：标记有待刷盘数据，并通知刷盘线程
                hasPendingFlush.set(true);
                notifyFlush();
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
            // 大命令直接同步写入
            try {
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);

                // 根据 AOF 刷盘策略决定是否立即刷盘
                if (syncPolicy == AofSyncPolicy.ALWAYS) {
                    writer.flush();
                    lastFlushTime = System.currentTimeMillis();
                } else if (syncPolicy == AofSyncPolicy.SMART) {
                    // 大命令也使用SMART刷盘机制
                    hasPendingFlush.set(true);
                    notifyFlush();
                }

                log.debug("大命令直接写入完成，大小: {}KB", byteSize / 1024);

            } catch (Exception e) {
                throw new IOException("大命令写入失败，大小: " + byteSize + " bytes", e);
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
                    writer.write(byteBuffer);

                    // 同步写入也要考虑刷盘策略
                    if (syncPolicy == AofSyncPolicy.ALWAYS) {
                        writer.flush();
                        lastFlushTime = System.currentTimeMillis();
                    } else if (syncPolicy == AofSyncPolicy.SMART) {
                        hasPendingFlush.set(true);
                        notifyFlush();
                    }

                    log.warn("AOF队列满，直接同步写入 - 队列大小: {}", writeQueue.size());
                } catch (Exception e) {
                    throw e;
                } finally {
                    byteBuf.release();
                }
            }
        } catch (InterruptedException e) {
            byteBuf.release();
            Thread.currentThread().interrupt();
            throw new IOException("写入过程被中断", e);
        } catch (Exception e) {
            throw new IOException("写入AOF失败", e);
        }
    }

    /**
     * 简化的刷盘操作 - 等待当前队列为空并刷盘
     */
    public void flush() throws IOException {
        flush(5000); // 默认5秒超时
    }

    public void flush(long timeoutMs) throws IOException {
        long startTime = System.currentTimeMillis();

        while (!writeQueue.isEmpty() && running.get()) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= timeoutMs) {
                throw new IOException("等待队列清空超时: " + elapsed + "ms");
            }
            Thread.yield(); // 短暂让出 CPU，避免忙等
        }

        // 执行底层刷盘
        try {
            if (syncPolicy == AofSyncPolicy.SMART) {
                // 在通知刷盘前，先设置好 CountDownLatch，准备等待
                // 确保在 notifyFlush() 之后，smartFlushLoop 来得及更新 hasPendingFlush 并执行刷盘。
                // 这里创建一个新的 CountDownLatch(1) 表示我们期望一次刷盘操作。
                currentFlushLatch = new CountDownLatch(1);
                hasPendingFlush.set(true); // 标记有数据需要刷盘

                notifyFlush(); // 通知刷盘线程立即执行刷盘

                // 等待刷盘完成，带超时。
                // 计算剩余的超时时间，因为之前已经等待队列清空了一段时间。
                long remainingTimeout = timeoutMs - (System.currentTimeMillis() - startTime);
                boolean flushed = currentFlushLatch.await(remainingTimeout, TimeUnit.MILLISECONDS);

                if (!flushed) { // 如果 await 返回 false，表示超时了
                    throw new IOException("SMART刷盘等待超时");
                }
                log.debug("flush() 方法：SMART刷盘操作完成并被通知");

            } else {
                // 其他模式直接刷盘
                writer.flush();
                lastFlushTime = System.currentTimeMillis();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("刷盘操作被中断", e);
        } catch (Exception e) {
            log.error("底层刷盘失败", e);
            throw new IOException("底层刷盘失败", e);
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

            // 3. 关闭SMART刷盘线程
            if (flushThread != null && flushThread.isAlive()) {
                flushThread.interrupt();
                try {
                    flushThread.join(2000);
                    if (flushThread.isAlive()) {
                        log.warn("SMART刷盘线程未在2秒内终止");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("等待刷盘线程终止时被中断");
                }
            }

            // 4. 关闭刷盘调度器
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

            // 5. 清理队列中剩余的ByteBuf
            cleanupWriteQueue();

            // 6. 执行最后的刷盘
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

    /**
     * 获取是否有待刷盘数据（仅SMART模式有效）
     */
    public boolean hasPendingFlush() {
        return hasPendingFlush.get();
    }


}
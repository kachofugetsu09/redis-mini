package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AOF 文件写入器
 * 
 * <p>负责将 Redis 命令持久化到 AOF 文件，支持文件预分配、重写等高级特性。
 * 基于 RedisCore 接口设计，实现与具体 Redis 实现的解耦。
 * 
 * <p>核心功能：
 * <ul>
 *     <li>命令写入 - 将 Redis 命令追加到 AOF 文件</li>
 *     <li>文件重写 - 支持 AOF 文件的重写和压缩</li>
 *     <li>空间预分配 - 通过预分配提升写入性能</li>
 *     <li>文件管理 - 安全的文件操作和备份机制</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>空间预分配 - 减少文件系统碎片</li>
 *     <li>批量写入 - 支持批量命令写入</li>
 *     <li>异步重写 - 后台执行文件重写</li>
 *     <li>安全备份 - 重写过程中保护原文件</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class AofWriter implements Writer {
    /**
     * AOF 文件对象
     */
    private File file;

    /**
     * 文件通道，用于写入操作
     */
    private FileChannel channel;

    /**
     * 随机访问文件，用于文件操作
     */
    private RandomAccessFile raf;

    /**
     * 是否启用预分配空间
     */
    private boolean isPreallocated;

    /**
     * 文件实际大小（不包含预分配空间）
     */
    private AtomicLong realSize;

    /**
     * 重写状态标志
     */
    private final AtomicBoolean rewriting = new AtomicBoolean(false);

    /**
     * 默认重写缓冲区大小
     */
    private static final int DEFAULT_REWRITE_BUFFER_SIZE = 100000;

    /**
     * 溢出文件管理器
     */
    private final OverflowFileManager overflowManager;

    /**
     * 内部类：管理溢出文件
     */
    private class OverflowFileManager implements AutoCloseable {
        private File currentOverflowFile;
        private RandomAccessFile currentRaf;
        private FileChannel currentChannel;
        private final AtomicLong totalOverflowBytes = new AtomicLong(0);
        private final AtomicInteger fileCounter = new AtomicInteger(0);
        private final List<File> overflowFiles = new CopyOnWriteArrayList<>();
        private final ReentrantLock lock = new ReentrantLock();

        OverflowFileManager() {
            createNewOverflowFile();
        }

        private void createNewOverflowFile() {
            lock.lock();
            try {
                closeCurrentFile();
                
                currentOverflowFile = File.createTempFile(
                    OVERFLOW_FILE_PREFIX + fileCounter.incrementAndGet(),
                    OVERFLOW_FILE_SUFFIX,
                    file.getParentFile()
                );
                currentRaf = new RandomAccessFile(currentOverflowFile, "rw");
                currentChannel = currentRaf.getChannel();
                overflowFiles.add(currentOverflowFile);
                
                log.info("创建新的溢出文件: {}", currentOverflowFile.getAbsolutePath());
            } catch (IOException e) {
                log.error("创建溢出文件失败", e);
                throw new RuntimeException("创建溢出文件失败", e);
            } finally {
                lock.unlock();
            }
        }

        void writeToOverflow(ByteBuf buffer) {
            lock.lock();
            try {
                // 如果当前文件太大，创建新文件
                if (currentChannel.size() > DEFAULT_REWRITE_BUFFER_SIZE * 2) {
                    createNewOverflowFile();
                }

                ByteBuffer nioBuffer = buffer.nioBuffer();
                int written = 0;
                while (written < nioBuffer.remaining()) {
                    written += currentChannel.write(nioBuffer);
                }
                totalOverflowBytes.addAndGet(written);
                currentChannel.force(false);
                
                if (written > 0) {
                    log.info("写入{}字节到溢出文件，总溢出量: {}", written, totalOverflowBytes.get());
                }
            } catch (IOException e) {
                log.error("写入溢出文件失败", e);
                throw new RuntimeException("写入溢出文件失败", e);
            } finally {
                lock.unlock();
            }
        }

        void mergeOverflowFiles(FileChannel targetChannel) throws IOException {
            if (overflowFiles.isEmpty()) {
                return;
            }

            log.info("开始合并{}个溢出文件，总大小: {}字节", overflowFiles.size(), totalOverflowBytes.get());
            
            // 关闭当前文件以确保所有数据都已写入
            closeCurrentFile();

            // 按创建顺序合并所有溢出文件
            for (File overflowFile : overflowFiles) {
                try (RandomAccessFile raf = new RandomAccessFile(overflowFile, "r");
                     FileChannel channel = raf.getChannel()) {
                    
                    long size = channel.size();
                    if (size > 0) {
                        channel.transferTo(0, size, targetChannel);
                        log.info("合并溢出文件: {}, 大小: {}字节", overflowFile.getAbsolutePath(), size);
                    }
                }
            }
        }

        private void closeCurrentFile() {
            if (currentChannel != null) {
                try {
                    currentChannel.force(true);
                    currentChannel.close();
                } catch (IOException e) {
                    log.warn("关闭当前溢出文件通道失败", e);
                }
                currentChannel = null;
            }
            
            if (currentRaf != null) {
                try {
                    currentRaf.close();
                } catch (IOException e) {
                    log.warn("关闭当前溢出文件失败", e);
                }
                currentRaf = null;
            }
        }

        @Override
        public void close() {
            closeCurrentFile();
            
            // 清理所有溢出文件
            for (File file : overflowFiles) {
                try {
                    Files.deleteIfExists(file.toPath());
                    log.info("清理溢出文件: {}", file.getAbsolutePath());
                } catch (IOException e) {
                    log.warn("清理溢出文件失败: {}", file.getAbsolutePath(), e);
                }
            }
            overflowFiles.clear();
        }
    }

    /**
     * 重写缓冲区队列
     */
    private final BlockingQueue<ByteBuf> rewriteBufferQueue;

    /**
     * Redis 核心接口
     */
    private final RedisCore redisCore;

    /**
     * 默认预分配空间大小（4MB）
     */
    private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;

    private final ByteBufAllocator allocator;

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private static final long SNAPSHOT_TIMEOUT_MS = 30_000L;  // 30秒超时

    private static final int BUFFER_DRAIN_TIMEOUT = 5000; // 5秒
    private volatile boolean stopBufferProcessing = false;
    private final CountDownLatch bufferProcessingComplete = new CountDownLatch(1);
    private final ConcurrentLinkedQueue<CompletableFuture<?>> pendingSnapshots = new ConcurrentLinkedQueue<>();

    private static final String OVERFLOW_FILE_PREFIX = "redis_aof_overflow";
    private static final String OVERFLOW_FILE_SUFFIX = ".tmp";

    /**
     * 构造函数：基于RedisCore接口的解耦架构
     *
     * @param file          AOF文件
     * @param preallocated  是否预分配磁盘空间
     * @param flushInterval 刷盘间隔
     * @param channel       文件通道
     * @param redisCore     Redis核心接口
     * @throws FileNotFoundException 文件未找到异常
     */
    public AofWriter(File file, boolean preallocated, int flushInterval,
                     FileChannel channel, RedisCore redisCore) throws IOException {
        this.file = file;
        this.isPreallocated = preallocated;
        this.redisCore = redisCore;
        this.allocator = PooledByteBufAllocator.DEFAULT;
        this.rewriteBufferQueue = new LinkedBlockingQueue<>(DEFAULT_REWRITE_BUFFER_SIZE);
        this.overflowManager = new OverflowFileManager();

        try {
            if (channel == null) {
                this.raf = new RandomAccessFile(file, "rw");
                this.channel = this.raf.getChannel();
            } else {
                this.channel = channel;
            }

            this.realSize = new AtomicLong(this.channel.size());

            if (isPreallocated) {
                preAllocated(DEFAULT_PREALLOCATE_SIZE);
            }

            this.channel.position(this.realSize.get());
        } catch (IOException e) {
            // 确保资源被正确清理
            closeQuietly(this.channel);
            closeQuietly(this.raf);
            this.channel = null;
            this.raf = null;
            throw new IOException("初始化AOF Writer时发生错误", e);
        }
    }

    private void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                log.warn("关闭资源时发生错误", e);
            }
        }
    }

    private void preAllocated(int defaultPreallocateSize) throws IOException {
        long currentSize = 0;
        try {
            currentSize = this.channel.size();
        } catch (IOException e) {
            log.error("获取文件长度时发生错误", e);
        }
        long newSize = currentSize + defaultPreallocateSize;
        if (this.raf != null) {
            this.raf.setLength(newSize);
        } else if (this.channel != null) {
            this.channel.truncate(newSize);
        }

        this.channel.position(currentSize);
        this.realSize.set(currentSize);
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        // 检查是否已关闭
        if (channel == null || !channel.isOpen()) {
            throw new IOException("AOF Writer 已关闭，无法执行写入操作");
        }

        // 1. 写入到文件
        int written = writtenFullyTo(channel, buffer);
        realSize.addAndGet(written);

        // 2. 如果正在重写，复制数据到重写缓冲区
        if (isRewriting()) {
            copyToRewriteBuffer(buffer);
        }

        // 3. 确保数据写入磁盘
        channel.force(false);

        return written;
    }

    private void copyToRewriteBuffer(ByteBuffer buffer) {
        if (rewriteBufferQueue == null) {
            return;
        }

        ByteBuf bufferCopy = null;
        try {
            bufferCopy = allocator.buffer(buffer.remaining());
            int originalPosition = buffer.position();
            bufferCopy.writeBytes(buffer.duplicate());
            buffer.position(originalPosition);

            // 尝试放入队列，如果失败则写入溢出文件
            if (!offerToQueue(bufferCopy)) {
                // 转移所有权到溢出管理器
                overflowManager.writeToOverflow(bufferCopy);
                bufferCopy = null; // 防止被释放
            }
        } finally {
            ReferenceCountUtil.safeRelease(bufferCopy);
        }
    }

    private boolean offerToQueue(ByteBuf buffer) {
        try {
            // 尝试放入队列，最多等待100ms
            return rewriteBufferQueue.offer(buffer, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    boolean isRewriting() {
        return rewriting.get();
    }

    private int writtenFullyTo(FileChannel channel, ByteBuffer buffer) {
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();
        int totalBytes = buffer.remaining();

        try {
            int written = 0;
            while (written < totalBytes) {
                written += channel.write(buffer);
            }
            return written;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            buffer.position(originalPosition);
            buffer.limit(originalLimit);
        }
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public boolean bgrewrite() throws IOException {
        if (rewriting.get()) {
            log.warn("正在进行AOF重写，无法再次执行");
            return false;
        }

        // 检查RedisCore是否可用
        if (redisCore == null) {
            log.warn("RedisCore未设置，无法执行AOF重写");
            return false;
        }

        rewriting.set(true);
        Thread rewriteThread = new Thread(this::twoPhaseRewriteTask);
        rewriteThread.start();
        return true;
    }

    private void twoPhaseRewriteTask() {
        File snapshotFile = null;  // 文件A：存储快照数据
        File bufferFile = null;    // 文件B：存储重写期间的新命令
        
        try {
            // 第一阶段：准备工作
            snapshotFile = File.createTempFile("redis_aof_snapshot", ".aof", file.getParentFile());
            bufferFile = File.createTempFile("redis_aof_buffer", ".aof", file.getParentFile());
            
            // 触发异步快照，真正的异步，不等待
            CompletableFuture<Map<Integer, Dict.DictSnapshot<RedisBytes, RedisData>>> snapshotsFuture = triggerAsyncSnapshot();
            
            // 启动缓冲区处理
            CompletableFuture<Void> bufferWriteFuture = processRewriteBuffer(bufferFile);
            
            try {
                // 等待快照完成
                Map<Integer, Dict.DictSnapshot<RedisBytes, RedisData>> snapshots = 
                    snapshotsFuture.get(30, TimeUnit.SECONDS);
                
                // 写入快照数据到文件A
                writeSnapshotsToFile(snapshots, snapshotFile);
                
                // 通知缓冲区处理线程可以开始准备退出
                stopBufferProcessing = true;
                
                // 等待缓冲区处理完成
                if (!bufferProcessingComplete.await(BUFFER_DRAIN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException("等待缓冲区处理完成超时");
                }
                
                // 等待缓冲区写入完成
                bufferWriteFuture.get(5, TimeUnit.SECONDS);
                
                // 第三阶段：合并文件
                mergeAndReplace(snapshotFile, bufferFile);
                
                log.info("AOF重写完成");
            } catch (Exception e) {
                // 取消所有未完成的快照
                while (!pendingSnapshots.isEmpty()) {
                    CompletableFuture<?> snapshot = pendingSnapshots.poll();
                    if (snapshot != null) {
                        snapshot.cancel(true);
                    }
                }
                throw e;
            }
            
        } catch (Exception e) {
            log.error("AOF重写失败", e);
            cleanupRewriteFiles(snapshotFile, bufferFile);
        } finally {
            stopBufferProcessing = true;
            rewriting.set(false);
            clearRewriteBuffer();
            bufferProcessingComplete.countDown(); // 确保不会死锁
        }
    }

    private CompletableFuture<Map<Integer, Dict.DictSnapshot<RedisBytes, RedisData>>> triggerAsyncSnapshot() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RedisDB[] dataBases = redisCore.getDataBases();
                Map<Integer, Dict.DictSnapshot<RedisBytes, RedisData>> snapshots = new ConcurrentHashMap<>();
                List<CompletableFuture<Void>> snapshotFutures = new ArrayList<>();
                
                // 为每个数据库并行触发快照
                for (RedisDB db : dataBases) {
                    if (db != null && db.size() > 0) {
                        CompletableFuture<Void> future = db.getData().createAofSnapshot()
                            .thenAccept(snapshot -> {
                                if (snapshot != null) {
                                    snapshots.put(db.getId(), snapshot);
                                }
                            });
                        snapshotFutures.add(future);
                        pendingSnapshots.offer(future);
                    }
                }
                
                // 等待所有快照完成
                CompletableFuture.allOf(snapshotFutures.toArray(new CompletableFuture[0]))
                    .get(SNAPSHOT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                
                return snapshots;
            } catch (Exception e) {
                throw new CompletionException("创建数据库快照失败", e);
            }
        });
    }

    private CompletableFuture<Void> processRewriteBuffer(File bufferFile) {
        return CompletableFuture.runAsync(() -> {
            try (RandomAccessFile raf = new RandomAccessFile(bufferFile, "rw");
                 FileChannel channel = raf.getChannel()) {
                
                while (!stopBufferProcessing || !rewriteBufferQueue.isEmpty()) {
                    ByteBuf buf = rewriteBufferQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (buf != null) {
                        try {
                            ByteBuffer nioBuffer = buf.nioBuffer();
                            writtenFullyTo(channel, nioBuffer);
                        } finally {
                            ReferenceCountUtil.safeRelease(buf);
                        }
                    }
                }
                
                channel.force(true);
                bufferProcessingComplete.countDown();
            } catch (Exception e) {
                bufferProcessingComplete.countDown(); // 确保在异常情况下也会释放锁
                throw new CompletionException("处理重写缓冲区失败", e);
            }
        });
    }

    private void writeSnapshotsToFile(Map<Integer, Dict.DictSnapshot<RedisBytes, RedisData>> snapshots, 
                                    File snapshotFile) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(snapshotFile, "rw");
             FileChannel channel = raf.getChannel()) {
            
            // 按数据库ID排序处理
            List<Integer> dbIds = new ArrayList<>(snapshots.keySet());
            Collections.sort(dbIds);
            
            for (Integer dbId : dbIds) {
                Dict.DictSnapshot<RedisBytes, RedisData> snapshot = snapshots.get(dbId);
                if (snapshot != null) {
                    // 写入SELECT命令
                    writeSelectCommand(dbId, channel);
                    
                    // 批量写入数据
                    List<Map.Entry<RedisBytes, RedisData>> batch = new ArrayList<>(1000);
                    for (Map.Entry<RedisBytes, RedisData> entry : snapshot) {
                        batch.add(entry);
                        if (batch.size() >= 1000) {
                            writeBatchToAof(batch, channel);
                            batch.clear();
                        }
                    }
                    
                    if (!batch.isEmpty()) {
                        writeBatchToAof(batch, channel);
                    }
                }
            }
            
            channel.force(true);
        }
    }

    private void mergeAndReplace(File snapshotFile, File bufferFile) throws IOException {
        File mergedFile = File.createTempFile("redis_aof_merged", ".aof", file.getParentFile());
        
        try {
            try (FileChannel targetChannel = new RandomAccessFile(mergedFile, "rw").getChannel();
                 FileChannel snapshotChannel = new RandomAccessFile(snapshotFile, "r").getChannel();
                 FileChannel bufferChannel = new RandomAccessFile(bufferFile, "r").getChannel()) {
                
                // 1. 复制快照文件内容
                snapshotChannel.transferTo(0, snapshotChannel.size(), targetChannel);
                
                // 2. 复制缓冲区文件内容
                bufferChannel.transferTo(0, bufferChannel.size(), targetChannel);
                
                // 3. 合并溢出文件内容
                overflowManager.mergeOverflowFiles(targetChannel);
                
                targetChannel.force(true);
            }
            
            // 替换原文件
            replaceAofFile(mergedFile);
            
        } finally {
            cleanupRewriteFiles(snapshotFile, bufferFile);
            if (mergedFile != null && mergedFile.exists()) {
                mergedFile.delete();
            }
        }
    }

    private void cleanupRewriteFiles(File... files) {
        for (File file : files) {
            if (file != null && file.exists()) {
                try {
                    file.delete();
                } catch (Exception e) {
                    log.warn("清理临时文件失败: " + file.getAbsolutePath(), e);
                }
            }
        }
    }

    private void replaceAofFile(final File rewriteFile) {
        RandomAccessFile oldRaf = null;
        FileChannel oldChannel = null;

        try {
            // 1. 保存当前资源引用并关闭
            oldRaf = this.raf;
            oldChannel = this.channel;
            this.raf = null;
            this.channel = null;
            closeFileResources(oldChannel, oldRaf);

            // 2. 执行文件替换操作
            performFileReplacement(rewriteFile);
            this.realSize.set(file.length());

            // 3. 重新打开文件
            reopenFile();
            log.info("文件重新打开完成，当前位置: {}", this.channel.position());

        } catch (IOException e) {
            log.error("替换AOF文件时发生错误", e);
            handleReopenFailure();
        }
    }

    private void performFileReplacement(File rewriteFile) throws IOException {
        File backupFile = null;
        try {
            // 创建备份
            backupFile = FileUtils.createBackupFile(file, ".bak");
            if (backupFile != null) {
                log.info("创建备份文件{}", backupFile.getAbsolutePath());
            }

            // 替换文件
            FileUtils.safeRenameFile(rewriteFile, file);
            log.info("重写AOF文件完成，替换原文件");

            // 删除备份
            deleteBackupFile(backupFile);

        } catch (Exception e) {
            log.error("重命名文件时发生错误", e);
            restoreFromBackup(backupFile);
            throw e;
        }
    }

    private void deleteBackupFile(File backupFile) {
        if (backupFile != null && backupFile.exists()) {
            try {
                Files.delete(backupFile.toPath());
                log.info("已删除备份文件: {}", backupFile.getAbsolutePath());
            } catch (IOException e) {
                log.warn("删除备份文件失败: {}", e.getMessage());
            }
        }
    }

    private void restoreFromBackup(File backupFile) {
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

    private void handleReopenFailure() {
        try {
            reopenFile();
        } catch (IOException reopenEx) {
            log.error("重新打开文件失败", reopenEx);
            throw new RuntimeException("AOF文件替换失败且无法重新打开", reopenEx);
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
                log.info("执行关闭前最后一次刷盘");
                fileChannel.force(true);

                // 2. 截断文件到实际大小
                final long currentSize = realSize.get();
                if (currentSize >= 0) { // 确保大小非负
                    // 显式处理文件逻辑大小为0的情况，强制物理截断为0
                    if (currentSize == 0 && randomAccessFile != null) {
                        randomAccessFile.setLength(0); // 直接将物理文件大小设置为0
                        log.info("AOF文件已截断到长度{} (物理截断为0)", currentSize); // 使用 info 级别日志
                    } else if (currentSize > 0) { // 正常截断到实际数据长度
                        fileChannel.truncate(currentSize);
                        log.info("AOF文件已截断到长度{}", currentSize); // 使用 info 级别日志
                    }
                } else {
                    log.warn("尝试截断到无效的负数长度: {}", currentSize);
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
    }

    private void applyRewriteBuffer(final FileChannel rewriteChannel) {
        int appliedCommands = 0;
        int totalBytes = 0;

        try {
            final int batchSize = 1000;
            final List<ByteBuf> buffers = new ArrayList<>(batchSize);

            while (rewriteBufferQueue.drainTo(buffers, batchSize) > 0) {
                for (final ByteBuf buffer : buffers) {
                    try {
                        ByteBuffer nioBuffer = buffer.nioBuffer();
                        final int written = writtenFullyTo(rewriteChannel, nioBuffer);
                        totalBytes += written;
                        appliedCommands++;
                    } finally {
                        //  使用ReferenceCountUtil安全释放
                        ReferenceCountUtil.safeRelease(buffer);
                    }
                }
                buffers.clear();
            }
            log.info("重写AOF文件的缓冲区已应用，应用了{}条命令，总字节数: {}",
                    appliedCommands, totalBytes);
        } catch (Exception e) {
            log.error("重写AOF文件的缓冲区应用时发生错误", e);
        }
    }

    private void writeDatabaseToAof(RedisDB db, FileChannel channel) throws IOException {
        Dict<RedisBytes, RedisData> data = db.getData();

        try {
            // 等待快照创建完成
            Dict.DictSnapshot<RedisBytes, RedisData> snapshot = data.createAofSnapshot()
                .get(SNAPSHOT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<Map.Entry<RedisBytes, RedisData>> batch = new ArrayList<>(1000);
            int batchSize = 1000;

            // 分批处理快照数据
            for (Map.Entry<RedisBytes, RedisData> entry : snapshot) {
                batch.add(entry);
                if (batch.size() >= batchSize) {
                    writeBatchToAof(batch, channel);
                    batch.clear();
                }
            }

            // 处理剩余数据
            if (!batch.isEmpty()) {
                writeBatchToAof(batch, channel);
                batch.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("AOF重写过程中被中断", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("AOF重写过程中创建快照失败", e);
        }
    }

    private void writeBatchToAof(List<Map.Entry<RedisBytes, RedisData>> batch, FileChannel channel) {
        for (Map.Entry<RedisBytes, RedisData> entry : batch) {
            RedisBytes key = entry.getKey();
            RedisData value = entry.getValue();
            log.info("正在重写key:{}", key.getString());
            AofUtils.writeDataToAof(key, value, channel);
        }
    }

    private void writeSelectCommand(int i, FileChannel channel) {
        List<Resp> selectCommand = new ArrayList<>();
        //使用 RedisBytes 缓存 SELECT 命令
        selectCommand.add(new BulkString(RedisBytes.fromString("SELECT")));
        selectCommand.add(new BulkString(RedisBytes.fromString(String.valueOf(i))));
        writeCommandToChannel(Collections.singletonList(new RespArray(selectCommand.toArray(new Resp[0]))), channel);
    }

    private void writeCommandToChannel(List<Resp> command, FileChannel channel) {
        if (command.isEmpty()) {
            return;
        }

        for (Resp cmd : command) {
            ByteBuf buf = null;
            try {
                buf = allocator.buffer(DEFAULT_BUFFER_SIZE);
                cmd.encode(cmd, buf);

                ByteBuffer byteBuffer = buf.nioBuffer();
                writeByteBufferToChannel(byteBuffer, channel);

            } finally {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
    }

    private void writeByteBufferToChannel(ByteBuffer byteBuffer, FileChannel channel) {
        int written = 0;
        while (written < byteBuffer.remaining()) {
            try {
                written += channel.write(byteBuffer);
            } catch (IOException e) {
                log.error("写入AOF文件时发生错误", e);
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        log.info("开始关闭 AOF Writer...");
        
        // 如果已经关闭，直接返回
        if (channel == null && raf == null) {
            log.info("AOF Writer 已经关闭");
            return;
        }

        try {
            // 1. 等待重写任务完成
            waitForRewriteCompletion();

            // 2. 清理重写缓冲区
            clearRewriteBuffer();

            // 3. 执行最后一次刷盘（如果channel还打开）
            if (channel != null && channel.isOpen()) {
                try {
                    log.info("执行最后一次刷盘，当前位置: {}", channel.position());
                    channel.force(true);
                } catch (IOException e) {
                    log.warn("最后一次刷盘时发生错误", e);
                    // 继续执行关闭流程
                }
            }

            // 4. 截断文件到实际大小（如果channel和raf还打开）
            final long currentSize = realSize.get();
            if (currentSize >= 0 && channel != null && channel.isOpen() && raf != null) {
                try {
                    if (currentSize == 0) {
                        log.info("AOF文件为空，执行物理截断");
                        raf.setLength(0);
                    } else {
                        log.info("截断AOF文件到实际大小: {}", currentSize);
                        channel.truncate(currentSize);
                    }
                    // 再次强制刷盘确保截断生效
                    channel.force(true);
                } catch (IOException e) {
                    log.warn("截断文件时发生错误", e);
                    // 继续执行关闭流程
                }
            }

            // 5. 关闭文件资源（按照正确的顺序：先channel后raf）
            IOException closeException = null;
            
            if (channel != null) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (IOException e) {
                    closeException = e;
                    log.warn("关闭channel时发生错误", e);
                } finally {
                    channel = null;
                }
            }

            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    if (closeException == null) {
                        closeException = e;
                    }
                    log.warn("关闭RandomAccessFile时发生错误", e);
                } finally {
                    raf = null;
                }
            }

            log.info("AOF Writer 已成功关闭");

            // 如果有异常发生，抛出最后捕获的异常
            if (closeException != null) {
                throw new IOException("关闭AOF文件时发生错误", closeException);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("关闭AOF文件时被中断", e);
        } catch (Exception e) {
            throw new IOException("关闭AOF文件时发生错误", e);
        } finally {
            overflowManager.close();
        }
    }
    private void waitForRewriteCompletion() throws InterruptedException {
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
    }
    private void clearRewriteBuffer() {
        if (rewriteBufferQueue != null) {
            ByteBuf buf;
            while ((buf = rewriteBufferQueue.poll()) != null) {
                ReferenceCountUtil.safeRelease(buf);
            }
            rewriteBufferQueue.clear();
        }
    }
}

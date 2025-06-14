package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import site.hnfy258.aof.AofManager;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.core.RedisCoreImpl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;

@Slf4j
public class AofExtremeCaseTest {

    @TempDir
    File tempDir;
    private File aofFile;
    private RedisCore redisCore;
    private AofManager aofManager;
    private Random random = new Random();
    // 测试配置
    private static final int LARGE_COMMAND_SIZE = 1024 * 1024; // 1MB 命令
    private static final int CONCURRENT_THREADS = 10;
    private static final int COMMANDS_PER_THREAD = 1000;
    private static final int CRASH_RECOVERY_COMMANDS = 5000;
    private static final int DEFAULT_DB_COUNT = 16;
    private static final int LARGE_COMMAND_THRESHOLD = 512 * 1024; // 与AofBatchWriter中的阈值保持一致
    @BeforeEach
    void setUp() throws Exception {
        log.info("=== 开始测试准备 ===");
        aofFile = new File(tempDir, "appendonly.aof");
        redisCore = new RedisCoreImpl(DEFAULT_DB_COUNT, null);
        aofManager = new AofManager(aofFile.getAbsolutePath(), redisCore);
        log.info("临时目录: {}", tempDir.getAbsolutePath());
        log.info("AOF文件: {}", aofFile.getAbsolutePath());
        log.info("=== 测试准备完成 ===\n");
    }
    @AfterEach
    void tearDown() throws Exception {
        log.info("\n=== 开始清理测试资源 ===");
        try {
            if (aofManager != null) {
                try {
                    aofManager.flush();
                } catch (Exception e) {
                    log.warn("刷新AOF管理器时出错", e);
                }

                try {
                    aofManager.close();
                } catch (Exception e) {
                    log.warn("关闭AOF管理器时出错", e);
                } finally {
                    aofManager = null;
                }
                log.info("AOF管理器已关闭");
            }

            // 确保强制释放文件资源
            System.gc();
            Thread.sleep(100);

            // 尝试删除所有测试文件
            int deletedCount = 0;
            int failedCount = 0;
            File[] filesToDelete = {
                    new File(tempDir, "appendonly.aof"),
                    new File(tempDir, "direct.aof"),
                    new File(tempDir, "batch.aof"),
                    new File(tempDir, "slow.aof"),
                    new File(tempDir, "source.tmp")
            };
            for (File file : filesToDelete) {
                if (deleteIfExists(file)) {
                    deletedCount++;
                } else {
                    failedCount++;
                }
            }
            log.info("成功删除文件数: {}, 删除失败文件数: {}", deletedCount, failedCount);
        } catch (Exception e) {
            log.error("清理资源时出错", e);
            // 不再抛出异常，避免测试中断
        } finally {
            log.info("=== 测试资源清理完成 ===\n");
        }
    }
    private boolean deleteIfExists(File file) {
        if (file != null && file.exists()) {
            try {
                boolean deleted = file.delete();
                if (!deleted) {
                    log.warn("无法删除文件: {}", file.getAbsolutePath());
                    // 在Windows上，有时需要强制关闭文件句柄
                    System.gc();
                    Thread.sleep(100);
                    deleted = file.delete();
                }
                if (deleted) {
                    log.info("成功删除文件: {}", file.getAbsolutePath());
                }
                return deleted;
            } catch (Exception e) {
                log.error("删除文件时出错: " + file.getAbsolutePath(), e);
                return false;
            }
        }
        return true;
    }
    /**
     * 测试间暂停，让系统资源恢复
     */
    private void pauseBetweenTests() {
        try {
            log.info("测试间暂停开始，等待系统资源恢复...");
            // 强制执行垃圾回收
            System.gc();
            // 等待一段时间让系统恢复
            Thread.sleep(1000);
            log.info("测试间暂停结束");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("测试暂停被中断", e);
        }
    }
    /**
     * 测试大量小命令快速写入
     */
    @Test
    void testHighFrequencySmallCommands() throws Exception {
        log.info("\n=== 开始测试高频小命令写入 ===");
        log.info("测试参数: 写入命令数量 = 100000");
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        // 生成10万个小命令并写入
        for (int i = 0; i < 100000; i++) {
            try {
                RespArray command = generateSetCommand("key" + i, "value" + i);
                aofManager.append(command);
                successCount++;
            } catch (Exception e) {
                log.error("写入命令失败，索引: {}", i, e);
            }
        }
        aofManager.flush();
        long endTime = System.currentTimeMillis();
        long fileSize = aofFile.length();
        log.info("测试结果:");
        log.info("- 总命令数: 100000");
        log.info("- 成功写入: {}", successCount);
        log.info("- 写入耗时: {}ms", (endTime - startTime));
        log.info("- 平均速度: {} 命令/秒", (successCount * 1000.0 / (endTime - startTime)));
        log.info("- 文件大小: {}KB", fileSize / 1024);
        // 验证数据恢复
        verifyDataRecovery();
        log.info("=== 高频小命令测试完成 ===\n");

        // 测试结束后暂停
        pauseBetweenTests();
    }
    /**
     * 测试大命令写入
     */
    @Test
    void testLargeCommands() throws Exception {
        log.info("\n=== 开始测试大命令写入 ===");
        log.info("测试参数:");
        log.info("- 命令数量: 10");
        log.info("- 每个命令大小: {}MB", LARGE_COMMAND_SIZE / (1024 * 1024));
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        long totalBytes = 0;
        // 生成10个大命令并写入
        for (int i = 0; i < 10; i++) {
            try {
                String largeValue = generateRandomString(LARGE_COMMAND_SIZE);
                RespArray command = generateSetCommand("largekey" + i, largeValue);
                aofManager.append(command);
                successCount++;
                totalBytes += LARGE_COMMAND_SIZE;
            } catch (Exception e) {
                log.error("写入大命令失败，索引: {}", i, e);
            }
        }
        aofManager.flush();
        long endTime = System.currentTimeMillis();
        long fileSize = aofFile.length();
        log.info("测试结果:");
        log.info("- 成功写入命令数: {}", successCount);
        log.info("- 写入总大小: {}MB", totalBytes / (1024 * 1024));
        log.info("- 写入耗时: {}ms", (endTime - startTime));
        log.info("- 写入速度: {}MB/s", (totalBytes / 1024.0 / 1024.0) / ((endTime - startTime) / 1000.0));
        log.info("- AOF文件大小: {}MB", fileSize / (1024 * 1024));
        // 验证数据恢复
        verifyDataRecovery();
        log.info("=== 大命令测试完成 ===\n");

        // 测试结束后暂停
        pauseBetweenTests();
    }
    /**
     * 测试高并发写入
     */
    @Test
    void testConcurrentWrites() throws Exception {
        log.info("\n=== 开始测试并发写入 ===");
        log.info("测试参数:");
        log.info("- 并发线程数: {}", CONCURRENT_THREADS);
        log.info("- 每线程命令数: {}", COMMANDS_PER_THREAD);
        log.info("- 总命令数: {}", CONCURRENT_THREADS * COMMANDS_PER_THREAD);
        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < COMMANDS_PER_THREAD; i++) {
                        try {
                            String key = "concurrent-key-" + threadId + "-" + i;
                            String value = "value-" + threadId + "-" + i;
                            RespArray command = generateSetCommand(key, value);
                            aofManager.append(command);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            log.error("线程{}写入失败，命令索引: {}", threadId, i, e);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        aofManager.flush();
        long endTime = System.currentTimeMillis();
        long fileSize = aofFile.length();
        log.info("测试结果:");
        log.info("- 总耗时: {}ms", (endTime - startTime));
        log.info("- 成功写入: {}", successCount.get());
        log.info("- 写入失败: {}", errorCount.get());
        log.info("- 写入速度: {} 命令/秒", (successCount.get() * 1000.0 / (endTime - startTime)));
        log.info("- AOF文件大小: {}KB", fileSize / 1024);
        // 验证数据恢复
        verifyDataRecovery();
        log.info("=== 并发写入测试完成 ===\n");

        // 测试结束后暂停
        pauseBetweenTests();
    }
    /**
     * 测试频繁关闭重启
     */
    @Test
    void testFrequentRestarts() throws Exception {
        log.info("开始测试频繁关闭重启...");
        long startTime = System.currentTimeMillis();
        List<String> keys = new ArrayList<>();
        // 执行10次关闭重启循环
        for (int cycle = 0; cycle < 10; cycle++) {
            // 每次写入1000个命令
            for (int i = 0; i < 1000; i++) {
                String key = "restart-key-" + cycle + "-" + i;
                String value = "value-" + cycle + "-" + i;
                RespArray command = generateSetCommand(key, value);
                aofManager.append(command);
                keys.add(key);
            }
            // 关闭并重启AOF管理器
            aofManager.flush();
            aofManager.close();
            aofManager = new AofManager(aofFile.getAbsolutePath(), redisCore);
            aofManager.load();
        }
        aofManager.flush();
        long endTime = System.currentTimeMillis();
        long fileSize = aofFile.length();
        log.info("频繁关闭重启测试完成，耗时: {}ms, 文件大小: {}KB",
                (endTime - startTime), fileSize / 1024);
        // 验证数据恢复
        verifyDataRecovery();

        // 测试结束后暂停
        pauseBetweenTests();
    }
    /**
     * 测试崩溃恢复
     */
    @Test
    void testCrashRecovery() throws Exception {
        log.info("\n=== 开始测试崩溃恢复 ===");
        log.info("测试参数: 写入命令数量 = {}", CRASH_RECOVERY_COMMANDS);
        // 写入一些数据
        for (int i = 0; i < CRASH_RECOVERY_COMMANDS; i++) {
            RespArray command = generateSetCommand("crash-key-" + i, "value-" + i);
            aofManager.append(command);
        }
        // 模拟崩溃前，确保数据已刷入磁盘
        aofManager.flush();
        // 记录原始文件大小
        long originalSize = aofFile.length();
        log.info("模拟崩溃前文件大小: {}KB", originalSize / 1024);
        // 模拟崩溃 - 正确关闭资源
        try {
            // 先关闭当前的aofManager
            if (aofManager != null) {
                aofManager.close();
                aofManager = null;
            }
            // 强制GC和等待
            System.gc();
            Thread.sleep(100);
            // 重新打开AOF文件
            redisCore = new RedisCoreImpl(DEFAULT_DB_COUNT, null);
            aofManager = new AofManager(aofFile.getAbsolutePath(), redisCore);
            // 加载并验证数据
            long loadStartTime = System.currentTimeMillis();
            aofManager.load();
            long loadEndTime = System.currentTimeMillis();
            log.info("崩溃恢复结果:");
            log.info("- 恢复耗时: {}ms", (loadEndTime - loadStartTime));
            log.info("- 文件大小: {}KB", aofFile.length() / 1024);
            // 验证数据恢复
            int recoveredKeys = verifyDataRecovery();
            log.info("- 恢复键数量: {}", recoveredKeys);
            // 验证恢复的数据量是否正确
            Assertions.assertEquals(CRASH_RECOVERY_COMMANDS, recoveredKeys,
                    "恢复的键数量应该等于写入的命令数");
        } catch (Exception e) {
            log.error("崩溃恢复测试失败", e);
            throw e;
        } finally {
            // 确保资源被释放
            if (aofManager != null) {
                try {
                    aofManager.close();
                    aofManager = null;
                } catch (Exception e) {
                    log.error("关闭AOF管理器时出错", e);
                }
            }
            // 强制GC和等待，帮助释放文件句柄
            System.gc();
            Thread.sleep(200);
        }
        log.info("=== 崩溃恢复测试完成 ===\n");

        // 测试结束后暂停
        pauseBetweenTests();
    }
    // 性能统计指标
    private static class PerformanceMetrics {
        long totalWriteTime;
        long totalBytes;
        int commandCount;
        int batchCount;
        double avgBatchSize;
        double maxLatency;
        double minLatency = Double.MAX_VALUE;
        double totalLatency;
        AtomicInteger backpressureCount = new AtomicInteger(0);
        AtomicInteger directWriteCount = new AtomicInteger(0);

        void recordLatency(long latencyNanos) {
            // 将纳秒转换为毫秒，提供更合理的延迟统计
            double latencyMs = latencyNanos / 1_000_000.0;
            maxLatency = Math.max(maxLatency, latencyMs);
            minLatency = Math.min(minLatency, latencyMs);
            totalLatency += latencyMs;
            commandCount++;
        }

        double getAvgLatency() {
            return commandCount > 0 ? totalLatency / commandCount : 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "性能统计:\n" +
                            "- 总写入时间: %d ms\n" +
                            "- 总写入字节: %.2f MB\n" +
                            "- 命令总数: %d\n" +
                            "- 批次总数: %d\n" +
                            "- 平均批次大小: %.2f\n" +
                            "- 最大延迟: %.3f ms\n" +
                            "- 最小延迟: %.3f ms\n" +
                            "- 平均延迟: %.3f ms\n" +
                            "- 背压触发次数: %d\n" +
                            "- 直写命令数: %d\n" +
                            "- 吞吐量: %.2f MB/s\n" +
                            "- 命令处理速率: %.2f 命令/秒",
                    totalWriteTime,
                    totalBytes / (1024.0 * 1024.0),
                    commandCount,
                    batchCount,
                    avgBatchSize,
                    maxLatency,
                    minLatency == Double.MAX_VALUE ? 0.0 : minLatency,
                    getAvgLatency(),
                    backpressureCount.get(),
                    directWriteCount.get(),
                    (totalBytes / (1024.0 * 1024.0)) / (totalWriteTime / 1000.0),
                    (commandCount * 1000.0) / totalWriteTime
            );
        }
    }

    /**
     * 测试直接写入和批量写入的对比
     */
    @Test
    void compareBatchVsDirectWrite() throws Exception {
        log.info("\n=== 开始性能对比测试 ===");
        // 准备测试数据
        int testSize = 100000;
        List<RespArray> commands = new ArrayList<>();
        for (int i = 0; i < testSize; i++) {
            commands.add(generateSetCommand("batch-key-" + i, "value-" + i));
        }
        // 直接写入性能指标
        PerformanceMetrics directMetrics = new PerformanceMetrics();
        // 批量写入性能指标
        PerformanceMetrics batchMetrics = new PerformanceMetrics();
        // 测试直接写入
        File directFile = new File(tempDir, "direct.aof");
        Writer directWriter = null;
        try {
            directWriter = new AofWriter(directFile, true, 0, null,redisCore);
            long startTime = System.currentTimeMillis();
            for (RespArray cmd : commands) {
                ByteBuf buf = Unpooled.buffer();
                try {
                    long cmdStartTime = System.nanoTime();
                    cmd.encode(cmd, buf);
                    directWriter.write(ByteBuffer.wrap(buf.array(), 0, buf.readableBytes()));
                    directMetrics.recordLatency(System.nanoTime() - cmdStartTime);
                    directMetrics.totalBytes += buf.readableBytes();
                } finally {
                    buf.release();
                }
            }
            directWriter.flush();
            directMetrics.totalWriteTime = System.currentTimeMillis() - startTime;
            directMetrics.commandCount = testSize;
            // 测试批量写入
            File batchFile = new File(tempDir, "batch.aof");
            Writer fileWriter = null;
            AofBatchWriter batchWriter = null;
            try {
                fileWriter = new AofWriter(batchFile, true, 0, null,redisCore);
                batchWriter = new AofBatchWriter(fileWriter, 1000);
                startTime = System.currentTimeMillis();
                for (RespArray cmd : commands) {
                    ByteBuf buf = Unpooled.buffer();
                    try {
                        long cmdStartTime = System.nanoTime();
                        cmd.encode(cmd, buf);
                        // 检查是否是大命令
                        if (buf.readableBytes() > LARGE_COMMAND_THRESHOLD) {
                            batchMetrics.directWriteCount.incrementAndGet();
                        }
                        batchWriter.write(buf);
                        batchMetrics.recordLatency(System.nanoTime() - cmdStartTime);
                        batchMetrics.totalBytes += buf.readableBytes();
                        batchMetrics.batchCount = (int) batchWriter.getBatchCount();
                        batchMetrics.avgBatchSize = (double) batchMetrics.totalBytes / batchMetrics.batchCount;
                    } catch (Exception e) {
                        buf.release();
                        throw e;
                    }
                }
                batchWriter.flush();
                batchMetrics.totalWriteTime = System.currentTimeMillis() - startTime;
                batchMetrics.commandCount = testSize;
                // 输出详细的性能对比
                log.info("\n=== 性能测试结果 ===");
                log.info("直接写入模式:\n{}", directMetrics);
                log.info("\n批量写入模式:\n{}", batchMetrics);
                // 计算性能提升
                double throughputImprovement =
                        ((batchMetrics.totalBytes / (double)batchMetrics.totalWriteTime) /
                                (directMetrics.totalBytes / (double)directMetrics.totalWriteTime) - 1) * 100;

                // 计算延迟改善（注意：较小的延迟更好）
                double directAvgLatency = directMetrics.getAvgLatency();
                double batchAvgLatency = batchMetrics.getAvgLatency();
                double latencyImprovement;

                if (directAvgLatency > 0 && batchAvgLatency > 0) {
                    latencyImprovement = ((directAvgLatency - batchAvgLatency) / directAvgLatency) * 100;
                } else {
                    latencyImprovement = 0.0;
                }

                log.info("\n性能提升:");
                if (throughputImprovement > 0) {
                    log.info("- 吞吐量提升: {}% (批量写入更快)", String.format("%.2f", throughputImprovement));
                } else {
                    log.info("- 吞吐量降低: {}% (直接写入更快)", String.format("%.2f", Math.abs(throughputImprovement)));
                }

                if (latencyImprovement > 0) {
                    log.info("- 平均延迟改善: {}% (批量写入延迟更低)", String.format("%.2f", latencyImprovement));
                } else {
                    log.info("- 平均延迟增加: {}% (直接写入延迟更低)", String.format("%.2f", Math.abs(latencyImprovement)));
                }
                // 验证数据一致性
                verifyDataConsistency(directFile, batchFile);
            } finally {
                if (batchWriter != null) {
                    batchWriter.close();
                }
                if (fileWriter != null) {
                    fileWriter.close();
                }
            }
        } finally {
            if (directWriter != null) {
                directWriter.close();
            }
        }

        // 测试结束后暂停
        pauseBetweenTests();
    }

    /**
     * 验证两个AOF文件的数据一致性
     */
    private void verifyDataConsistency(File file1, File file2) throws Exception {
        log.info("\n=== 验证数据一致性 ===");
        // 创建两个独立的RedisCore实例
        RedisCore core1 = new RedisCoreImpl(DEFAULT_DB_COUNT, null);
        RedisCore core2 = new RedisCoreImpl(DEFAULT_DB_COUNT, null);
        // 加载并比较数据
        AofManager manager1 = new AofManager(file1.getAbsolutePath(), core1);
        AofManager manager2 = new AofManager(file2.getAbsolutePath(), core2);
        try {
            manager1.load();
            manager2.load();
            // 比较所有数据库的数据
            RedisDB[] dbs1 = core1.getDataBases();
            RedisDB[] dbs2 = core2.getDataBases();
            int totalKeys = 0;
            int mismatchCount = 0;
            for (int i = 0; i < dbs1.length; i++) {
                RedisDB db1 = dbs1[i];
                RedisDB db2 = dbs2[i];
                totalKeys += db1.size();
                // 比较键值对
                for (RedisBytes key : db1.keys()) {
                    if (!db2.exist(key)) {
                        mismatchCount++;
                        log.error("数据不一致: key={}存在于file1但不存在于file2", new String(key.getBytes()));
                    } else {
                        // 获取两边的值对象
                        RedisString value1 = (RedisString) db1.get(key);
                        RedisString value2 = (RedisString)db2.get(key);

                        // 比较字节数组内容而非对象引用
                        if (!(value1.getValue()).equals(value2.getValue())) {
                            mismatchCount++;
                            log.error("数据不一致: key={} 值不同", new String(key.getBytes()));
                        }
                    }
                }
            }
            log.info("数据一致性检查结果:");
            log.info("- 总键数: {}", totalKeys);
            log.info("- 不一致数: {}", mismatchCount);
            Assertions.assertEquals(0, mismatchCount, "数据一致性检查失败");
        } finally {
            manager1.close();
            manager2.close();
        }
    }

    /**
     * 慢速写入器，用于测试背压机制
     */
    private static class SlowWriter implements Writer {
        private final AofWriter realWriter;
        private final AtomicInteger writeCount = new AtomicInteger(0);
        
        public SlowWriter(final File file, final RedisCore redisCore) throws IOException {
            this.realWriter = new AofWriter(file, true, 0, null, redisCore);
        }
        
        @Override
        public int write(final ByteBuffer buffer) throws IOException {
            // 模拟写入延迟（增加延迟，但保持在合理范围内）
            try {
                final int count = writeCount.incrementAndGet();
                // 每10次写入增加额外延迟，模拟突发性能下降
                if (count % 10 == 0) {
                    Thread.sleep(40); // 增加到40ms以增加背压可能性
                } else {
                    Thread.sleep(15); // 增加到15ms
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return realWriter.write(buffer);
        }
        
        @Override
        public void flush() throws IOException {
            realWriter.flush();
        }
        
        @Override
        public void close() throws IOException {
            realWriter.close();
        }

        @Override
        public boolean bgrewrite() throws IOException {
            return realWriter.bgrewrite();
        }
    }

    /**
     * 辅助方法：生成SET命令的RESP数组
     */
    private RespArray generateSetCommand(String key, String value) {
        BulkString[] array = new BulkString[3];
        array[0] = new BulkString("SET".getBytes());
        array[1] = new BulkString(key.getBytes());
        array[2] = new BulkString(value.getBytes());
        return new RespArray(array);
    }
    /**
     * 辅助方法：生成随机字符串
     */
    private String generateRandomString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = (char) ('a' + random.nextInt(26));
            sb.append(c);
        }
        return sb.toString();
    }
    /**
     * 辅助方法：验证数据恢复
     */
    private int verifyDataRecovery() throws Exception {
        log.info("\n--- 开始验证数据恢复 ---");
        // 关闭当前管理器
        if (aofManager != null) {
            aofManager.flush();
            aofManager.close();
        }
        // 创建新的RedisCore和AofManager
        RedisCore newCore = new RedisCoreImpl(DEFAULT_DB_COUNT, null);
        AofManager newManager = null;
        int totalKeys = 0;  // 将变量声明移到try块外部
        try {
            newManager = new AofManager(aofFile.getAbsolutePath(), newCore);
            // 加载AOF文件
            long startTime = System.currentTimeMillis();
            newManager.load();
            long endTime = System.currentTimeMillis();
            // 检查恢复的数据量
            RedisDB[] databases = newCore.getDataBases();
            for (RedisDB db : databases) {
                totalKeys += db.size();
            }
            log.info("数据恢复结果:");
            log.info("- 恢复耗时: {}ms", (endTime - startTime));
            log.info("- 恢复键数量: {}", totalKeys);
            log.info("- AOF文件大小: {}KB", aofFile.length() / 1024);
        } finally {
            if (newManager != null) {
                try {
                    newManager.close();
                } catch (Exception e) {
                    log.error("关闭新的AofManager时出错", e);
                }
            }
        }
        log.info("--- 数据恢复验证完成 ---\n");
        return totalKeys;
    }

    @Override
    protected void finalize() throws Throwable {
        // 在对象被回收时确保资源被释放
        try {
            if (aofManager != null) {
                aofManager.close();
            }
        } finally {
            super.finalize();
        }
    }
}
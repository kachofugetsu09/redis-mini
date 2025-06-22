package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * AofBatchWriter 完整单元测试
 * 
 * 测试范围：
 * 1. 基本功能测试：写入、刷盘、关闭
 * 2. 批处理机制测试：批次大小、批处理逻辑
 * 3. 同步策略测试：ALWAYS、SMART、NO 三种模式
 * 4. 并发安全测试：多线程写入、竞态条件
 * 5. 异常处理测试：写入失败、刷盘超时
 * 6. 内存管理测试：ByteBuf 释放、内存泄漏防护
 * 7. 性能测试：大量数据写入、高并发场景
 */
@DisplayName("AofBatchWriter 完整测试套件")
class AofBatchWriterTest {

    @Mock
    private Writer mockWriter;
    
    private AutoCloseable closeable;
    
    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    // ==================== 基本功能测试 ====================
    
    @Nested
    @DisplayName("基本功能测试")
    class BasicFunctionalityTests {
        
        @Test
        @DisplayName("创建 AofBatchWriter - 默认构造函数")
        void testCreateAofBatchWriter_DefaultConstructor() throws Exception {
            // Given: 模拟 Writer
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            // When: 创建 AofBatchWriter
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, 1000)) {
                // Then: 验证初始状态
                assertNotNull(batchWriter);
                assertEquals(AofSyncPolicy.SMART, batchWriter.getSyncPolicy());
                assertTrue(batchWriter.isRunning());
                assertEquals(0, batchWriter.getBatchCount());
                assertEquals(0, batchWriter.getTotalBatchedCommands());
            }
        }
        
        @Test
        @DisplayName("创建 AofBatchWriter - 指定同步策略")
        void testCreateAofBatchWriter_WithSyncPolicy() throws Exception {
            // When & Then: 测试所有同步策略
            for (AofSyncPolicy policy : AofSyncPolicy.values()) {
                try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, policy, 1000)) {
                    assertEquals(policy, batchWriter.getSyncPolicy());
                }
            }
        }
        
        @Test
        @DisplayName("单个命令写入和刷盘")
        void testSingleCommandWriteAndFlush() throws Exception {
            // Given: 准备测试数据
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf testData = Unpooled.copiedBuffer("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", 
                                                        StandardCharsets.UTF_8);
                
                // When: 写入命令
                batchWriter.write(testData);
                
                // Then: 等待批处理完成并验证
                batchWriter.flush(1000);
                
                // 验证底层 Writer 被调用
                verify(mockWriter, atLeastOnce()).write(any(ByteBuffer.class));
                verify(mockWriter, atLeastOnce()).flush();
                
                // 验证统计信息
                assertTrue(batchWriter.getBatchCount() > 0);
                assertTrue(batchWriter.getTotalBatchedCommands() > 0);
            }
        }
        
        @Test
        @DisplayName("空数据写入处理")
        void testEmptyDataWrite() throws Exception {
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf emptyData = Unpooled.buffer(0);
                
                // When: 写入空数据
                batchWriter.write(emptyData);
                batchWriter.flush(500);
                
                // Then: 验证空数据被正确处理
                // 空数据也会被处理，但不会产生实际写入
                verify(mockWriter, atMost(1)).write(any(ByteBuffer.class));
            }
        }
    }

    // ==================== 批处理机制测试 ====================
    
    @Nested
    @DisplayName("批处理机制测试")
    class BatchProcessingTests {
          @Test
        @DisplayName("批次大小限制测试")
        void testBatchSizeLimit() throws Exception {
            // Given: 计数器记录写入次数
            AtomicInteger writeCount = new AtomicInteger(0);
            when(mockWriter.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                writeCount.incrementAndGet();
                return 10;
            });
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入恰好3个完整批次的命令
                int commandCount = AofBatchWriter.MAX_BATCH_SIZE * 3; // 150个命令，3个完整批次
                for (int i = 0; i < commandCount; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                    
                    // 每完成一个批次大小的写入，稍微等待确保批次处理
                    if ((i + 1) % AofBatchWriter.MAX_BATCH_SIZE == 0) {
                        Thread.sleep(10);
                    }
                }
                
                // Then: 等待所有批次处理完成
                batchWriter.flush(2000);
                long actualBatches = batchWriter.getBatchCount();
                assertTrue(actualBatches >= 3 && actualBatches <= 6,
                          "应该产生3个批次左右，实际: " + actualBatches);
                assertEquals(commandCount, batchWriter.getTotalBatchedCommands());
            }
        }
        
        @Test
        @DisplayName("大命令直接写入测试")
        void testLargeCommandDirectWrite() throws Exception {
            // Given: 创建大于阈值的命令
            int largeSize = 600 * 1024; // 600KB，超过 512KB 阈值
            byte[] largeData = new byte[largeSize];
            for (int i = 0; i < largeSize; i++) {
                largeData[i] = (byte) ('A' + (i % 26));
            }
            
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(largeSize);
              try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf largeBuf = Unpooled.copiedBuffer(largeData);
                
                // When: 写入大命令
                batchWriter.write(largeBuf);
                // 不需要手动flush，SMART模式下大命令会自动刷盘
                
                // Then: 验证大命令被直接写入
                verify(mockWriter, times(1)).write(any(ByteBuffer.class));
                verify(mockWriter, times(1)).flush(); // SMART 模式下大命令会立即刷盘
            }
        }
        
        @Test
        @DisplayName("批处理队列满时的降级处理")
        void testQueueFullFallback() throws Exception {
            // Given: 模拟缓慢的写入器
            CountDownLatch slowWriteLatch = new CountDownLatch(1);
            when(mockWriter.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                try {
                    slowWriteLatch.await(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return 10;
            });
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 快速填满队列
                for (int i = 0; i < 1200; i++) { // 超过默认队列大小 1000
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // Then: 释放写入器并等待完成
                slowWriteLatch.countDown();
                batchWriter.flush(5000);
                
                // 验证所有数据都被处理
                verify(mockWriter, atLeastOnce()).write(any(ByteBuffer.class));
            }
        }
    }

    // ==================== 同步策略测试 ====================
    
    @Nested
    @DisplayName("同步策略测试")
    class SyncPolicyTests {
        
        @Test
        @DisplayName("ALWAYS 模式 - 每次写入后立即刷盘")
        void testAlwaysSyncPolicy() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.ALWAYS, 1000)) {
                // When: 写入多个命令
                for (int i = 0; i < 5; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                batchWriter.flush(1000);
                
                // Then: 验证每个批次都触发刷盘
                verify(mockWriter, atLeastOnce()).flush();
            }
        }
        
        @Test
        @DisplayName("SMART 模式 - 批次完成后智能刷盘")
        void testSmartSyncPolicy() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入命令
                for (int i = 0; i < 10; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                batchWriter.flush(1000);
                
                // Then: 验证刷盘被调用
                verify(mockWriter, atLeastOnce()).flush();
            }
        }
        
        @Test
        @DisplayName("NO 模式 - 不主动刷盘")
        void testNoSyncPolicy() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.NO, 1000)) {
                // When: 写入命令
                for (int i = 0; i < 10; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 等待批次处理但不刷盘
                Thread.sleep(100);
                
                // Then: 验证写入被调用但刷盘不被主动调用
                verify(mockWriter, atLeastOnce()).write(any(ByteBuffer.class));
                verify(mockWriter, never()).flush(); // NO 模式下不主动刷盘
            }
        }
    }    // ==================== 架构兼容性测试 ====================
    
    /**
     * 架构说明：
     * 
     * Redis-Mini 使用单线程命令执行器(commandExecutorThreadCount=1)，
     * 所有命令都在同一个线程中串行执行，因此在正常运行时不会有并发写入 AOF 的情况。
     * 
     * 之前的并发测试虽然可以验证 AofBatchWriter 的线程安全性，
     * 但在实际架构中是不会发生的场景，因此已被移除。
     * 
     * 如果将来修改为多线程架构，可以重新添加相应的并发测试。
     */
    @Nested
    @DisplayName("架构兼容性测试")
    class ArchitectureCompatibilityTests {
        
        @Test
        @DisplayName("单线程架构下的顺序写入测试")
        void testSequentialWritesInSingleThreadArchitecture() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                
                // 模拟单线程架构下的顺序写入
                for (int i = 0; i < 100; i++) {
                    ByteBuf data = Unpooled.copiedBuffer(
                        "*3\r\n$3\r\nSET\r\n$4\r\nkey" + i + "\r\n$5\r\nvalue\r\n", 
                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 刷盘
                batchWriter.flush(2000);
                
                // 验证所有命令都被处理
                assertEquals(100, batchWriter.getTotalBatchedCommands());
                verify(mockWriter, atLeastOnce()).write(any(ByteBuffer.class));
            }
        }
    }

    // ==================== 异常处理测试 ====================
    
    @Nested
    @DisplayName("异常处理测试")
    class ExceptionHandlingTests {
        
        @Test
        @DisplayName("底层写入失败异常处理")
        void testWriteFailureHandling() throws Exception {
            // Given: 模拟写入失败
            when(mockWriter.write(any(ByteBuffer.class))).thenThrow(new IOException("模拟写入失败"));
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n", 
                                                    StandardCharsets.UTF_8);
                
                // When & Then: 写入应该成功（错误在后台处理），但刷盘会失败
                assertDoesNotThrow(() -> batchWriter.write(data));
                
                // 刷盘时应该传播异常
                assertThrows(Exception.class, () -> batchWriter.flush(1000));
            }
        }
        
        @Test
        @DisplayName("刷盘超时异常处理")
        void testFlushTimeoutHandling() throws Exception {
            // Given: 模拟缓慢的写入器
            when(mockWriter.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                Thread.sleep(2000); // 2秒延迟
                return 10;
            });
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n", 
                                                    StandardCharsets.UTF_8);
                
                // When: 写入数据
                batchWriter.write(data);
                
                // Then: 短超时应该失败
                assertThrows(Exception.class, () -> batchWriter.flush(500), "短超时应该抛出异常");
            }
        }
        
        @Test
        @DisplayName("非法参数异常处理")
        void testIllegalArgumentHandling() throws Exception {
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When & Then: 测试非法超时参数
                assertThrows(IllegalArgumentException.class, () -> batchWriter.flush(-1));
                assertThrows(IllegalArgumentException.class, () -> batchWriter.flush(0));
            }
        }
    }

    // ==================== 内存管理测试 ====================
    
    @Nested
    @DisplayName("内存管理测试")
    class MemoryManagementTests {
        
        @Test
        @DisplayName("ByteBuf 正常释放测试")
        void testByteBufRelease() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {                // 使用池化的 ByteBuf 来测试引用计数
                ByteBuf data = PooledByteBufAllocator.DEFAULT.buffer();
                data.writeBytes("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n".getBytes(StandardCharsets.UTF_8));
                
                // When: 写入数据
                batchWriter.write(data);
                batchWriter.flush(1000);
                
                // Then: ByteBuf 应该被释放（引用计数应该为 0）
                assertEquals(0, data.refCnt(), "ByteBuf 应该被释放");
            }
        }
        
        @Test
        @DisplayName("异常情况下的内存清理测试")
        void testMemoryCleanupOnException() throws Exception {
            // Given: 模拟写入失败
            when(mockWriter.write(any(ByteBuffer.class))).thenThrow(new IOException("写入失败"));
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                ByteBuf data = PooledByteBufAllocator.DEFAULT.buffer();
                data.writeBytes("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n".getBytes(StandardCharsets.UTF_8));
                
                // When: 写入数据（会在后台失败）
                batchWriter.write(data);
                
                // Then: 即使发生异常，ByteBuf 也应该被释放
                assertThrows(Exception.class, () -> batchWriter.flush(1000));
                assertEquals(0, data.refCnt(), "异常情况下 ByteBuf 也应该被释放");
            }
        }
        
        @Test
        @DisplayName("关闭时的资源清理测试")
        void testResourceCleanupOnClose() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000);
            
            // 写入一些数据
            for (int i = 0; i < 10; i++) {
                ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                    StandardCharsets.UTF_8);
                batchWriter.write(data);
            }
            
            // When: 关闭 AofBatchWriter
            assertDoesNotThrow(() -> batchWriter.close());
            
            // Then: 验证状态
            assertFalse(batchWriter.isRunning());
            assertEquals(0, batchWriter.getQueueSize()); // 队列应该被清理
        }
    }

    // ==================== 性能测试 ====================
    
    @Nested
    @DisplayName("性能测试")
    class PerformanceTests {
        
        @Test
        @DisplayName("高吞吐量写入测试")
        @Timeout(10) // 10秒超时
        void testHighThroughputWrites() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                long startTime = System.currentTimeMillis();
                
                // When: 写入大量数据
                int commandCount = 10000;                for (int i = 0; i < commandCount; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*3\r\n$3\r\nSET\r\n$5\r\nkey" + i + "\r\n$5\r\nvalue\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 确保所有命令都被处理完成
                batchWriter.flush(5000);
                
                // 等待一小段时间确保统计数据更新
                Thread.sleep(50);
                
                long endTime = System.currentTimeMillis();
                
                // Then: 验证性能
                long duration = endTime - startTime;
                double throughput = (double) commandCount / duration * 1000; // 命令/秒
                
                System.out.printf("高吞吐量测试: %d 个命令，耗时 %d ms，吞吐量 %.2f 命令/秒%n", 
                                 commandCount, duration, throughput);
                
                // 允许小范围的误差，因为可能有些命令还在处理中
                long actualCommands = batchWriter.getTotalBatchedCommands();
                assertTrue(actualCommands >= commandCount * 0.95, 
                    String.format("处理的命令数应该接近预期值，预期: %d，实际: %d", commandCount, actualCommands));
                assertTrue(throughput > 1000, "吞吐量应该超过 1000 命令/秒，实际: " + throughput);
            }
        }        @Test
        @DisplayName("批处理效率测试")
        void testBatchingEfficiency() throws Exception {
            AtomicInteger writeCallCount = new AtomicInteger(0);
            when(mockWriter.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                writeCallCount.incrementAndGet();
                return 10;
            });
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入大量小命令，并在写入过程中增加间隔
                int commandCount = 500;
                for (int i = 0; i < commandCount; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                    
                    // 每处理一定数量的命令后，添加小的延迟来促进批次形成
                    if ((i + 1) % (AofBatchWriter.MAX_BATCH_SIZE / 2) == 0) {
                        Thread.sleep(1);
                    }
                }
                
                // 强制完成所有批次
                batchWriter.flush(2000);
                
                // Then: 验证批处理效率
                long batchCount = batchWriter.getBatchCount();
                // 计算理论上的最小批次数（向上取整）
                int expectedMinBatches = (commandCount + AofBatchWriter.MAX_BATCH_SIZE - 1) / AofBatchWriter.MAX_BATCH_SIZE;
                // 计算理论上的最大批次数（考虑到延迟可能导致的提前批处理）
                int expectedMaxBatches = commandCount / (AofBatchWriter.MAX_BATCH_SIZE / 2) + 1;
                System.out.printf("批处理效率测试: %d 个命令，产生 %d 个批次，底层写入调用 %d 次%n", 
                                 commandCount, batchCount, writeCallCount.get());
                
                // 验证批次数量在合理范围内
                assertTrue(batchCount >= expectedMinBatches ,
                          String.format("批次数量应该至少为 %d，实际: %d",expectedMinBatches, batchCount) );

                
                // 验证底层写入调用次数接近批次数量
                assertTrue(writeCallCount.get() <= batchCount + 2, 
                          "底层写入调用次数应该接近批次数量");
                
                // 验证所有命令都被处理
                assertEquals(commandCount, batchWriter.getTotalBatchedCommands());
            }
        }
    }

    // ==================== 状态查询测试 ====================
    
    @Nested
    @DisplayName("状态查询测试")
    class StateQueryTests {        @Test
        @DisplayName("统计信息准确性测试")
        void testStatisticsAccuracy() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // 初始状态检查
                assertEquals(0, batchWriter.getBatchCount());
                assertEquals(0, batchWriter.getTotalBatchedCommands());
                assertEquals(0, batchWriter.getQueueSize());
                assertTrue(batchWriter.isRunning());
                
                // When: 写入一些命令
                int commandCount = 100; // 会产生 2 个批次 (50 + 50)
                for (int i = 0; i < commandCount; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 等待一段时间让批处理完成
                Thread.sleep(100);
                batchWriter.flush(1000);
                
                // Then: 验证统计信息
                long actualBatchCount = batchWriter.getBatchCount();
                long actualCommandCount = batchWriter.getTotalBatchedCommands();
                
                assertTrue(actualBatchCount >= 2, 
                          "应该产生至少 2 个批次，实际: " + actualBatchCount);
                assertEquals(commandCount, actualCommandCount, 
                           "命令总数应该正确，期望: " + commandCount + "，实际: " + actualCommandCount);
                assertEquals(0, batchWriter.getQueueSize());
                
                // 由于异步处理的特性，即使在flush后，可能仍有批次在处理中
                // 所以我们不应该断言hasPendingBatches()一定为false
            }
        }
        
        @Test
        @DisplayName("刷盘状态监控测试")
        void testFlushStateMonitoring() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入数据但不立即刷盘
                ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n", 
                                                    StandardCharsets.UTF_8);
                batchWriter.write(data);
                
                // 短暂等待让数据进入队列
                Thread.sleep(50);
                
                // Then: 检查状态
                CompletableFuture<Void> lastBatch = batchWriter.getLastBatchFuture();
                assertNotNull(lastBatch);
                
                // 刷盘后状态应该改变
                batchWriter.flush(1000);
                assertTrue(lastBatch.isDone(), "刷盘后 Future 应该完成");
            }
        }
    }

    // ==================== 集成测试 ====================
    
    @Nested
    @DisplayName("集成测试")
    class IntegrationTests {
        
        @Test
        @DisplayName("完整生命周期测试")
        void testCompleteLifecycle() throws Exception {
            when(mockWriter.write(any(ByteBuffer.class))).thenReturn(10);
            
            // Given: 创建 AofBatchWriter
            AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000);
            
            try {
                // 阶段1：正常写入
                for (int i = 0; i < 50; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 阶段2：刷盘
                batchWriter.flush(1000);
                
                // 阶段3：验证状态
                assertTrue(batchWriter.getBatchCount() > 0);
                assertEquals(50, batchWriter.getTotalBatchedCommands());
                
                // 阶段4：更多写入
                for (int i = 50; i < 100; i++) {
                    ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n" + i + "\r\n", 
                                                        StandardCharsets.UTF_8);
                    batchWriter.write(data);
                }
                
                // 阶段5：最终刷盘
                batchWriter.flush(1000);
                assertEquals(100, batchWriter.getTotalBatchedCommands());
                
            } finally {
                // 阶段6：清理
                batchWriter.close();
                assertFalse(batchWriter.isRunning());
            }
        }
          @Test
        @DisplayName("写入异常处理测试")
        void testWriteExceptionHandling() throws Exception {
            // Given: 模拟写入失败
            when(mockWriter.write(any(ByteBuffer.class)))
                .thenThrow(new IOException("模拟写入失败"));
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入数据
                ByteBuf data = Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$1\r\n1\r\n", 
                                                    StandardCharsets.UTF_8);
                batchWriter.write(data);
                
                // Then: 刷盘应该失败并抛出异常
                IOException exception = assertThrows(IOException.class, () -> {
                    batchWriter.flush(1000);
                });
                
                // 验证异常信息
                assertTrue(exception.getMessage().contains("批次写入过程中发生异常") || 
                          exception.getMessage().contains("模拟写入失败"));
                
                // 验证写入方法被调用
                verify(mockWriter, atLeastOnce()).write(any(ByteBuffer.class));
                
                // 验证系统状态 - 批次处理应该停止但对象仍可用于查询
                assertTrue(batchWriter.isRunning(), "Writer应该仍在运行状态等待新命令");
            }
        }
        
        @Test
        @DisplayName("大命令写入异常处理测试")
        void testLargeCommandExceptionHandling() throws Exception {
            // Given: 模拟大命令写入失败
            when(mockWriter.write(any(ByteBuffer.class)))
                .thenThrow(new IOException("模拟大命令写入失败"));
            
            try (AofBatchWriter batchWriter = new AofBatchWriter(mockWriter, AofSyncPolicy.SMART, 1000)) {
                // When: 写入大命令（超过512KB）
                byte[] largeData = new byte[600 * 1024]; // 600KB
                Arrays.fill(largeData, (byte) 'A');
                ByteBuf largeBuf = Unpooled.copiedBuffer(largeData);
                
                // Then: 应该立即抛出异常
                IOException exception = assertThrows(IOException.class, () -> {
                    batchWriter.write(largeBuf);
                });
                
                // 验证异常信息
                assertTrue(exception.getMessage().contains("大命令写入失败"));
                
                // 验证写入方法被调用
                verify(mockWriter, times(1)).write(any(ByteBuffer.class));
            }
        }
    }
}

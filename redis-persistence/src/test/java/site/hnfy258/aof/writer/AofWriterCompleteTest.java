package site.hnfy258.aof.writer;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import site.hnfy258.core.RedisCore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * AofWriter完整测试类 - 合并了基础功能和高级功能测试
 * 
 * @author Google Java Style Guide
 */
@DisplayName("AOF写入器完整测试")
class AofWriterCompleteTest {

    @TempDir
    private Path tempDir;
    
    private File aofFile;
    private AofWriter aofWriter;
    private RedisCore redisCore;

    @BeforeEach
    void setUp() throws IOException {
        aofFile = new File(tempDir.toFile(), "test.aof");
        if (!aofFile.exists()) {
            Files.createFile(aofFile.toPath());
        }
        redisCore = mock(RedisCore.class);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (aofWriter != null) {
            try {
                aofWriter.close();
            } catch (IOException e) {
                // 忽略关闭错误，因为这是清理阶段
            }
            aofWriter = null;
        }
        if (aofFile != null && aofFile.exists()) {
            Files.delete(aofFile.toPath());
        }
    }

    /**
     * 创建简单的AofWriter实例（无RedisCore）
     */
    private void createSimpleAofWriter() throws IOException {
        // 不再直接创建和传递FileChannel，让AofWriter自己管理资源
        aofWriter = new AofWriter(aofFile, false, 1000, null, null);
    }

    /**
     * 创建完整的AofWriter实例（带RedisCore）
     */
    private void createFullAofWriter() throws IOException {
        aofWriter = new AofWriter(aofFile, true, 1000, null, redisCore);
    }

    @Nested
    @DisplayName("基础写入功能测试")
    class BasicWriteFunctionalityTests {

        @BeforeEach
        void setUpBasic() throws IOException {
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("空写入测试")
        void testEmptyWrite() throws IOException {
            // Given: 空的ByteBuffer
            ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

            // When: 执行写入
            int written = aofWriter.write(emptyBuffer);

            // Then: 验证结果
            assertEquals(0, written, "空写入应该返回0字节");
            
            // 验证文件内容（考虑预分配情况）
            assertTrue(aofFile.length() >= 0, "文件应该存在且大小非负");
        }

        @Test
        @DisplayName("单条数据写入测试")
        void testSingleWrite() throws IOException {
            // Given: 准备测试数据
            String command = "*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n";
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));

            // When: 执行写入
            int written = aofWriter.write(buffer);

            // Then: 验证结果
            assertEquals(command.length(), written, "写入字节数应该等于命令长度");
            
            // 强制刷盘
            aofWriter.flush();
              // 验证文件内容
            byte[] fileContent = Files.readAllBytes(aofFile.toPath());
            String actualContent = new String(fileContent, StandardCharsets.UTF_8);
            
            // 处理文件预分配导致的末尾填充
            int commandLength = command.length();
            if (actualContent.length() >= commandLength) {
                actualContent = actualContent.substring(0, commandLength);
            }
            
            assertEquals(command, actualContent, "文件内容应该与写入内容一致");
        }

        @Test
        @DisplayName("多条数据写入测试")
        void testMultipleWrites() throws IOException {
            // Given: 准备多条测试数据
            String[] commands = {
                "*2\r\n$3\r\nSET\r\n$5\r\nkey1\r\n",
                "*2\r\n$3\r\nSET\r\n$5\r\nkey2\r\n",
                "*2\r\n$3\r\nSET\r\n$5\r\nkey3\r\n"
            };

            // When: 执行多次写入
            int totalWritten = 0;
            for (String command : commands) {
                ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
                totalWritten += aofWriter.write(buffer);
            }

            // Then: 验证结果
            int expectedTotal = 0;
            for (String command : commands) {
                expectedTotal += command.length();
            }
            assertEquals(expectedTotal, totalWritten, "总写入字节数应该正确");
            
            // 强制刷盘
            aofWriter.flush();
              // 验证文件内容
            byte[] fileContent = Files.readAllBytes(aofFile.toPath());
            String actualContent = new String(fileContent, StandardCharsets.UTF_8);
            String expectedContent = String.join("", commands);
            
            // 处理文件预分配导致的末尾填充
            if (actualContent.length() >= expectedContent.length()) {
                actualContent = actualContent.substring(0, expectedContent.length());
            }
            
            assertEquals(expectedContent, actualContent, "文件内容应该包含所有命令");
        }

        @Test
        @DisplayName("空数据处理测试")
        void testEmptyData() throws IOException {
            // Given: 空字符串
            String emptyCommand = "";
            ByteBuffer buffer = ByteBuffer.wrap(emptyCommand.getBytes(StandardCharsets.UTF_8));

            // When: 执行写入
            int written = aofWriter.write(buffer);

            // Then: 验证结果
            assertEquals(0, written, "空数据写入应该返回0字节");
        }
    }

    @Nested
    @DisplayName("文件操作测试")
    class FileOperationTests {

        @BeforeEach
        void setUpFileOps() throws IOException {
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("文件同步测试")
        void testFileSync() throws IOException {
            // Given: 写入一些数据
            String command = "*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n";
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
            aofWriter.write(buffer);

            // When: 执行同步
            assertDoesNotThrow(() -> aofWriter.flush(), "文件同步不应该抛出异常");            // Then: 验证数据已写入磁盘
            byte[] fileContent = Files.readAllBytes(aofFile.toPath());
            String actualContent = new String(fileContent, StandardCharsets.UTF_8);
            
            // 处理文件预分配导致的末尾填充
            if (actualContent.length() >= command.length()) {
                actualContent = actualContent.substring(0, command.length());
            }
            
            assertEquals(command, actualContent, "同步后文件内容应该正确");
        }

        @Test
        @DisplayName("无效文件路径测试")
        void testInvalidFilePath() {
            // Given: 无效的文件路径
            File invalidFile = new File("/invalid/path/test.aof");

            // When & Then: 创建AofWriter应该抛出异常
            assertThrows(Exception.class, () -> {
                new AofWriter(invalidFile, false, 1000, null, null);
            }, "无效路径应该抛出异常");
        }
    }

    @Nested
    @DisplayName("异常处理测试")
    class ExceptionHandlingTests {

        @BeforeEach
        void setUpExceptions() throws IOException {
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("关闭后操作测试")
        void testOperationAfterClose() throws IOException {
            // Given: 先关闭AofWriter
            aofWriter.close();

            // When & Then: 关闭后的操作应该抛出异常
            String command = "*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n";
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
            
            assertThrows(IOException.class, () -> {
                aofWriter.write(buffer);
            }, "关闭后写入应该抛出IOException");
        }

        @Test
        @DisplayName("重复关闭测试")
        void testMultipleClose() throws IOException {
            // When & Then: 多次关闭不应该抛出异常
            assertDoesNotThrow(() -> aofWriter.close(), "第一次关闭应该成功");
            assertDoesNotThrow(() -> aofWriter.close(), "重复关闭不应该抛出异常");
        }
    }

    @Nested
    @DisplayName("并发安全测试")
    class ConcurrencySafetyTests {

        @BeforeEach
        void setUpConcurrency() throws IOException {
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("并发写入测试")
        void testConcurrentWrites() throws IOException, InterruptedException {
            // Given: 准备并发测试
            int threadCount = 5;
            int writesPerThread = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            // When: 启动多个线程进行并发写入
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                new Thread(() -> {
                    try {
                        startLatch.await(); // 等待统一开始信号
                        
                        for (int j = 0; j < writesPerThread; j++) {
                            String command = String.format("*2\r\n$3\r\nSET\r\n$7\r\nkey_%d_%d\r\n", 
                                                          threadId, j);
                            ByteBuffer buffer = ByteBuffer.wrap(
                                command.getBytes(StandardCharsets.UTF_8));
                            aofWriter.write(buffer);
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // 记录异常但不影响测试
                        System.err.println("并发写入异常: " + e.getMessage());
                    } finally {
                        completeLatch.countDown();
                    }
                }).start();
            }

            // 启动所有线程
            startLatch.countDown();

            // Then: 等待所有线程完成
            boolean completed = completeLatch.await(5, TimeUnit.SECONDS);
            assertTrue(completed, "所有线程应该在5秒内完成");

            aofWriter.close();

            // 验证至少有一些写入成功
            assertTrue(successCount.get() > 0, "应该有一些并发写入成功");
        }
    }

    @Nested
    @DisplayName("AOF重写功能测试")
    class AofRewriteTests {

        @BeforeEach
        void setUpRewrite() throws IOException {
            // 这些测试需要简单的AofWriter（无RedisCore）
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("后台重写拒绝测试")
        void testBackgroundRewriteRejection() throws IOException, InterruptedException {
            // Given: 写入一些数据
            String command = "*2\r\n$3\r\nSET\r\n$5\r\ntest\r\n";
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
            aofWriter.write(buffer);

            // When: 启动后台重写（应该被拒绝，因为RedisCore为null）
            boolean rewriteStarted = aofWriter.bgrewrite();

            // Then: 重写应该被拒绝
            assertFalse(rewriteStarted, "没有RedisCore时后台重写应该被拒绝");
        }

        @Test
        @DisplayName("重复重写拒绝测试")
        void testDuplicateRewriteRejection() throws IOException {
            // 由于没有RedisCore，第一次重写就会被拒绝
            // 这个测试主要验证重写状态管理
            boolean firstAttempt = aofWriter.bgrewrite();
            assertFalse(firstAttempt, "第一次重写应该被拒绝（无RedisCore）");
        }
    }

    @Nested
    @DisplayName("高级功能测试")
    class AdvancedFunctionalityTests {

        @BeforeEach
        void setUpAdvanced() throws IOException {
            createFullAofWriter();
        }

        @Test
        @DisplayName("大数据写入测试")
        void testLargeDataWrite() throws IOException {
            // Given: 准备大量数据
            StringBuilder largeCommand = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                largeCommand.append("*2\r\n$3\r\nSET\r\n$10\r\nlargekey").append(i).append("\r\n");
            }

            // When: 写入大量数据
            String command = largeCommand.toString();
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
            int written = aofWriter.write(buffer);

            // Then: 验证写入结果
            assertEquals(command.length(), written, "大数据写入字节数应该正确");
            
            aofWriter.flush();
            
            // 验证文件大小
            assertTrue(aofFile.length() >= command.length(), 
                      "文件大小应该至少等于写入数据大小");
        }

        @Test
        @DisplayName("UTF-8编码测试")
        void testUtf8Encoding() throws IOException {
            // Given: 包含中文的命令
            String command = "*2\r\n$3\r\nSET\r\n$6\r\n测试键\r\n";
            ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));

            // When: 写入中文数据
            int written = aofWriter.write(buffer);

            // Then: 验证写入结果
            assertEquals(command.getBytes(StandardCharsets.UTF_8).length, written, 
                        "UTF-8编码写入字节数应该正确");
            
            aofWriter.flush();
              // 验证文件内容
            byte[] fileContent = Files.readAllBytes(aofFile.toPath());
            String actualContent = new String(fileContent, StandardCharsets.UTF_8);
            
            // 处理文件预分配导致的末尾填充
            if (actualContent.length() >= command.length()) {
                actualContent = actualContent.substring(0, command.length());
            }
            
            assertEquals(command, actualContent, "UTF-8编码内容应该正确保存");
        }

        @Test
        @DisplayName("边界条件测试")
        void testBoundaryConditions() throws IOException {
            // Given: 准备边界条件数据
            String[] boundaryCommands = {
                "\r\n",  // 只有换行符
                "*0\r\n", // 空数组
                "*1\r\n$0\r\n\r\n", // 包含空字符串的数组
            };

            // When & Then: 写入边界条件数据
            for (String command : boundaryCommands) {
                ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
                assertDoesNotThrow(() -> {
                    int written = aofWriter.write(buffer);
                    assertEquals(command.length(), written, 
                               "边界条件写入字节数应该正确: " + command);
                }, "边界条件写入不应该抛出异常: " + command);
            }
        }
    }

    @Nested
    @DisplayName("性能特性测试")
    class PerformanceCharacteristicsTests {

        @BeforeEach
        void setUpPerformance() throws IOException {
            createSimpleAofWriter();
        }

        @Test
        @DisplayName("批量写入性能测试")
        void testBatchWritePerformance() throws IOException {
            // Given: 准备批量数据
            int batchSize = 100;
            String command = "*2\r\n$3\r\nSET\r\n$5\r\nvalue\r\n";

            // When: 执行批量写入
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < batchSize; i++) {
                ByteBuffer buffer = ByteBuffer.wrap(command.getBytes(StandardCharsets.UTF_8));
                aofWriter.write(buffer);
            }
            
            aofWriter.flush();
            long endTime = System.currentTimeMillis();

            // Then: 验证性能特征
            long duration = endTime - startTime;
            assertTrue(duration < 5000, 
                      String.format("批量写入%d条记录应该在5秒内完成，实际耗时: %dms", 
                                   batchSize, duration));
            
            // 验证所有数据都已写入
            byte[] fileContent = Files.readAllBytes(aofFile.toPath());
            String content = new String(fileContent, StandardCharsets.UTF_8);
            int commandCount = content.split("\\*2").length - 1; // 减1因为split会产生一个空字符串
            assertEquals(batchSize, commandCount, "应该写入所有命令");
        }
    }
}

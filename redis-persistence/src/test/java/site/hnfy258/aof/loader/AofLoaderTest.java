package site.hnfy258.aof.loader;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import site.hnfy258.core.RedisCore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * AofLoader完整测试类
 * 
 * @author Google Java Style Guide
 */
@DisplayName("AOF加载器完整测试")
class AofLoaderTest {

    @TempDir
    private Path tempDir;
    
    private File aofFile;
    private RedisCore mockRedisCore;
    private AofLoader aofLoader;
    private static final int PRE_ALLOCATE_SIZE = 4 * 1024 * 1024; // 4MB 预分配空间

    @BeforeEach
    void setUp() {
        aofFile = tempDir.resolve("test.aof").toFile();
        mockRedisCore = mock(RedisCore.class);
        
        // 默认情况下，RedisCore 执行命令成功
        when(mockRedisCore.executeCommand(any(String.class), any(String[].class)))
            .thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        if (aofLoader != null) {
            aofLoader.close();
        }
    }

    /**
     * 写入AOF文件内容，并预分配空间
     */
    private void writeAofContent(String content) throws Exception {
        // 首先写入实际内容
        try (RandomAccessFile raf = new RandomAccessFile(aofFile, "rw")) {
            raf.write(content.getBytes(StandardCharsets.UTF_8));
            
            // 预分配额外空间
            raf.setLength(raf.length() + PRE_ALLOCATE_SIZE);
        }
    }

    @Nested
    @DisplayName("文件操作测试")
    class FileOperationTests {

        @Test
        @DisplayName("不存在的文件测试")
        void testNonExistentFile() throws Exception {
            // Given: 不存在的文件
            File nonExistentFile = tempDir.resolve("nonexistent.aof").toFile();

            // When: 创建AofLoader
            aofLoader = new AofLoader(nonExistentFile.getAbsolutePath(), mockRedisCore);

            // Then: 应该正常创建，不抛出异常
            assertNotNull(aofLoader, "AofLoader应该能处理不存在的文件");
            
            // When: 加载空文件
            assertDoesNotThrow(() -> aofLoader.load(), "加载不存在的文件不应该抛出异常");
            
            // Then: 不应该调用RedisCore的命令执行
            verify(mockRedisCore, never()).executeCommand(any(String.class), any(String[].class));
        }

        @Test
        @DisplayName("空文件测试")
        void testEmptyFile() throws Exception {
            // Given: 创建空文件并预分配空间
            writeAofContent("");

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            
            // Then: 应该正常处理
            assertDoesNotThrow(() -> aofLoader.load(), "加载空文件不应该抛出异常");
            
            // 不应该调用RedisCore的命令执行
            verify(mockRedisCore, never()).executeCommand(any(String.class), any(String[].class));
        }        @Test
        @DisplayName("无效文件路径测试")
        void testInvalidFilePath() throws IOException {
            // Given: 创建一个无法读取的文件（存在但没有读取权限）
            File restrictedFile = tempDir.resolve("restricted.aof").toFile();
            Files.write(restrictedFile.toPath(), "test data".getBytes());
            
            // 在Windows上设置文件为只写（去除读权限）
            boolean success = restrictedFile.setReadable(false);
            
            if (success) {
                // When & Then: 创建AofLoader应该抛出异常
                assertThrows(Exception.class, () -> {
                    new AofLoader(restrictedFile.getAbsolutePath(), mockRedisCore);
                }, "无读取权限的文件应该抛出异常");
                
                // 恢复权限以便清理
                restrictedFile.setReadable(true);            } else {
                // 如果无法设置权限（某些Windows版本限制），跳过此测试
                System.out.println("无法设置文件权限，跳过权限测试");
            }
        }
    }

    @Nested
    @DisplayName("命令解析测试")
    class CommandParsingTests {

        @Test
        @DisplayName("单个SET命令测试")
        void testSingleSetCommand() throws Exception {
            // Given: 准备正确的RESP格式AOF数据
            String aofContent = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
            writeAofContent(aofContent);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证命令执行
            verify(mockRedisCore, times(1)).executeCommand(
                eq("SET"), 
                eq(new String[]{"key1", "value1"})
            );
        }

        @Test
        @DisplayName("多个命令测试")
        void testMultipleCommands() throws Exception {
            // Given: 准备多个命令的AOF数据
            StringBuilder aofContent = new StringBuilder();
            aofContent.append("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n");  // SET key1 value1
            aofContent.append("*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n");  // SET key2 value2
            aofContent.append("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");                  // GET key1
            aofContent.append("*2\r\n$3\r\nDEL\r\n$4\r\nkey2\r\n");                  // DEL key2
            
            writeAofContent(aofContent.toString());

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证所有命令都被执行
            verify(mockRedisCore, times(1)).executeCommand("SET", new String[]{"key1", "value1"});
            verify(mockRedisCore, times(1)).executeCommand("SET", new String[]{"key2", "value2"});
            verify(mockRedisCore, times(1)).executeCommand("GET", new String[]{"key1"});
            verify(mockRedisCore, times(1)).executeCommand("DEL", new String[]{"key2"});
        }

        @Test
        @DisplayName("复杂命令测试")
        void testComplexCommands() throws Exception {
            // Given: 准备复杂命令的AOF数据
            StringBuilder aofContent = new StringBuilder();
            // HSET hash field1 value1 field2 value2
            aofContent.append("*6\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n");
            // LPUSH list item1 item2 item3
            aofContent.append("*5\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$5\r\nitem1\r\n$5\r\nitem2\r\n$5\r\nitem3\r\n");
            
            writeAofContent(aofContent.toString());

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证复杂命令执行
            verify(mockRedisCore, times(1)).executeCommand(
                "HSET", 
                new String[]{"hash", "field1", "value1", "field2", "value2"}
            );
            verify(mockRedisCore, times(1)).executeCommand(
                "LPUSH", 
                new String[]{"list", "item1", "item2", "item3"}
            );
        }        @Test
        @DisplayName("UTF-8编码测试")
        void testUtf8Encoding() throws Exception {
            // Given: 准备包含中文的AOF数据
            String key = "中文键";
            String value = "中文值";
            
            // 计算UTF-8字节长度
            int keyLength = key.getBytes(StandardCharsets.UTF_8).length;
            int valueLength = value.getBytes(StandardCharsets.UTF_8).length;
            
            String aofContent = String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
                keyLength, key, valueLength, value);
            writeAofContent(aofContent);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证UTF-8编码正确处理
            verify(mockRedisCore, times(1)).executeCommand(
                "SET", 
                new String[]{"中文键", "中文值"}
            );
        }
    }

    @Nested
    @DisplayName("错误处理测试")
    class ErrorHandlingTests {

        @Test
        @DisplayName("格式错误数据测试")
        void testMalformedData() throws Exception {
            // Given: 准备格式错误的数据
            String malformedContent = "这不是RESP格式的数据\r\n无效内容\r\n";
            writeAofContent(malformedContent);

            // When & Then: 加载应该处理错误但不抛出异常
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            assertDoesNotThrow(() -> aofLoader.load(), "格式错误的数据应该被优雅处理");

            // 不应该调用任何命令执行
            verify(mockRedisCore, never()).executeCommand(any(String.class), any(String[].class));
        }

        @Test
        @DisplayName("不完整命令测试")
        void testIncompleteCommand() throws Exception {
            // Given: 准备不完整的RESP数据
            String incompleteContent = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n"; // 缺少值部分
            writeAofContent(incompleteContent);

            // When & Then: 加载应该处理错误
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            assertDoesNotThrow(() -> aofLoader.load(), "不完整的命令应该被优雅处理");
        }

        @Test
        @DisplayName("命令执行失败测试")
        void testCommandExecutionFailure() throws Exception {
            // Given: 准备正确的AOF数据，但模拟RedisCore执行失败
            String aofContent = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
            writeAofContent(aofContent);
            
            // 模拟命令执行失败
            when(mockRedisCore.executeCommand("SET", new String[]{"key1", "value1"}))
                .thenReturn(false);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            
            // Then: 应该正常处理，不抛出异常
            assertDoesNotThrow(() -> aofLoader.load(), "命令执行失败应该被优雅处理");
            
            // 验证命令仍然被调用
            verify(mockRedisCore, times(1)).executeCommand("SET", new String[]{"key1", "value1"});
        }

        @Test
        @DisplayName("RedisCore异常测试")
        void testRedisCoreException() throws Exception {
            // Given: 准备正确的AOF数据，但模拟RedisCore抛出异常
            String aofContent = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
            writeAofContent(aofContent);
            
            // 模拟RedisCore抛出异常
            when(mockRedisCore.executeCommand("SET", new String[]{"key1", "value1"}))
                .thenThrow(new RuntimeException("Redis执行异常"));

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            
            // Then: 应该正常处理异常
            assertDoesNotThrow(() -> aofLoader.load(), "RedisCore异常应该被优雅处理");
        }
    }


    @Nested
    @DisplayName("资源管理测试")
    class ResourceManagementTests {

        @Test
        @DisplayName("资源正确释放测试")
        void testResourceCleanup() throws Exception {
            // Given: 准备AOF数据
            String aofContent = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
            writeAofContent(aofContent);

            // When: 创建AofLoader，加载并关闭
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();
            
            // Then: 多次关闭不应该抛出异常
            assertDoesNotThrow(() -> aofLoader.close(), "第一次关闭应该成功");
            assertDoesNotThrow(() -> aofLoader.close(), "重复关闭不应该抛出异常");
        }

        @Test
        @DisplayName("加载过程中的异常清理测试")
        void testExceptionCleanup() throws Exception {
            // Given: 准备可能导致异常的数据
            byte[] invalidData = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
            writeAofContent(new String(invalidData, StandardCharsets.UTF_8));

            // When: 创建AofLoader并尝试加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            
            // Then: 即使加载过程中有异常，资源也应该被正确清理
            assertDoesNotThrow(() -> aofLoader.load(), "异常情况下资源应该被正确清理");
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {

        @Test
        @DisplayName("空命令测试")
        void testEmptyCommand() throws Exception {
            // Given: 准备空数组命令
            String aofContent = "*0\r\n";
            writeAofContent(aofContent);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 空命令应该被忽略
            verify(mockRedisCore, never()).executeCommand(any(String.class), any(String[].class));
        }

        @Test
        @DisplayName("单字符命令测试")
        void testSingleCharacterCommand() throws Exception {
            // Given: 准备单字符命令
            String aofContent = "*1\r\n$1\r\nX\r\n";
            writeAofContent(aofContent);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证单字符命令执行
            verify(mockRedisCore, times(1)).executeCommand("X", new String[]{});
        }

        @Test
        @DisplayName("极长值测试")
        void testVeryLongValue() throws Exception {
            // Given: 准备包含极长值的命令
            StringBuilder longValue = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                longValue.append("very_long_value_");
            }
            
            String aofContent = String.format(
                "*3\r\n$3\r\nSET\r\n$8\r\nlong_key\r\n$%d\r\n%s\r\n",
                longValue.length(), longValue.toString()
            );
            writeAofContent(aofContent);

            // When: 创建AofLoader并加载
            aofLoader = new AofLoader(aofFile.getAbsolutePath(), mockRedisCore);
            aofLoader.load();

            // Then: 验证极长值正确处理
            verify(mockRedisCore, times(1)).executeCommand(
                "SET", 
                new String[]{"long_key", longValue.toString()}
            );
        }
    }
}

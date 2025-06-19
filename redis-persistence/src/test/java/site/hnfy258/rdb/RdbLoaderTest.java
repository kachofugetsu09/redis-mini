package site.hnfy258.rdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Dict;
import site.hnfy258.internal.Sds;
import site.hnfy258.rdb.crc.Crc64OutputStream;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;

/**
 * RdbLoader单元测试
 * 
 * <p>测试RDB加载器的功能，包括：</p>
 * <ul>
 *     <li>RDB文件加载</li>
 *     <li>CRC64校验</li>
 *     <li>错误处理</li>
 *     <li>数据恢复</li>
 * </ul>
 */
@DisplayName("RdbLoader测试")
class RdbLoaderTest {

    @Mock
    private RedisCore mockRedisCore;
    
    private RdbLoader rdbLoader;
    private AutoCloseable mockCloser;

    @BeforeEach
    void setUp() {
        mockCloser = MockitoAnnotations.openMocks(this);
        rdbLoader = new RdbLoader(mockRedisCore);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockCloser != null) {
            mockCloser.close();
        }
    }

    @Nested
    @DisplayName("基本加载功能测试")
    class BasicLoadTests {

        @Test
        @DisplayName("不存在文件加载测试")
        void testLoadNonExistentFile(@TempDir Path tempDir) {
            // 1. 准备不存在的文件
            File nonExistentFile = tempDir.resolve("nonexistent.rdb").toFile();
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(nonExistentFile);
            
            // 3. 验证结果（不存在的文件应该返回true，表示加载成功但无数据）
            assertTrue(result, "不存在的文件加载应该返回true");
            
            // 4. 验证没有调用RedisCore的方法
            verifyNoInteractions(mockRedisCore);
        }

        @Test
        @DisplayName("空RDB文件加载测试")
        void testLoadEmptyRdbFile(@TempDir Path tempDir) throws IOException {
            // 1. 创建空的有效RDB文件
            File rdbFile = createValidEmptyRdbFile(tempDir);
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 3. 验证结果
            assertTrue(result, "空RDB文件加载应该成功");
        }

        @Test
        @DisplayName("无效RDB头部测试")
        void testLoadInvalidHeader(@TempDir Path tempDir) throws IOException {
            // 1. 创建带有无效头部的文件
            File rdbFile = tempDir.resolve("invalid_header.rdb").toFile();
            try (FileOutputStream fos = new FileOutputStream(rdbFile)) {
                fos.write("INVALID12".getBytes()); // 错误的头部
            }
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 3. 验证结果
            assertFalse(result, "无效头部的RDB文件加载应该失败");
        }

        @Test
        @DisplayName("CRC64校验失败测试")
        void testLoadCorruptedFile(@TempDir Path tempDir) throws IOException {
            // 1. 创建损坏的RDB文件（CRC64不匹配）
            File rdbFile = createCorruptedRdbFile(tempDir);
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 3. 验证结果
            assertFalse(result, "CRC64校验失败的文件加载应该失败");
        }

        @Test
        @DisplayName("含数据RDB文件加载测试")
        void testLoadRdbWithData(@TempDir Path tempDir) throws IOException {
            // 1. 创建含有数据的RDB文件
            File rdbFile = createRdbFileWithData(tempDir);
            
            // 2. 设置Mock期望
            setupMockExpectations();
            
            // 3. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 4. 验证结果
            assertTrue(result, "含数据的RDB文件加载应该成功");
            
            // 5. 验证Mock调用
            verify(mockRedisCore, atLeastOnce()).selectDB(anyInt());
            verify(mockRedisCore, atLeastOnce()).put(any(RedisBytes.class), any(RedisData.class));
        }

        private File createValidEmptyRdbFile(Path tempDir) throws IOException {
            File rdbFile = tempDir.resolve("empty.rdb").toFile();
            
            try (Crc64OutputStream crc64Stream = new Crc64OutputStream(
                    new BufferedOutputStream(new FileOutputStream(rdbFile)))) {
                
                DataOutputStream dos = crc64Stream.getDataOutputStream();
                
                // 写入RDB头部
                dos.writeBytes("REDIS0009");
                
                // 写入EOF
                dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
                
                // 写入CRC64校验和
                crc64Stream.writeCrc64Checksum();
            }
            
            return rdbFile;
        }

        private File createCorruptedRdbFile(Path tempDir) throws IOException {
            File rdbFile = tempDir.resolve("corrupted.rdb").toFile();
            
            try (FileOutputStream fos = new FileOutputStream(rdbFile)) {
                // 写入RDB头部
                fos.write("REDIS0009".getBytes());
                
                // 写入一些数据
                fos.write("some data".getBytes());
                
                // 写入EOF
                fos.write(RdbConstants.RDB_OPCODE_EOF);
                
                // 写入错误的CRC64校验和
                for (int i = 0; i < 8; i++) {
                    fos.write(0xFF);
                }
            }
            
            return rdbFile;
        }

        private File createRdbFileWithData(Path tempDir) throws IOException {
            File rdbFile = tempDir.resolve("with_data.rdb").toFile();
            
            try (Crc64OutputStream crc64Stream = new Crc64OutputStream(
                    new BufferedOutputStream(new FileOutputStream(rdbFile)))) {
                
                DataOutputStream dos = crc64Stream.getDataOutputStream();
                
                // 写入RDB头部
                dos.writeBytes("REDIS0009");
                
                // 写入数据库选择
                dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);
                dos.writeByte(0); // 数据库0
                
                // 写入一个字符串键值对
                dos.writeByte(RdbConstants.STRING_TYPE);
                
                // 写入键
                String key = "test-key";
                dos.writeByte(key.length()); // 长度
                dos.writeBytes(key); // 键内容
                
                // 写入值
                String value = "test-value";
                dos.writeByte(value.length()); // 长度
                dos.writeBytes(value); // 值内容
                
                // 写入EOF
                dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
                
                // 写入CRC64校验和
                crc64Stream.writeCrc64Checksum();
            }
            
            return rdbFile;
        }

        private void setupMockExpectations() {
            // 模拟selectDB调用
            doNothing().when(mockRedisCore).selectDB(anyInt());
            
            // 模拟put调用
            doNothing().when(mockRedisCore).put(any(RedisBytes.class), any(RedisData.class));
        }
    }

    @Nested
    @DisplayName("数据类型加载测试")
    class DataTypeLoadTests {

        @Test
        @DisplayName("字符串类型加载测试")
        void testLoadStringType(@TempDir Path tempDir) throws IOException {
            // 1. 创建包含字符串的RDB文件
            File rdbFile = createRdbFileWithString(tempDir, "string-key", "string-value");
            
            // 2. 设置Mock期望
            setupMockExpectations();
            
            // 3. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 4. 验证结果
            assertTrue(result, "字符串类型加载应该成功");
            
            // 5. 验证字符串被正确加载
            verify(mockRedisCore).put(any(RedisBytes.class), any(RedisString.class));
        }

        @Test
        @DisplayName("列表类型加载测试")
        void testLoadListType(@TempDir Path tempDir) throws IOException {
            // 1. 创建包含列表的RDB文件
            File rdbFile = createRdbFileWithList(tempDir);
            
            // 2. 设置Mock期望
            setupMockExpectations();
            
            // 3. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 4. 验证结果
            assertTrue(result, "列表类型加载应该成功");
            
            // 5. 验证列表被正确加载
            verify(mockRedisCore).put(any(RedisBytes.class), any(RedisList.class));
        }

        private File createRdbFileWithString(Path tempDir, String key, String value) throws IOException {
            File rdbFile = tempDir.resolve("string.rdb").toFile();
            
            try (Crc64OutputStream crc64Stream = new Crc64OutputStream(
                    new BufferedOutputStream(new FileOutputStream(rdbFile)))) {
                
                DataOutputStream dos = crc64Stream.getDataOutputStream();
                
                // 写入RDB头部
                dos.writeBytes("REDIS0009");
                
                // 写入数据库选择
                dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);
                dos.writeByte(0);
                
                // 写入字符串类型
                dos.writeByte(RdbConstants.STRING_TYPE);
                dos.writeByte(key.length());
                dos.writeBytes(key);
                dos.writeByte(value.length());
                dos.writeBytes(value);
                
                // 写入EOF和校验和
                dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
                crc64Stream.writeCrc64Checksum();
            }
            
            return rdbFile;
        }

        private File createRdbFileWithList(Path tempDir) throws IOException {
            File rdbFile = tempDir.resolve("list.rdb").toFile();
            
            try (Crc64OutputStream crc64Stream = new Crc64OutputStream(
                    new BufferedOutputStream(new FileOutputStream(rdbFile)))) {
                
                DataOutputStream dos = crc64Stream.getDataOutputStream();
                
                // 写入RDB头部
                dos.writeBytes("REDIS0009");
                
                // 写入数据库选择
                dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);
                dos.writeByte(0);
                
                // 写入列表类型
                dos.writeByte(RdbConstants.LIST_TYPE);
                
                // 写入键
                String key = "list-key";
                dos.writeByte(key.length());
                dos.writeBytes(key);
                
                // 写入列表大小
                dos.writeByte(2); // 2个元素
                
                // 写入列表元素
                String item1 = "item1";
                dos.writeByte(item1.length());
                dos.writeBytes(item1);
                
                String item2 = "item2";
                dos.writeByte(item2.length());
                dos.writeBytes(item2);
                
                // 写入EOF和校验和
                dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
                crc64Stream.writeCrc64Checksum();
            }
            
            return rdbFile;
        }

        private void setupMockExpectations() {
            doNothing().when(mockRedisCore).selectDB(anyInt());
            doNothing().when(mockRedisCore).put(any(RedisBytes.class), any(RedisData.class));
        }
    }

    @Nested
    @DisplayName("异常处理测试")
    class ExceptionHandlingTests {

        @Test
        @DisplayName("IO异常处理测试")
        void testIOExceptionHandling(@TempDir Path tempDir) throws IOException {
            // 1. 创建一个文件并立即删除（模拟访问时文件不存在）
            File rdbFile = tempDir.resolve("temp.rdb").toFile();
            rdbFile.createNewFile();
            
            // 2. 创建一个输入流并立即关闭它
            FileInputStream fis = new FileInputStream(rdbFile);
            fis.close();
            
            // 3. 删除文件
            rdbFile.delete();
            
            // 4. 执行加载（应该处理IO异常）
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 5. 验证结果
            assertTrue(result, "不存在的文件应该返回true"); // 因为不存在文件被当作空数据库处理
        }

        @Test
        @DisplayName("数据损坏异常处理测试")
        void testDataCorruptionHandling(@TempDir Path tempDir) throws IOException {
            // 1. 创建部分损坏的RDB文件
            File rdbFile = tempDir.resolve("partial.rdb").toFile();
            try (FileOutputStream fos = new FileOutputStream(rdbFile)) {
                fos.write("REDIS0009".getBytes()); // 只写头部，没有EOF和CRC64
            }
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(rdbFile);
            
            // 3. 验证结果
            assertFalse(result, "损坏的文件加载应该失败");
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {

        @Test
        @DisplayName("空文件测试")
        void testEmptyFile(@TempDir Path tempDir) throws IOException {
            // 1. 创建空文件
            File emptyFile = tempDir.resolve("empty.rdb").toFile();
            emptyFile.createNewFile();
            
            // 2. 执行加载
            boolean result = rdbLoader.loadRdb(emptyFile);
            
            // 3. 验证结果
            assertFalse(result, "空文件加载应该失败");
        }

        @Test
        @DisplayName("超大文件处理测试")
        void testLargeFileHandling(@TempDir Path tempDir) throws IOException {
            // 1. 创建一个相对较大的有效RDB文件
            File largeFile = createLargeValidRdbFile(tempDir);
            
            // 2. 设置Mock期望
            setupMockExpectations();
            
            // 3. 执行加载
            boolean result = rdbLoader.loadRdb(largeFile);
            
            // 4. 验证结果
            assertTrue(result, "大文件加载应该成功");
        }

        private File createLargeValidRdbFile(Path tempDir) throws IOException {
            File rdbFile = tempDir.resolve("large.rdb").toFile();
            
            try (Crc64OutputStream crc64Stream = new Crc64OutputStream(
                    new BufferedOutputStream(new FileOutputStream(rdbFile)))) {
                
                DataOutputStream dos = crc64Stream.getDataOutputStream();
                
                // 写入RDB头部
                dos.writeBytes("REDIS0009");
                
                // 写入数据库选择
                dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);
                dos.writeByte(0);
                
                // 写入多个字符串键值对
                for (int i = 0; i < 100; i++) {
                    dos.writeByte(RdbConstants.STRING_TYPE);
                    
                    String key = "key" + i;
                    dos.writeByte(key.length());
                    dos.writeBytes(key);
                    
                    String value = "value" + i + " with some additional data to make it larger";
                    dos.writeByte(value.length());
                    dos.writeBytes(value);
                }
                
                // 写入EOF和校验和
                dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
                crc64Stream.writeCrc64Checksum();
            }
            
            return rdbFile;
        }

        private void setupMockExpectations() {
            doNothing().when(mockRedisCore).selectDB(anyInt());
            doNothing().when(mockRedisCore).put(any(RedisBytes.class), any(RedisData.class));
        }
    }
}

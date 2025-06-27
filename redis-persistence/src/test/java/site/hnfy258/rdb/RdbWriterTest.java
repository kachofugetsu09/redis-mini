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

import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

/**
 * RdbWriter单元测试
 *
 * <p>测试RDB写入器的功能，包括：</p>
 * <ul>
 *     <li>同步写入功能</li>
 *     <li>异步写入功能</li>
 *     <li>错误处理</li>
 *     <li>资源管理</li>
 * </ul>
 */
@DisplayName("RdbWriter测试")
class RdbWriterTest {

    @Mock
    private RedisCore mockRedisCore;

    @Mock
    private RedisDB mockRedisDB;

    private RdbWriter rdbWriter;
    private AutoCloseable mockCloser;

    @BeforeEach
    void setUp() {
        mockCloser = MockitoAnnotations.openMocks(this);
        rdbWriter = new RdbWriter(mockRedisCore);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (rdbWriter != null) {
            rdbWriter.close();
        }
        if (mockCloser != null) {
            mockCloser.close();
        }
    }

    @Nested
    @DisplayName("同步写入测试")
    class SynchronousWriteTests {

        @Test
        @DisplayName("空数据库同步写入测试")
        void testWriteRdbWithEmptyDatabases(@TempDir Path tempDir) throws IOException {
            // 1. 准备空数据库
            RedisDB[] emptyDatabases = new RedisDB[0];
            when(mockRedisCore.getDataBases()).thenReturn(emptyDatabases);

            // 2. 执行写入
            Path testFile = tempDir.resolve("empty.rdb");
            boolean result = rdbWriter.writeRdb(testFile.toString());

            // 3. 验证结果
            assertTrue(result, "空数据库写入应该成功");
            assertTrue(testFile.toFile().exists(), "RDB文件应该被创建");

            // 4. 验证文件内容（至少包含头部、EOF和CRC64）
            long fileSize = testFile.toFile().length();
            assertTrue(fileSize >= 18, "文件大小至少18字节（头部9 + EOF1 + CRC64 8）");
        }

        @Test
        @DisplayName("单个数据库同步写入测试")
        void testWriteRdbWithSingleDatabase(@TempDir Path tempDir) throws IOException {
            // 1. 准备测试数据
            setupSingleDatabaseMock();

            // 2. 执行写入
            Path testFile = tempDir.resolve("single.rdb");
            boolean result = rdbWriter.writeRdb(testFile.toString());

            // 3. 验证结果
            assertTrue(result, "单数据库写入应该成功");
            assertTrue(testFile.toFile().exists(), "RDB文件应该被创建");

            // 4. 验证Mock调用
            verify(mockRedisCore, times(1)).getDataBases();
        }

        @Test
        @DisplayName("写入异常处理测试")
        void testWriteException() {
            // 1. 使用无效路径触发IOException
            String invalidPath = "/invalid/path/that/does/not/exist/test.rdb";

            // 2. 执行写入
            boolean result = rdbWriter.writeRdb(invalidPath);

            // 3. 验证结果
            assertFalse(result, "无效路径写入应该失败");
        }

        @SuppressWarnings("unchecked")
        private void setupSingleDatabaseMock() {
            // 创建测试数据
            Map<RedisBytes, RedisData> testData = new HashMap<>();
            RedisBytes testKey = new RedisBytes("test-key".getBytes());
            RedisString testValue = new RedisString(Sds.create("test-value".getBytes()));
            testData.put(testKey, testValue);

            // 创建Mock字典
            Dict<RedisBytes, RedisData> mockDict = mock(Dict.class);
            when(mockDict.createSnapshot().toMap()).thenReturn(testData);

            // 设置Mock数据库
            when(mockRedisDB.getId()).thenReturn(0);
            when(mockRedisDB.size()).thenReturn(1L);
            when(mockRedisDB.getData()).thenReturn(mockDict);

            // 设置RedisCore返回数据库数组
            when(mockRedisCore.getDataBases()).thenReturn(new RedisDB[]{mockRedisDB});
        }
    }

    @Nested
    @DisplayName("异步写入测试")
    class AsynchronousWriteTests {

        @Test
        @DisplayName("基本异步写入测试")
        void testBgSaveRdb(@TempDir Path tempDir) throws Exception {
            // 1. 准备数据
            setupAsyncDatabaseMock();

            // 2. 执行异步写入
            Path testFile = tempDir.resolve("async.rdb");
            CompletableFuture<Boolean> future = rdbWriter.bgSaveRdb(testFile.toString());

            // 3. 等待完成
            Boolean result = future.get(5, TimeUnit.SECONDS);

            // 4. 验证结果
            assertTrue(result, "异步写入应该成功");
            assertTrue(testFile.toFile().exists(), "RDB文件应该被创建");
        }

        @Test
        @DisplayName("异步写入并发保护测试")
        void testBgSaveConcurrentProtection(@TempDir Path tempDir) throws Exception {
            // 1. 准备数据
            setupAsyncDatabaseMock();

            // 2. 启动第一个异步写入
            Path testFile1 = tempDir.resolve("async1.rdb");
            CompletableFuture<Boolean> future1 = rdbWriter.bgSaveRdb(testFile1.toString());

            // 3. 立即尝试第二个异步写入
            Path testFile2 = tempDir.resolve("async2.rdb");
            CompletableFuture<Boolean> future2 = rdbWriter.bgSaveRdb(testFile2.toString());

            // 4. 等待两个任务完成
            Boolean result1 = future1.get(5, TimeUnit.SECONDS);
            Boolean result2 = future2.get(5, TimeUnit.SECONDS);

            // 5. 验证结果（第二个应该被拒绝）
            assertTrue(result1, "第一个异步写入应该成功");
            assertFalse(result2, "第二个异步写入应该被拒绝");
        }

        @Test
        @DisplayName("异步写入异常处理测试")
        void testBgSaveException() throws Exception {
            // 1. 使用无效路径
            String invalidPath = "/invalid/path/async.rdb";

            // 2. 执行异步写入
            CompletableFuture<Boolean> future = rdbWriter.bgSaveRdb(invalidPath);

            // 3. 等待完成
            Boolean result = future.get(5, TimeUnit.SECONDS);

            // 4. 验证结果
            assertFalse(result, "异步写入异常应该返回false");
        }

        @SuppressWarnings("unchecked")
        private void setupAsyncDatabaseMock() {
            // 与同步测试中相同的设置
            Map<RedisBytes, RedisData> testData = new HashMap<>();
            RedisBytes testKey = new RedisBytes("test-key".getBytes());
            RedisString testValue = new RedisString(Sds.create("test-value".getBytes()));
            testData.put(testKey, testValue);

            Dict<RedisBytes, RedisData> mockDict = mock(Dict.class);
            when(mockDict.createSnapshot().toMap()).thenReturn(testData);

            when(mockRedisDB.getId()).thenReturn(0);
            when(mockRedisDB.size()).thenReturn(1L);
            when(mockRedisDB.getData()).thenReturn(mockDict);

            when(mockRedisCore.getDataBases()).thenReturn(new RedisDB[]{mockRedisDB});
        }
    }

    @Nested
    @DisplayName("数据类型写入测试")
    class DataTypeWriteTests {

        @Test
        @DisplayName("字符串类型写入测试")
        void testWriteStringType(@TempDir Path tempDir) throws IOException {
            // 1. 准备字符串数据
            Map<RedisBytes, RedisData> testData = new HashMap<>();
            RedisBytes key = new RedisBytes("string-key".getBytes());
            RedisString value = new RedisString(Sds.create("string-value".getBytes()));
            testData.put(key, value);

            setupDatabaseWithData(testData);

            // 2. 执行写入
            Path testFile = tempDir.resolve("string.rdb");
            boolean result = rdbWriter.writeRdb(testFile.toString());

            // 3. 验证结果
            assertTrue(result, "字符串类型写入应该成功");
            assertTrue(testFile.toFile().length() > 20, "文件应该包含字符串数据");
        }

        @Test
        @DisplayName("列表类型写入测试")
        void testWriteListType(@TempDir Path tempDir) throws IOException {
            // 1. 准备列表数据
            Map<RedisBytes, RedisData> testData = new HashMap<>();
            RedisBytes key = new RedisBytes("list-key".getBytes());
            RedisList value = new RedisList();
            value.lpush(new RedisBytes("item1".getBytes()));
            value.lpush(new RedisBytes("item2".getBytes()));
            testData.put(key, value);

            setupDatabaseWithData(testData);

            // 2. 执行写入
            Path testFile = tempDir.resolve("list.rdb");
            boolean result = rdbWriter.writeRdb(testFile.toString());

            // 3. 验证结果
            assertTrue(result, "列表类型写入应该成功");
        }

        @Test
        @DisplayName("集合类型写入测试")
        void testWriteSetType(@TempDir Path tempDir) throws IOException {
            // 1. 准备集合数据
            Map<RedisBytes, RedisData> testData = new HashMap<>();
            RedisBytes key = new RedisBytes("set-key".getBytes());
            RedisSet value = new RedisSet();

            // 使用正确的add方法
            List<RedisBytes> members = Arrays.asList(
                    new RedisBytes("member1".getBytes()),
                    new RedisBytes("member2".getBytes())
            );
            value.add(members);
            testData.put(key, value);

            setupDatabaseWithData(testData);

            // 2. 执行写入
            Path testFile = tempDir.resolve("set.rdb");
            boolean result = rdbWriter.writeRdb(testFile.toString());

            // 3. 验证结果
            assertTrue(result, "集合类型写入应该成功");
        }

        @SuppressWarnings("unchecked")
        private void setupDatabaseWithData(Map<RedisBytes, RedisData> testData) {
            Dict<RedisBytes, RedisData> mockDict = mock(Dict.class);
            when(mockDict.createSnapshot().toMap()).thenReturn(testData);

            when(mockRedisDB.getId()).thenReturn(0);
            when(mockRedisDB.size()).thenReturn((long) testData.size());
            when(mockRedisDB.getData()).thenReturn(mockDict);

            when(mockRedisCore.getDataBases()).thenReturn(new RedisDB[]{mockRedisDB});
        }
    }

    @Nested
    @DisplayName("资源管理测试")
    class ResourceManagementTests {

        @Test
        @DisplayName("正常关闭测试")
        void testNormalClose() throws Exception {
            // 1. 创建writer
            RdbWriter writer = new RdbWriter(mockRedisCore);

            // 2. 正常关闭
            assertDoesNotThrow(() -> writer.close(), "正常关闭不应该抛出异常");

            // 3. 二次关闭也应该安全
            assertDoesNotThrow(() -> writer.close(), "二次关闭应该安全");
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {        @Test
        @DisplayName("空文件名测试")
        void testEmptyFileName() {
            assertThrows(IllegalArgumentException.class, () ->
                    rdbWriter.writeRdb(""), "空文件名应该抛出IllegalArgumentException");
        }@Test
        @DisplayName("null文件名测试")
        void testNullFileName() {
            assertThrows(IllegalArgumentException.class, () ->
                    rdbWriter.writeRdb(null), "null文件名应该抛出IllegalArgumentException");
        }

        @Test
        @DisplayName("bgSaveRdb null文件名测试")
        void testBgSaveRdbNullFileName() {
            assertThrows(IllegalArgumentException.class, () ->
                    rdbWriter.bgSaveRdb(null), "bgSaveRdb null文件名应该抛出IllegalArgumentException");
        }

        @Test
        @DisplayName("bgSaveRdb 空文件名测试")
        void testBgSaveRdbEmptyFileName() {
            assertThrows(IllegalArgumentException.class, () ->
                    rdbWriter.bgSaveRdb(""), "bgSaveRdb 空文件名应该抛出IllegalArgumentException");
        }

        @Test
        @DisplayName("长文件名测试")
        void testLongFileName() {
            // 使用字符串拼接创建长文件名
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("a");
            }
            String longFileName = sb.toString() + ".rdb";

            boolean result = rdbWriter.writeRdb(longFileName);
            assertFalse(result, "超长文件名应该写入失败");
        }
    }
}

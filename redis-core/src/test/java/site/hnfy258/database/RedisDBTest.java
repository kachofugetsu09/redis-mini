package site.hnfy258.database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisDB类的单元测试
 * 
 * <p>全面测试Redis数据库的功能，包括：
 * <ul>
 *     <li>基本的键值对操作（存储、获取、删除）</li>
 *     <li>键的存在性检查和枚举</li>
 *     <li>数据库大小统计</li>
 *     <li>数据库清空操作</li>
 *     <li>并发安全性测试</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@DisplayName("RedisDB单元测试")
class RedisDBTest {

    private static final int TEST_DB_ID = 0;

    private RedisDB redisDB;
    private RedisBytes testKey;
    private RedisString testValue;

    @BeforeEach
    void setUp() {
        redisDB = new RedisDB(TEST_DB_ID);
        testKey = RedisBytes.fromString("test_key");
        testValue = new RedisString(Sds.create("test_value".getBytes()));
    }

    @Test
    @DisplayName("测试基本构造和初始状态")
    void testBasicConstructionAndInitialState() {
        assertNotNull(redisDB.getData());
        assertEquals(TEST_DB_ID, redisDB.getId());
        assertEquals(0, redisDB.size());
    }

    @Test
    @DisplayName("测试键值对操作")
    void testKeyValueOperations() {
        // 1. 测试存储和获取
        redisDB.put(testKey, testValue);
        assertTrue(redisDB.exist(testKey));
        assertEquals(testValue, redisDB.get(testKey));
        assertEquals(1, redisDB.size());

        // 2. 测试删除
        RedisString deletedValue = (RedisString) redisDB.delete(testKey);
        assertEquals(testValue, deletedValue);
        assertFalse(redisDB.exist(testKey));
        assertEquals(0, redisDB.size());

        // 3. 测试获取不存在的键
        assertNull(redisDB.get(RedisBytes.fromString("nonexistent")));
    }

    @Test
    @DisplayName("测试键集合操作")
    void testKeysOperation() {
        // 1. 测试空数据库
        Set<RedisBytes> keys = redisDB.keys();
        assertTrue(keys.isEmpty());

        // 2. 添加多个键值对
        RedisBytes key1 = RedisBytes.fromString("key1");
        RedisBytes key2 = RedisBytes.fromString("key2");
        RedisString value1 = new RedisString(Sds.create("value1".getBytes()));
        RedisString value2 = new RedisString(Sds.create("value2".getBytes()));

        redisDB.put(key1, value1);
        redisDB.put(key2, value2);

        // 3. 验证键集合
        keys = redisDB.keys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));
    }

    @Test
    @DisplayName("测试数据库清空操作")
    void testClearOperation() {
        // 1. 准备测试数据
        redisDB.put(testKey, testValue);
        RedisBytes key2 = RedisBytes.fromString("key2");
        RedisString value2 = new RedisString(Sds.create("value2".getBytes()));
        redisDB.put(key2, value2);

        assertEquals(2, redisDB.size());

        // 2. 测试清空操作
        redisDB.clear();
        assertEquals(0, redisDB.size());
        assertTrue(redisDB.keys().isEmpty());
        assertFalse(redisDB.exist(testKey));
        assertFalse(redisDB.exist(key2));
    }    @Test
    @DisplayName("测试读写分离并发安全性")
    void testConcurrentReadWhileWrite() throws InterruptedException {
        final int INITIAL_DATA_SIZE = 100;
        final int WRITE_OPERATIONS = 50;
        final int READ_OPERATIONS = 500;
        
        // 1. 预填充一些初始数据
        for (int i = 0; i < INITIAL_DATA_SIZE; i++) {
            RedisBytes key = RedisBytes.fromString("initial_key_" + i);
            RedisString value = new RedisString(Sds.create(("initial_value_" + i).getBytes()));
            redisDB.put(key, value);
        }
        
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch writerLatch = new CountDownLatch(1);
        final CountDownLatch readerLatch = new CountDownLatch(1);
        final AtomicInteger readSuccessCount = new AtomicInteger(0);
        final AtomicInteger readFailureCount = new AtomicInteger(0);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        
        // 2. 启动写线程（模拟命令处理线程）
        executor.submit(() -> {
            try {
                startLatch.await();
                
                // 模拟连续的写操作
                for (int i = 0; i < WRITE_OPERATIONS; i++) {
                    RedisBytes key = RedisBytes.fromString("write_key_" + i);
                    RedisString value = new RedisString(Sds.create(("write_value_" + i).getBytes()));
                    redisDB.put(key, value);
                    
                    // 模拟一些处理时间
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writerLatch.countDown();
            }
        });
        
        // 3. 启动读线程（模拟后台持久化线程）
        executor.submit(() -> {
            try {
                startLatch.await();
                
                // 连续读取操作
                for (int i = 0; i < READ_OPERATIONS; i++) {
                    try {
                        // 随机读取一些键
                        String keyStr = "initial_key_" + (i % INITIAL_DATA_SIZE);
                        RedisBytes key = RedisBytes.fromString(keyStr);
                        RedisData data = redisDB.get(key);
                        
                        if (data != null) {
                            readSuccessCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        readFailureCount.incrementAndGet();
                        System.err.println("读取操作失败: " + e.getMessage());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                readerLatch.countDown();
            }
        });
        
        // 4. 开始测试
        startLatch.countDown();
        
        // 5. 等待完成
        assertTrue(writerLatch.await(10, TimeUnit.SECONDS), "写操作应该在10秒内完成");
        assertTrue(readerLatch.await(10, TimeUnit.SECONDS), "读操作应该在10秒内完成");
        
        // 6. 验证结果
        final int expectedFinalSize = INITIAL_DATA_SIZE + WRITE_OPERATIONS;
        final long actualSize = redisDB.size();
        
        System.out.println("=== 读写分离测试结果 ===");
        System.out.println("初始数据大小: " + INITIAL_DATA_SIZE);
        System.out.println("写入操作数: " + WRITE_OPERATIONS);
        System.out.println("读取操作数: " + READ_OPERATIONS);
        System.out.println("读取成功次数: " + readSuccessCount.get());
        System.out.println("读取失败次数: " + readFailureCount.get());
        System.out.println("预期最终大小: " + expectedFinalSize);
        System.out.println("实际最终大小: " + actualSize);
        
        // 验证写操作的完整性
        assertEquals(expectedFinalSize, actualSize, "最终数据库大小应该正确");
        
        // 验证读操作的稳定性
        assertEquals(0, readFailureCount.get(), "读操作不应该失败");
        assertTrue(readSuccessCount.get() > 0, "应该有成功的读操作");
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "线程池应该在5秒内关闭");
    }@Test
    @DisplayName("测试边界条件")
    void testBoundaryConditions() {
        // 1. 测试null键 - Dict实现抛出IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> redisDB.put(null, testValue));
        
        // get、exist、delete方法允许null，有相应的处理逻辑
        assertNull(redisDB.get(null), "get(null)应该返回null");
        assertFalse(redisDB.exist(null), "exist(null)应该返回false");
        assertNull(redisDB.delete(null), "delete(null)应该返回null");

        // 2. 测试null值 - Dict只检查key，不检查value
        assertDoesNotThrow(() -> redisDB.put(testKey, null));
        assertNull(redisDB.get(testKey), "存储null值后应该能正确获取null");

        // 3. 测试空字符串键
        RedisBytes emptyKey = RedisBytes.fromString("");
        redisDB.put(emptyKey, testValue);
        assertTrue(redisDB.exist(emptyKey));
        assertEquals(testValue, redisDB.get(emptyKey));

        // 4. 测试特殊字符键
        String specialChars = "!@#$%^&*()中文\n\t\r";
        RedisBytes specialKey = RedisBytes.fromString(specialChars);
        redisDB.put(specialKey, testValue);
        assertTrue(redisDB.exist(specialKey));
        assertEquals(testValue, redisDB.get(specialKey));
    }

    @Test
    @DisplayName("测试大量数据操作")
    void testLargeDataOperations() {
        final int LARGE_DATA_SIZE = 10000;

        // 1. 批量插入
        for (int i = 0; i < LARGE_DATA_SIZE; i++) {
            RedisBytes key = RedisBytes.fromString("key_" + i);
            RedisString value = new RedisString(Sds.create(("value_" + i).getBytes()));
            redisDB.put(key, value);
        }

        assertEquals(LARGE_DATA_SIZE, redisDB.size());

        // 2. 随机访问
        RedisBytes randomKey = RedisBytes.fromString("key_5000");
        assertTrue(redisDB.exist(randomKey));
        RedisString value = (RedisString) redisDB.get(randomKey);
        assertEquals("value_5000", value.getSds().toString());

        // 3. 批量删除
        for (int i = 0; i < LARGE_DATA_SIZE; i += 2) {
            RedisBytes key = RedisBytes.fromString("key_" + i);
            redisDB.delete(key);
        }

        assertEquals(LARGE_DATA_SIZE / 2, redisDB.size());
    }
    

}
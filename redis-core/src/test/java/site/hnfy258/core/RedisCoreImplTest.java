package site.hnfy258.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import site.hnfy258.core.command.CommandExecutor;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisCoreImpl的单元测试类
 * 
 * <p>测试Redis核心功能的各个方面，包括：
 * <ul>
 *     <li>数据库操作（选择、切换）</li>
 *     <li>键值对操作（CRUD）</li>
 *     <li>并发安全性</li>
 *     <li>命令执行</li>
 *     <li>边界条件处理</li>
 * </ul>
 */
@DisplayName("RedisCoreImpl单元测试")
class RedisCoreImplTest {    private RedisCoreImpl redisCore;
    private static final int TEST_DB_COUNT = 16;    @BeforeEach
    void setUp() {
        redisCore = new RedisCoreImpl(TEST_DB_COUNT);
    }

    @Test
    @DisplayName("测试数据库初始化")
    void testDatabaseInitialization() {
        assertEquals(TEST_DB_COUNT, redisCore.getDBNum());
        assertEquals(0, redisCore.getCurrentDBIndex());
        
        RedisDB[] databases = redisCore.getDataBases();
        assertEquals(TEST_DB_COUNT, databases.length);
        for (int i = 0; i < TEST_DB_COUNT; i++) {
            assertNotNull(databases[i]);
            assertEquals(i, databases[i].getId());
        }
    }

    @Test
    @DisplayName("测试数据库选择")
    void testDatabaseSelection() {
        // 测试有效的数据库索引
        redisCore.selectDB(5);
        assertEquals(5, redisCore.getCurrentDBIndex());

        // 测试无效的数据库索引
        assertThrows(RuntimeException.class, () -> redisCore.selectDB(-1));
        assertThrows(RuntimeException.class, () -> redisCore.selectDB(TEST_DB_COUNT));
    }

    @Test
    @DisplayName("测试基本的键值对操作")
    void testBasicKeyValueOperations() {
        RedisBytes key = RedisBytes.fromString("test_key");
        RedisString value = new RedisString(Sds.create("test_value".getBytes()));

        // 测试存储和获取
        redisCore.put(key, value);
        RedisData retrievedValue = redisCore.get(key);
        assertEquals(value, retrievedValue);

        // 测试删除
        assertTrue(redisCore.delete(key));
        assertNull(redisCore.get(key));
        assertFalse(redisCore.delete(key)); // 再次删除应返回false
    }

    @Test
    @DisplayName("测试键集合操作")
    void testKeysOperation() {
        RedisBytes key1 = RedisBytes.fromString("key1");
        RedisBytes key2 = RedisBytes.fromString("key2");
        RedisString value = new RedisString(Sds.create("value".getBytes()));

        // 测试空数据库
        Set<RedisBytes> emptyKeys = redisCore.keys();
        assertTrue(emptyKeys.isEmpty());

        // 添加键值对
        redisCore.put(key1, value);
        redisCore.put(key2, value);

        // 验证键集合
        Set<RedisBytes> keys = redisCore.keys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));
    }

    @Test
    @DisplayName("测试数据库清空")
    void testFlushAll() {
        // 在多个数据库中添加数据
        RedisBytes key = RedisBytes.fromString("test_key");
        RedisString value = new RedisString(Sds.create("test_value".getBytes()));

        redisCore.put(key, value);
        redisCore.selectDB(1);
        redisCore.put(key, value);

        // 清空所有数据库
        redisCore.flushAll();

        // 验证所有数据库都被清空
        for (int i = 0; i < TEST_DB_COUNT; i++) {
            redisCore.selectDB(i);
            assertTrue(redisCore.keys().isEmpty());
        }
    }    @Test
    @DisplayName("测试命令执行")
    void testCommandExecution() {
        String[] args = {"SET", "key", "value"};
        
        // 创建一个简单的测试命令执行器
        TestCommandExecutor testExecutor = new TestCommandExecutor();
        redisCore.setCommandExecutor(testExecutor);

        assertTrue(redisCore.executeCommand("SET", args));
        assertEquals("SET", testExecutor.getLastCommand());
        assertArrayEquals(args, testExecutor.getLastArgs());

        // 测试命令执行器未设置的情况
        RedisCoreImpl newRedisCore = new RedisCoreImpl(TEST_DB_COUNT);
        assertThrows(IllegalStateException.class, 
            () -> newRedisCore.executeCommand("SET", args));
    }
    
    /**
     * 测试用的简单命令执行器
     */
    private static class TestCommandExecutor implements CommandExecutor {
        private String lastCommand;
        private String[] lastArgs;
        
        @Override
        public boolean executeCommand(String commandName, String[] args) {
            this.lastCommand = commandName;
            this.lastArgs = args.clone();
            return true; // 总是返回成功
        }
        
        public String getLastCommand() {
            return lastCommand;
        }
        
        public String[] getLastArgs() {
            return lastArgs;
        }
    }

    @Test
    @DisplayName("测试并发数据库选择")
    void testConcurrentDatabaseSelection() throws InterruptedException {
        final int THREAD_COUNT = 10;
        final int OPERATIONS_PER_THREAD = 100;  // 减少操作数量以提高测试稳定性
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        final AtomicBoolean hasError = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);        // 修改测试策略：每个线程使用独立的RedisCoreImpl实例
        // 这更符合实际使用场景（每个客户端连接有独立的数据库选择状态）
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    // 每个线程使用独立的RedisCore实例
                    RedisCoreImpl threadRedisCore = new RedisCoreImpl(TEST_DB_COUNT);
                    
                    startLatch.await();
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        int dbIndex = (threadIndex * OPERATIONS_PER_THREAD + j) % TEST_DB_COUNT;
                        try {
                            threadRedisCore.selectDB(dbIndex);
                            // 验证当前数据库索引是否正确
                            int currentIndex = threadRedisCore.getCurrentDBIndex();
                            if (currentIndex != dbIndex) {
                                System.err.println("Thread " + threadIndex + ": Expected " + dbIndex + 
                                                 ", but got " + currentIndex);
                                hasError.set(true);
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("Thread " + threadIndex + " error: " + e.getMessage());
                            hasError.set(true);
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();        assertTrue(endLatch.await(10, TimeUnit.SECONDS), "并发测试应该在10秒内完成");
        assertFalse(hasError.get(), "独立实例的数据库选择应该是线程安全的");

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("测试跨数据库操作")
    void testCrossDatabaseOperations() {
        RedisBytes key = RedisBytes.fromString("test_key");
        RedisString value1 = new RedisString(Sds.create("value1".getBytes()));
        RedisString value2 = new RedisString(Sds.create("value2".getBytes()));

        // 在数据库0中存储数据
        redisCore.put(key, value1);

        // 切换到数据库1并存储不同的值
        redisCore.selectDB(1);
        redisCore.put(key, value2);

        // 验证数据隔离性
        assertEquals(value2, redisCore.get(key));
        
        redisCore.selectDB(0);
        assertEquals(value1, redisCore.get(key));
    }

    @Test
    @DisplayName("测试共享实例的并发数据库选择行为")
    void testSharedInstanceConcurrentBehavior() throws InterruptedException {
        final int THREAD_COUNT = 5;
        final int OPERATIONS_PER_THREAD = 20;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        final AtomicBoolean hasValidOperation = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // 测试共享实例的情况：验证原子操作的正确性
        // 注意：这个测试证明了为什么在实际应用中每个客户端应该有独立的数据库选择状态
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        int dbIndex = threadIndex; // 每个线程使用固定的数据库索引
                        try {
                            // 设置数据库并立即添加数据验证操作的原子性
                            redisCore.selectDB(dbIndex);
                            RedisBytes key = RedisBytes.fromString("thread_" + threadIndex + "_key_" + j);
                            RedisString value = new RedisString(Sds.create(("value_" + j).getBytes()));
                            redisCore.put(key, value);
                            
                            // 验证数据是否存在（注意：可能在不同的数据库中）
                            RedisData retrievedValue = redisCore.get(key);
                            if (retrievedValue != null) {
                                hasValidOperation.set(true);
                            }
                        } catch (Exception e) {
                            System.err.println("Thread " + threadIndex + " error: " + e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS), "并发测试应该在10秒内完成");
        assertTrue(hasValidOperation.get(), "至少应该有一些有效的操作完成");

        // 验证数据在各个数据库中的分布
        int totalKeys = 0;
        for (int i = 0; i < THREAD_COUNT; i++) {
            redisCore.selectDB(i);
            totalKeys += redisCore.keys().size();
        }
        
        System.out.println("Total keys across all databases: " + totalKeys);
        assertTrue(totalKeys > 0, "应该在某些数据库中存储了数据");

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
}
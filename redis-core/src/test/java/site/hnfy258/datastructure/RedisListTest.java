package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisList类的单元测试
 * 
 * <p>全面测试Redis列表数据结构的功能，包括：
 * <ul>
 *     <li>基本的列表操作（左右推入和弹出）</li>
 *     <li>范围查询操作</li>
 *     <li>过期时间管理</li>
 *     <li>Redis协议转换</li>
 *     <li>边界条件和异常情况</li>
 *     <li>并发安全性测试</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@DisplayName("RedisList单元测试")
class RedisListTest {

    private static final String TEST_KEY = "test_list";
    private static final int THREAD_COUNT = 10;
    private static final int OPERATIONS_PER_THREAD = 100;

    private RedisList redisList;
    private RedisBytes testKey;

    @BeforeEach
    void setUp() {
        redisList = new RedisList();
        testKey = RedisBytes.fromString(TEST_KEY);
        redisList.setKey(testKey);
    }

    @Test
    @DisplayName("测试基本构造和初始状态")
    void testBasicConstructionAndInitialState() {
        assertNotNull(redisList.getList());
        assertEquals(0, redisList.size());
        assertEquals(-1, redisList.timeout());
        assertEquals(TEST_KEY, redisList.getKey().getString());
    }

    @Test
    @DisplayName("测试左端推入和弹出操作")
    void testLeftPushAndPop() {
        // 1. 测试左端推入
        RedisBytes value1 = RedisBytes.fromString("value1");
        RedisBytes value2 = RedisBytes.fromString("value2");
        
        redisList.lpush(value1, value2);
        assertEquals(2, redisList.size());
        
        // 2. 测试左端弹出
        RedisBytes popped = redisList.lpop();
        assertNotNull(popped);
        assertEquals("value2", popped.getString());
        assertEquals(1, redisList.size());
        
        popped = redisList.lpop();
        assertNotNull(popped);
        assertEquals("value1", popped.getString());
        assertEquals(0, redisList.size());
        
        // 3. 测试空列表弹出
        assertNull(redisList.lpop());
    }

    @Test
    @DisplayName("测试右端推入和弹出操作")
    void testRightPushAndPop() {
        // 1. 测试右端推入
        RedisBytes value1 = RedisBytes.fromString("value1");
        RedisBytes value2 = RedisBytes.fromString("value2");
        
        redisList.rpush(value1, value2);
        assertEquals(2, redisList.size());
        
        // 2. 测试右端弹出
        RedisBytes popped = redisList.rpop();
        assertNotNull(popped);
        assertEquals("value2", popped.getString());
        assertEquals(1, redisList.size());
        
        popped = redisList.rpop();
        assertNotNull(popped);
        assertEquals("value1", popped.getString());
        assertEquals(0, redisList.size());
        
        // 3. 测试空列表弹出
        assertNull(redisList.rpop());
    }

    @Test
    @DisplayName("测试范围查询操作")
    void testRangeOperations() {
        // 1. 准备测试数据
        for (int i = 0; i < 5; i++) {
            redisList.rpush(RedisBytes.fromString("value" + i));
        }
        
        // 2. 测试正常范围
        List<RedisBytes> range = redisList.lrange(1, 3);
        assertEquals(3, range.size());
        assertEquals("value1", range.get(0).getString());
        assertEquals("value3", range.get(2).getString());
        
        // 3. 测试负数索引
        range = redisList.lrange(-3, -1);
        assertEquals(3, range.size());
        assertEquals("value2", range.get(0).getString());
        assertEquals("value4", range.get(2).getString());
        
        // 4. 测试越界范围
        range = redisList.lrange(-10, 10);
        assertEquals(5, range.size());
        
        // 5. 测试空范围
        range = redisList.lrange(3, 1);
        assertTrue(range.isEmpty());
    }

    @Test
    @DisplayName("测试过期时间管理")
    void testTimeoutManagement() {
        // 1. 测试默认过期时间
        assertEquals(-1, redisList.timeout());
        
        // 2. 测试设置过期时间
        long expireTime = System.currentTimeMillis() + 10000;
        redisList.setTimeout(expireTime);
        assertEquals(expireTime, redisList.timeout());
        
        // 3. 测试设置永不过期
        redisList.setTimeout(-1);
        assertEquals(-1, redisList.timeout());
    }

    @Test
    @DisplayName("测试Redis协议转换")
    void testConvertToResp() {
        // 1. 测试空列表转换
        List<Resp> respList = redisList.convertToResp();
        assertTrue(respList.isEmpty());
        
        // 2. 测试有数据的列表转换
        redisList.lpush(
            RedisBytes.fromString("value1"),
            RedisBytes.fromString("value2")
        );
        
        respList = redisList.convertToResp();
        assertEquals(1, respList.size());
        
        RespArray respArray = (RespArray) respList.get(0);
        Resp[] commands = respArray.getContent();
        
        assertEquals("LPUSH", ((BulkString) commands[0]).getContent().getString());
        assertEquals(TEST_KEY, ((BulkString) commands[1]).getContent().getString());
        assertEquals("value2", ((BulkString) commands[2]).getContent().getString());
        assertEquals("value1", ((BulkString) commands[3]).getContent().getString());
    }

    @Test
    @DisplayName("测试元素移除操作")
    void testRemoveElements() {
        // 1. 准备测试数据
        RedisBytes target = RedisBytes.fromString("target");
        redisList.lpush(
            RedisBytes.fromString("value1"),
            target,
            RedisBytes.fromString("value2"),
            target
        );
        
        // 2. 测试移除元素
        int removed = redisList.remove(target);
        assertEquals(2, removed);
        assertEquals(2, redisList.size());
        
        // 3. 测试移除不存在的元素
        removed = redisList.remove(RedisBytes.fromString("nonexistent"));
        assertEquals(0, removed);
    }

    @Test
    @DisplayName("测试并发安全性")
    void testConcurrentOperations() throws InterruptedException {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        final AtomicInteger totalOperations = new AtomicInteger(0);

        // 1. 并发推入测试
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        if (j % 2 == 0) {
                            redisList.lpush(RedisBytes.fromString("t" + threadId + "v" + j));
                        } else {
                            redisList.rpush(RedisBytes.fromString("t" + threadId + "v" + j));
                        }
                        totalOperations.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));
        
        assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD, totalOperations.get());
        assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD, redisList.size());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("测试边界条件")
    void testBoundaryConditions() {
        // 1. 测试空字符串
        RedisBytes emptyValue = RedisBytes.fromString("");
        redisList.lpush(emptyValue);
        assertEquals(1, redisList.size());
        assertEquals("", redisList.lpop().getString());
        
        // 2. 测试特殊字符
        String specialChars = "!@#$%^&*()中文\n\t\r";
        RedisBytes specialValue = RedisBytes.fromString(specialChars);
        redisList.lpush(specialValue);
        assertEquals(specialChars, redisList.lpop().getString());
        
        // 3. 测试大量数据
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("a");
        }
        RedisBytes longValue = RedisBytes.fromString(longString.toString());
        redisList.lpush(longValue);
        assertEquals(longString.toString(), redisList.lpop().getString());
    }

    @Test
    @DisplayName("测试获取所有元素")
    void testGetAll() {
        // 1. 测试空列表
        RedisBytes[] allElements = redisList.getAll();
        assertEquals(0, allElements.length);
        
        // 2. 测试有数据的列表
        RedisBytes value1 = RedisBytes.fromString("value1");
        RedisBytes value2 = RedisBytes.fromString("value2");
        redisList.lpush(value1, value2);
        
        allElements = redisList.getAll();
        assertEquals(2, allElements.length);
        assertEquals("value2", allElements[0].getString());
        assertEquals("value1", allElements[1].getString());
    }
} 
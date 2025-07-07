package site.hnfy258.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * RedisBatchOptimizer的单元测试类
 * 
 * <p>测试批量操作的各种场景，包括：
 * <ul>
 *     <li>小批量数据处理</li>
 *     <li>大批量数据处理</li>
 *     <li>边界条件处理</li>
 *     <li>并发安全性</li>
 *     <li>数值操作的正确性</li>
 * </ul>
 */
@DisplayName("RedisBatchOptimizer单元测试")
class RedisBatchOptimizerTest {

    private RedisCore redisCore;
    private static final int SMALL_BATCH_SIZE = 5;
    private static final int LARGE_BATCH_SIZE = 20;

    @BeforeEach
    void setUp() {
        redisCore = mock(RedisCore.class);
    }

    @Test
    @DisplayName("测试小批量字符串设置")
    void testSmallBatchSetStrings() {
        // 准备测试数据
        Map<RedisBytes, RedisBytes> keyValuePairs = new HashMap<>();
        for (int i = 0; i < SMALL_BATCH_SIZE; i++) {
            keyValuePairs.put(
                RedisBytes.fromString("key" + i),
                RedisBytes.fromString("value" + i)
            );
        }

        // 执行批量设置
        RedisBatchOptimizer.batchSetStrings(redisCore, keyValuePairs);

        // 验证每个键值对都被正确设置
        verify(redisCore, times(SMALL_BATCH_SIZE)).put(any(RedisBytes.class), any(RedisString.class));
    }

    @Test
    @DisplayName("测试大批量字符串设置")
    void testLargeBatchSetStrings() {
        // 准备测试数据
        Map<RedisBytes, RedisBytes> keyValuePairs = new HashMap<>();
        for (int i = 0; i < LARGE_BATCH_SIZE; i++) {
            keyValuePairs.put(
                RedisBytes.fromString("key" + i),
                RedisBytes.fromString("value" + i)
            );
        }

        // 执行批量设置
        RedisBatchOptimizer.batchSetStrings(redisCore, keyValuePairs);

        // 验证所有键值对都被正确设置
        verify(redisCore, times(LARGE_BATCH_SIZE)).put(any(RedisBytes.class), any(RedisString.class));
    }

    @Test
    @DisplayName("测试空Map处理")
    void testEmptyBatchSetStrings() {
        Map<RedisBytes, RedisBytes> emptyMap = new HashMap<>();
        RedisBatchOptimizer.batchSetStrings(redisCore, emptyMap);
        
        // 验证没有调用put方法
        verify(redisCore, never()).put(any(RedisBytes.class), any(RedisString.class));
    }

    @Test
    @DisplayName("测试批量递增 - 正常数值")
    void testBatchIncrement() {
        // 准备测试数据
        RedisBytes[] keys = new RedisBytes[]{
            RedisBytes.fromString("counter1"),
            RedisBytes.fromString("counter2")
        };

        // 模拟当前值
        when(redisCore.get(keys[0])).thenReturn(new RedisString(Sds.create("5".getBytes())));
        when(redisCore.get(keys[1])).thenReturn(new RedisString(Sds.create("10".getBytes())));

        // 执行批量递增
        RedisBatchOptimizer.batchIncrement(redisCore, keys, 2);

        // 验证结果
        verify(redisCore).put(eq(keys[0]), argThat(value -> 
            ((RedisString)value).getSds().toString().equals("7")));
        verify(redisCore).put(eq(keys[1]), argThat(value -> 
            ((RedisString)value).getSds().toString().equals("12")));
    }

    @Test
    @DisplayName("测试批量递增 - 空值处理")
    void testBatchIncrementWithNullValues() {
        RedisBytes[] keys = new RedisBytes[]{
            RedisBytes.fromString("newCounter1"),
            RedisBytes.fromString("newCounter2")
        };

        // 模拟键不存在的情况
        when(redisCore.get(any(RedisBytes.class))).thenReturn(null);

        // 执行批量递增
        RedisBatchOptimizer.batchIncrement(redisCore, keys, 1);

        // 验证从0开始计数
        verify(redisCore, times(2)).put(any(RedisBytes.class), argThat(value -> 
            ((RedisString)value).getSds().toString().equals("1")));
    }

    @Test
    @DisplayName("测试批量递增 - 无效数值处理")
    void testBatchIncrementWithInvalidValues() {
        RedisBytes key = RedisBytes.fromString("invalidCounter");
        RedisBytes[] keys = new RedisBytes[]{key};

        // 模拟无效数值
        when(redisCore.get(key)).thenReturn(new RedisString(Sds.create("invalid".getBytes())));

        // 验证抛出异常
        assertThrows(NumberFormatException.class, () -> 
            RedisBatchOptimizer.batchIncrement(redisCore, keys, 1));
    }

    @Test
    @DisplayName("测试并发批量设置")
    void testConcurrentBatchSetStrings() throws InterruptedException {
        final int THREAD_COUNT = 5;
        final int BATCH_SIZE = 100;
        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Map<RedisBytes, RedisBytes> keyValuePairs = new HashMap<>();
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        String key = "thread" + threadId + "_key" + i;
                        String value = "thread" + threadId + "_value" + i;
                        keyValuePairs.put(
                            RedisBytes.fromString(key),
                            RedisBytes.fromString(value)
                        );
                    }
                    RedisBatchOptimizer.batchSetStrings(redisCore, keyValuePairs);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "并发测试应该在10秒内完成");
        verify(redisCore, times(THREAD_COUNT * BATCH_SIZE))
            .put(any(RedisBytes.class), any(RedisString.class));

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
} 
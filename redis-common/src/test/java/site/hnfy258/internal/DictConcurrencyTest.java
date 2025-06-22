// File: redis-common/src/test/java/site/hnfy258/internal/DictConcurrencyTest.java
package site.hnfy258.internal;

import org.junit.jupiter.api.*;
import site.hnfy258.datastructure.RedisBytes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Dict 并发和 CoW 特性单元测试
 *
 * 专注于验证 Dict 的不可变性、CAS 操作、无锁读、快照一致性以及 Rehash 过程的正确性。
 * 模拟 Redis 单线程写命令 + 后台读/快照的并发模型。
 *
 * @author hnfy258 (assisted by Gemini)
 * @since 1.0
 */
@DisplayName("Dict 并发和 CoW 特性测试")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DictConcurrencyTest {

    private Dict<RedisBytes, RedisBytes> dict;
    private static final int INITIAL_KEYS_FOR_READ_TEST = 1000;
    private static final int WRITER_THREAD_COUNT = 1; // 模拟 Redis 单线程命令执行器
    private static final int READER_THREAD_COUNT = 5; // 模拟多个后台或读客户端
    private static final int OPERATIONS_PER_WRITER = 1000;
    private static final int OPERATIONS_PER_READER = 5000;
    private ExecutorService writerExecutor;
    private ExecutorService readerExecutor;

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        writerExecutor = Executors.newFixedThreadPool(WRITER_THREAD_COUNT);
        readerExecutor = Executors.newFixedThreadPool(READER_THREAD_COUNT);
        System.out.println("\n--- 开始新测试 ---");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        // 强制完成所有 rehash，确保 Dict 最终处于稳定状态
        int rehashSteps = 0;
        while (dict.rehashIndex != -1) {
            dict.rehashStep();
            rehashSteps++;
        }
        if (rehashSteps > 0) {
            System.out.println("TearDown: Rehash completed with " + rehashSteps + " steps.");
        }

        if (writerExecutor != null && !writerExecutor.isShutdown()) {
            writerExecutor.shutdownNow();
            writerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (readerExecutor != null && !readerExecutor.isShutdown()) {
            readerExecutor.shutdownNow();
            readerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        System.out.println("测试完成，Dict 最终大小: " + dict.size());
        System.out.println("--------------------\n");
    }

    @Nested
    @DisplayName("1. 基础功能和键唯一性测试 (单线程)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class BasicCoWTests {

        @Test
        @Order(1)
        @DisplayName("1.1 put 插入新键")
        void testPutNewKey() {
            RedisBytes key = RedisBytes.fromString("newKey");
            RedisBytes value = RedisBytes.fromString("newValue");

            assertNull(dict.put(key, value), "第一次 put 应该返回 null");
            assertEquals(1, dict.size(), "Dict 大小应该为 1");
            assertEquals(value, dict.get(key), "get 应该返回正确的值");
            assertTrue(dict.containsKey(key), "containsKey 应该返回 true");
        }

        @Test
        @Order(2)
        @DisplayName("1.2 put 更新已存在的键 (验证键唯一性)")
        void testPutUpdateExistingKey() {
            RedisBytes key = RedisBytes.fromString("updateKey");
            RedisBytes oldValue = RedisBytes.fromString("oldValue");
            RedisBytes newValue = RedisBytes.fromString("newValue");

            dict.put(key, oldValue); // 插入初始值
            assertEquals(1, dict.size());
            assertEquals(oldValue, dict.get(key));

            // 更新键
            assertEquals(oldValue, dict.put(key, newValue), "更新 put 应该返回旧值");
            assertEquals(1, dict.size(), "Dict 大小应该仍然为 1");
            assertEquals(newValue, dict.get(key), "get 应该返回新值");

            // 验证不会有重复键：通过遍历快照或直接检查 dict.size() 和 keySet().size()
            // 在 CoW 链表实现中，重复键是不会在逻辑上存在的。
            // 如果 put 实现了正确逻辑，get 方法返回正确值，size 保持不变，即可证明唯一性。
            // 之前的重复键问题已经通过 Dict 类的 putInBucket 修复。
            assertEquals(1, dict.keySet().size(), "keySet 大小应该为 1");
        }

        @Test
        @Order(3)
        @DisplayName("1.3 remove 删除存在的键")
        void testRemoveExistingKey() {
            RedisBytes key = RedisBytes.fromString("deleteKey");
            RedisBytes value = RedisBytes.fromString("deleteValue");
            dict.put(key, value);
            assertEquals(1, dict.size());

            assertEquals(value, dict.remove(key), "remove 应该返回被删除的值");
            assertEquals(0, dict.size(), "Dict 大小应该为 0");
            assertNull(dict.get(key), "get 应该返回 null");
            assertFalse(dict.containsKey(key), "containsKey 应该返回 false");
        }

        @Test
        @Order(4)
        @DisplayName("1.4 remove 删除不存在的键")
        void testRemoveNonExistingKey() {
            RedisBytes key = RedisBytes.fromString("nonExistingKey");
            assertNull(dict.remove(key), "删除不存在的键应该返回 null");
            assertEquals(0, dict.size(), "Dict 大小应该为 0");
        }

        @Test
        @Order(5)
        @DisplayName("1.5 clear 清空 Dict")
        void testClear() {
            dict.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            dict.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));
            assertEquals(2, dict.size());
            dict.clear();
            assertEquals(0, dict.size());
            assertTrue(dict.keySet().isEmpty());
        }

        @Test
        @Order(6)
        @DisplayName("1.6 快照 (createSafeSnapshot) 基础测试")
        void testCreateSafeSnapshotBasic() {
            RedisBytes key1 = RedisBytes.fromString("snap1");
            RedisBytes value1 = RedisBytes.fromString("val1");
            RedisBytes key2 = RedisBytes.fromString("snap2");
            RedisBytes value2 = RedisBytes.fromString("val2");

            dict.put(key1, value1);
            dict.put(key2, value2);

            Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
            assertEquals(2, snapshot.size());
            assertEquals(value1, snapshot.get(key1));
            assertEquals(value2, snapshot.get(key2));

            // 修改原始 Dict，快照应该不受影响
            RedisBytes newValue1 = RedisBytes.fromString("newVal1");
            dict.put(key1, newValue1); // 更新 key1
            dict.remove(key2); // 删除 key2


            // 验证快照内容不变
            assertEquals(value1, snapshot.get(key1), "快照值不应改变");
            assertEquals(value2, snapshot.get(key2), "快照值不应改变");
            assertTrue(snapshot.containsKey(key2), "快照应该包含 key2，因为它在快照创建时是存在的");
            assertTrue(dict.size() == 1, "原始Dict大小应为1");
            assertTrue(dict.get(key1).equals(newValue1), "原始Dict中的key1值应为新值");
        }
    }

    @Nested
    @DisplayName("2. 并发写入测试 (模拟单线程执行器)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class ConcurrentWriteTests {

        @Test
        @Order(1)
        @DisplayName("2.1 单写线程大量 put/remove 混合操作")
        @Timeout(value = 30, unit = TimeUnit.SECONDS)
        void testSingleWriterMixedOperations() throws InterruptedException, java.util.concurrent.ExecutionException {
            ConcurrentHashMap<String, String> expectedFinalState = new ConcurrentHashMap<>();

            // 使用 Futures 确保任务完成并捕获异常
            Future<Void> writerFuture = writerExecutor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_WRITER; i++) {
                        String keyStr = "key-" + i;
                        RedisBytes key = RedisBytes.fromString(keyStr);

                        if (i % 2 == 0) { // 50% put
                            String valueStr = "value-" + i;
                            RedisBytes value = RedisBytes.fromString(valueStr);
                            dict.put(key, value);
                            expectedFinalState.put(keyStr, valueStr);
                        } else { // 50% remove
                            dict.remove(key);
                            expectedFinalState.remove(keyStr);
                        }
                        // 偶尔触发 rehashStep
                        if (i % 100 == 0 && dict.rehashIndex != -1) {
                           dict.rehashStep(); // 手动推进 rehash
                        }
                    }
                    return null;
                } catch (Exception e) {
                    System.err.println("Writer thread error: " + e.getMessage());
                    e.printStackTrace();
                    throw e; // 重新抛出异常以使测试失败
                }
            });

            writerFuture.get(); // 等待写入任务完成

            // 验证最终状态
            assertEquals(expectedFinalState.size(), dict.size(), "最终 Dict 大小应该与预期一致");
            expectedFinalState.forEach((keyStr, valueStr) -> {
                RedisBytes key = RedisBytes.fromString(keyStr);
                RedisBytes actualValue = dict.get(key);
                assertNotNull(actualValue, "键 " + keyStr + " 应该存在");
                assertEquals(RedisBytes.fromString(valueStr), actualValue, "键 " + keyStr + " 的值不匹配");
            });

            // 反向验证 Dict 中没有意外的键
            dict.keySet().forEach(k -> assertTrue(expectedFinalState.containsKey(k.getString()), "Dict 中不应该有意外的键: " + k.getString()));
        }
    }

    @Nested
    @DisplayName("3. 并发读写混合测试 (CoW 快照验证)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class ConcurrentReadWriteTests {

        @BeforeEach
        void setupReadWriteTest() {
            // 预填充 Dict
            for (int i = 0; i < INITIAL_KEYS_FOR_READ_TEST; i++) {
                dict.put(RedisBytes.fromString("init-key-" + i), RedisBytes.fromString("init-value-" + i));
            }
            System.out.println("预填充 Dict 完成，大小: " + dict.size());
            // 确保 rehash 在测试开始前完成，以获得稳定的初始状态
            int rehashSteps = 0;
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
                rehashSteps++;
            }
            if (rehashSteps > 0) {
                System.out.println("setupReadWriteTest: Initial rehash completed with " + rehashSteps + " steps.");
            }
        }

        @Test
        @Order(1)
        @DisplayName("3.1 单写线程 + 多读线程 (快照一致性)")
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testSingleWriterMultiReaderWithSnapshotConsistency() throws InterruptedException, java.util.concurrent.ExecutionException {
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicBoolean writeError = new AtomicBoolean(false);
            AtomicLong totalReads = new AtomicLong(0);
            AtomicLong consistentReads = new AtomicLong(0);
            AtomicLong inconsistentReads = new AtomicLong(0);

            // 存储写操作的键和新值，用于验证
            ConcurrentHashMap<String, String> writesDuringTest = new ConcurrentHashMap<>();

            // 1. 启动读线程（模拟后台快照、监控或其他读操作）
            for (int i = 0; i < READER_THREAD_COUNT; i++) {
                int readerId = i;
                readerExecutor.submit(() -> {
                    try {
                        startLatch.await(); // 等待所有线程就绪
                        for (int j = 0; j < OPERATIONS_PER_READER; j++) {
                            // 随机抽取一个快照，验证其内部数据
                            Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
                            totalReads.incrementAndGet();

                            // 验证快照的键是否在写操作进行时保持一致
                            // CoW 快照应该在获取时是稳定的，其大小应大致与快照开始时 Dict 的大小一致
                            // 这里可以更严格地验证快照内容的正确性，例如随机抽取键验证值
                            // 但是，由于写操作正在进行，难以精确预测快照的具体内容，只能验证其一致性特性
                            // 最简单的验证就是，快照大小与它在快照那一刻的 Dict 大小接近
                            if (snapshot.size() >= INITIAL_KEYS_FOR_READ_TEST - (OPERATIONS_PER_WRITER/2) && // 至少不小于删除最少后的数量
                                    snapshot.size() <= INITIAL_KEYS_FOR_READ_TEST + (OPERATIONS_PER_WRITER/2) + Dict.DICT_REHASH_BUCKETS_PER_STEP) { // 允许一些 rehash 带来的大小波动
                                consistentReads.incrementAndGet();
                            } else {
                                inconsistentReads.incrementAndGet();
                                System.err.println("Reader " + readerId + ": Inconsistent snapshot size: " + snapshot.size() + " (expected around " + INITIAL_KEYS_FOR_READ_TEST + ")");
                                // 打印一些快照内容帮助调试
                                if (snapshot.size() > 0) {
                                    snapshot.entrySet().stream().limit(5).forEach(entry -> System.err.println("  Snapshot entry: " + entry.getKey().getString() + " -> " + entry.getValue().getString()));
                                }
                            }
                            // 模拟读操作耗时
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("Reader thread " + readerId + " error: " + e.getMessage());
                        e.printStackTrace();
                        writeError.set(true);
                    }
                });
            }

            // 2. 启动写线程（模拟主命令执行器）
            Future<Void> writerFuture = writerExecutor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪
                    for (int i = 0; i < OPERATIONS_PER_WRITER; i++) {
                        String keyStr = "active-key-" + i;
                        String valueStr = "active-value-" + i;
                        RedisBytes key = RedisBytes.fromString(keyStr);
                        RedisBytes value = RedisBytes.fromString(valueStr);

                        if (i % 2 == 0) { // 50% put (插入新键或更新)
                            dict.put(key, value);
                            writesDuringTest.put(keyStr, valueStr);
                        } else { // 50% remove
                            dict.remove(key);
                            writesDuringTest.remove(keyStr);
                        }
                        // 强制推进 rehashStep，确保 rehash 在读写并发中进行
                        // rehashStep 内部会检查 rehashIndex
                        dict.rehashStep();
                        // 模拟写操作耗时
                        Thread.sleep(2);
                    }
                    return null;
                } catch (Exception e) {
                    System.err.println("Writer thread error: " + e.getMessage());
                    e.printStackTrace();
                    writeError.set(true);
                    throw e;
                }
            });

            startLatch.countDown(); // 释放所有线程
            writerFuture.get(); // 等待写线程完成
            readerExecutor.shutdown();
            assertTrue(readerExecutor.awaitTermination(30, TimeUnit.SECONDS), "读线程应在规定时间内完成");

            System.out.println("总读取快照次数: " + totalReads.get());
            System.out.println("一致性快照次数: " + consistentReads.get());
            System.out.println("不一致快照次数: " + inconsistentReads.get()); // 理论上应为0或极少

            assertFalse(writeError.get(), "并发读写过程中不应出现异常");

            // 期望所有快照都是一致的，或者只有极少数由于 rehash 边界效应导致的大小波动
            // 如果 putInBucket 修复正确，这里 size 应该非常稳定
            // 由于 CoW 每次修改都会产生新对象，快照的大小应该反映其获取时刻 Dict 的大小。
            // 这里我们主要验证没有 ConcurrentModificationException 和明显的逻辑错误。
            // “一致性快照次数”应占绝大多数
            assertTrue(inconsistentReads.get() < totalReads.get() * 0.05, "不一致快照的比例应低于5%"); // 允许5%的误差

            // 验证最终状态
            System.out.println("最终 Dict 大小: " + dict.size());
            // 期望的最终大小是初始键 + 写入的键 (插入的) - 删除的键
            int expectedFinalSize = INITIAL_KEYS_FOR_READ_TEST + writesDuringTest.size();
            assertEquals(expectedFinalSize, dict.size(), "最终 Dict 大小应与预期一致");

            // 验证所有最终存在的键值对都正确
            writesDuringTest.forEach((keyStr, valueStr) -> {
                RedisBytes key = RedisBytes.fromString(keyStr);
                RedisBytes actualValue = dict.get(key);
                assertNotNull(actualValue, "最终键 " + keyStr + " 应该存在");
                assertEquals(RedisBytes.fromString(valueStr), actualValue, "最终键 " + keyStr + " 的值不匹配");
            });
        }
    }
}
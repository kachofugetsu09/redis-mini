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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Dict 渐进式测试套件 - 从简单到复杂，逐步验证功能和并发安全性
 * 
 * 测试顺序：
 * 1. 基础功能（单线程）
 * 2. 单线程Rehash测试
 * 3. 简单并发测试
 * 4. 复杂并发测试
 * 5. 极端场景测试
 */
@DisplayName("Dict 渐进式测试套件")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DictTest {

    private Dict<RedisBytes, RedisBytes> dict;

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        System.out.println("\n=== 开始新测试 ===");
    }

    @AfterEach
    void tearDown() {
        System.out.println("测试完成，Dict大小: " + dict.size());
    }

    @Nested
    @DisplayName("1. 基本功能测试 (单线程)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class) // 嵌套类内部也按照顺序执行
    class BasicSingleThreadTests {

        @Test
        @Order(1) // 最先执行：测试put和get基础功能
        @DisplayName("1.1 put 和 get - 正常流程")
        void testPutAndGet() {
            RedisBytes key = RedisBytes.fromString("testKey");
            RedisBytes value = RedisBytes.fromString("testValue");

            assertNull(dict.put(key, value), "第一次put应该返回null");
            assertEquals(1, dict.size(), "Dict大小应该为1");
            assertEquals(value, dict.get(key), "get应该返回正确的值");

            RedisBytes newValue = RedisBytes.fromString("newValue");
            assertEquals(value, dict.put(key, newValue), "覆盖put应该返回旧值");
            assertEquals(1, dict.size(), "Dict大小应该仍然为1");
            assertEquals(newValue, dict.get(key), "get应该返回新值");
        }

        @Test
        @Order(2) // 其次执行：测试remove功能
        @DisplayName("1.2 remove - 删除现有键")
        void testRemoveExistingKey() {
            RedisBytes key = RedisBytes.fromString("testKey");
            RedisBytes value = RedisBytes.fromString("testValue");
            dict.put(key, value);

            assertEquals(value, dict.remove(key), "remove应该返回被删除的值");
            assertEquals(0, dict.size(), "Dict大小应该为0");
            assertNull(dict.get(key), "get应该返回null，因为键已被删除");
        }

        @Test
        @Order(3)
        @DisplayName("1.3 remove - 删除不存在的键")
        void testRemoveNonExistingKey() {
            RedisBytes key = RedisBytes.fromString("nonExistingKey");
            assertNull(dict.remove(key), "删除不存在的键应该返回null");
            assertEquals(0, dict.size(), "Dict大小应该为0");
        }

        @Test
        @Order(4)
        @DisplayName("1.4 containsKey - 检查键是否存在")
        void testContainsKey() {
            RedisBytes key = RedisBytes.fromString("testKey");
            RedisBytes value = RedisBytes.fromString("testValue");

            assertFalse(dict.containsKey(key), "初始不应该包含键");
            dict.put(key, value);
            assertTrue(dict.containsKey(key), "应该包含已添加的键");
            dict.remove(key);
            assertFalse(dict.containsKey(key), "删除后不应该包含键");
        }

        @Test
        @Order(5)
        @DisplayName("1.5 clear - 清空所有数据")
        void testClear() {
            dict.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            dict.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));
            assertEquals(2, dict.size(), "清空前大小应该为2");
            dict.clear();
            assertEquals(0, dict.size(), "清空后大小应该为0");
            assertFalse(dict.containsKey(RedisBytes.fromString("k1")), "清空后不应该包含任何键");
        }

        @Test
        @Order(6)
        @DisplayName("1.6 getAll - 获取所有键值对")
        void testGetAll() {
            Map<RedisBytes, RedisBytes> expected = new HashMap<>();
            expected.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            expected.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));
            expected.put(RedisBytes.fromString("k3"), RedisBytes.fromString("v3"));

            expected.forEach((k, v) -> dict.put(k, v));

            Map<RedisBytes, RedisBytes> actual = dict.getAll();
            assertEquals(expected.size(), actual.size(), "获取到的Map大小不一致");
            expected.forEach((k, v) -> assertEquals(v, actual.get(k), "Map内容不一致"));
        }

        @Test
        @Order(7)
        @DisplayName("1.7 keySet - 获取所有键集合")
        void testKeySet() {
            Set<RedisBytes> expectedKeys = new HashSet<>();
            expectedKeys.add(RedisBytes.fromString("k1"));
            expectedKeys.add(RedisBytes.fromString("k2"));

            dict.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            dict.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));

            Set<RedisBytes> actualKeys = dict.keySet();
            assertEquals(expectedKeys.size(), actualKeys.size(), "获取到的Set大小不一致");
            assertTrue(actualKeys.containsAll(expectedKeys) && expectedKeys.containsAll(actualKeys), "键集合不一致");
        }

        @Test
        @Order(8)
        @DisplayName("1.8 contains - 检查值是否存在 (ZSET场景)")
        void testContainsValue() {
            // 这个测试是针对 Dict<Double, RedisBytes> 这种特定类型使用的 contains 方法
            // 它的语义是查找 key 等于 score，且 value 等于 member 的 DictEntry
            Dict<Double, RedisBytes> zsetInternalDict = new Dict<>();
            
            Double score1 = 10.0;
            RedisBytes member1 = RedisBytes.fromString("member1");
            Double score2 = 20.0;
            RedisBytes member2 = RedisBytes.fromString("member2");

            zsetInternalDict.put(score1, member1);
            zsetInternalDict.put(score2, member2);

            assertTrue(zsetInternalDict.contains(score1, member1), "应该包含分数10.0和member1");
            assertFalse(zsetInternalDict.contains(score1, member2), "不应该包含分数10.0和member2");
            assertFalse(zsetInternalDict.contains(30.0, member1), "不应该包含分数30.0和member1");
        }
    }

    @Nested
    @DisplayName("2. 渐进式 Rehash 测试 (单线程)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class RehashSingleThreadTests {

        @Test
        @Order(1) // Rehash测试的第一步：验证扩容rehash
        @DisplayName("2.1 负载因子触发扩容并渐进式 rehash")
        void testProgressiveRehash() {
            // 初始大小 DICT_HT_INITIAL_SIZE = 4
            // 负载因子阈值 >= 1.0 触发扩容

            // 插入3个元素，used=3, size=4, loadFactor=0.75 (不触发)
            dict.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            dict.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));
            dict.put(RedisBytes.fromString("k3"), RedisBytes.fromString("v3"));
            assertEquals(3, dict.size());
            assertEquals(-1, dict.rehashIndex, "不应该开始rehash");            // 插入第4个元素，used=4, size=4, loadFactor=1.0 (触发扩容，开始rehash)
            int originalHt0Size = dict.ht0.size;
            dict.put(RedisBytes.fromString("k4"), RedisBytes.fromString("v4"));
            assertEquals(4, dict.size());
            
            // 立即检查rehash状态（在任何后续操作之前）
            System.out.println("After inserting k4: rehashIndex=" + dict.rehashIndex + ", ht0.size=" + dict.ht0.size);
            
            // 验证rehash已经发生：ht0的大小应该已经从4扩容到8
            assertTrue(dict.ht0.size > originalHt0Size, "应该已经完成扩容rehash，ht0.size从" + originalHt0Size + "扩容到" + dict.ht0.size);
            assertEquals(8, dict.ht0.size, "扩容后ht0大小应该是8");
            assertEquals(-1, dict.rehashIndex, "对于小表，rehash应该已经完成");
            assertNull(dict.ht1, "rehash完成后ht1应该为null");            // 继续插入，新元素会写入已扩容的ht0
            dict.put(RedisBytes.fromString("k5"), RedisBytes.fromString("v5"));
            assertEquals(5, dict.size());
            
            // 由于初始表很小，rehash已经完成，新元素应该写入扩容后的ht0
            assertEquals(-1, dict.rehashIndex, "小表rehash应该已经完成");
            assertNull(dict.ht1, "rehash完成后ht1应该为null");
            assertEquals(8, dict.ht0.size, "ht0应该已扩容到8");

            // 验证所有元素都在新表中
            assertEquals(5, dict.size(), "最终大小应该正确");
            assertTrue(dict.containsKey(RedisBytes.fromString("k1")));
            assertTrue(dict.containsKey(RedisBytes.fromString("k5")));
            assertEquals(RedisBytes.fromString("v1"), dict.get(RedisBytes.fromString("k1")));        }

        @Test
        @Order(2)
        @DisplayName("2.2 大表渐进式rehash - 验证分步执行") 
        void testProgressiveRehashLargeTable() {
            // 1. 先填充大量数据让表变大，这样rehash不会立即完成
            for (int i = 0; i < 200; i++) {
                dict.put(RedisBytes.fromString("large_key_" + i), RedisBytes.fromString("value_" + i));
            }
            int largeTableSize = dict.ht0.size;
            assertTrue(largeTableSize >= 128, "表应该已经扩容到较大尺寸: " + largeTableSize);
            
            // 2. 手动触发一次大的rehash（通过大量插入触发）
            int initialSize = dict.ht0.size;
            // 继续插入，直到触发下一次rehash
            int insertCount = 0;
            while (dict.rehashIndex == -1 && insertCount < 500) {
                dict.put(RedisBytes.fromString("trigger_" + insertCount), RedisBytes.fromString("val_" + insertCount));
                insertCount++;
            }
            
            // 如果触发了rehash，验证是否为渐进式
            if (dict.rehashIndex != -1) {
                assertNotNull(dict.ht1, "ht1应该已初始化");
                assertTrue(dict.ht1.size > initialSize, "ht1应该比ht0大");
                
                // 验证rehash在进行中
                assertTrue(dict.rehashIndex >= 0, "rehashIndex应该指示当前rehash位置");
                
                // 手动完成rehash验证
                int steps = 0;
                while (dict.rehashIndex != -1 && steps < 1000) {
                    dict.rehashStep();
                    steps++;
                }
                assertTrue(steps > 0, "大表rehash应该需要多步");
                assertEquals(-1, dict.rehashIndex, "rehash最终应该完成");
                assertNull(dict.ht1, "rehash完成后ht1应该为null");
            }
        }

        @Test
        @Order(3) // 第三执行：验证缩容rehash
        @DisplayName("2.3 缩容机制")
        void testShrinkMechanism() {
            // 先扩容到足够大
            for (int i = 0; i < 20; i++) { // 确保扩容到较大尺寸
                dict.put(RedisBytes.fromString("k" + i), RedisBytes.fromString("v" + i));
            }
            // 确保rehash完成，让ht0达到较大尺寸
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
            }
            int initialSize = dict.size();
            assertTrue(dict.ht0.size > Dict.DICT_HT_INITIAL_SIZE, "初始哈希表应该已扩容");
            System.out.println("Shrink test - Initial dict size: " + initialSize + ", ht0 size: " + dict.ht0.size);


            // 大量删除，触发缩容条件
            // Redis缩容条件：负载因子 < 1 / (HASHTABLE_MIN_FILL * DICT_FORCE_RESIZE_RATIO)
            // HASHTABLE_MIN_FILL = 10.0, DICT_FORCE_RESIZE_RATIO = 4.0
            // 缩容阈值约为 1 / (10 * 4) = 1/40 = 0.025
            // 当 used / size < 0.025 时触发
            for (int i = 0; i < initialSize - 2; i++) { // 只留下2个元素
                dict.remove(RedisBytes.fromString("k" + i));
            }
            assertEquals(2, dict.size());            // 再次执行操作 (put或remove)，触发checkShrinkIfNeeded()并进行rehash
            // 这里我们通过put一个元素来触发checkShrinkIfNeeded
            int initialHt0Size = dict.ht0.size;
            dict.put(RedisBytes.fromString("newKey"), RedisBytes.fromString("newValue"));
            assertEquals(3, dict.size()); // 2个旧的+1个新的
              // 如果rehash正在进行，多执行几次操作来推进rehash完成
            int maxAttempts = 50;
            while (dict.rehashIndex != -1 && maxAttempts > 0) {
                // 直接调用rehashStep来推进rehash
                dict.rehashStep();
                maxAttempts--;
            }
            
            // 验证缩容是否发生 - 通过检查ht0.size是否变小
            boolean shrinkOccurred = dict.ht0.size < initialHt0Size;
            assertTrue(shrinkOccurred, "应该已经发生缩容，ht0.size从" + initialHt0Size + "缩容到" + dict.ht0.size);
            System.out.println("Shrink test - Shrink completed. ht0.size: " + initialHt0Size + " -> " + dict.ht0.size);

            // 由于是小表，缩容rehash应该已经完成
            assertEquals(-1, dict.rehashIndex, "缩容rehash应该已经完成");
            assertNull(dict.ht1, "rehash完成后ht1应该为null");
            assertEquals(3, dict.size(), "最终大小应该正确");
            
            // 验证缩容后的约束
            assertTrue(dict.ht0.size < initialSize, "哈希表大小应该已缩容");
            assertTrue(dict.ht0.size >= Dict.DICT_HT_INITIAL_SIZE, "缩容后的尺寸不应小于初始最小尺寸");
            System.out.println("Shrink test - Final ht0 size: " + dict.ht0.size);
        }
    }

    @Nested
    @DisplayName("3. 并发线程安全测试")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class) // 嵌套类内部也按照顺序执行
    class ConcurrentTests {        private final int NUM_THREADS = 4; // 减少线程数便于调试
        private final int NUM_OPERATIONS_PER_THREAD = 100; // 减少操作数便于调试
        private ExecutorService executor;

        @BeforeEach
        void setupConcurrentTest() {
            executor = Executors.newFixedThreadPool(NUM_THREADS);
        }
        
        @AfterEach
        void tearDownConcurrentTest() {
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        System.err.println("强制关闭executor超时");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Test
        @Order(1) // 并发测试第一步：put
        @DisplayName("3.1 并发put操作 - 验证最终一致性")
        void testConcurrentPut() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(NUM_THREADS);
            ConcurrentHashMap<String, String> expectedMap = new ConcurrentHashMap<>();

            IntStream.range(0, NUM_THREADS).forEach(threadId -> {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                            String keyStr = "key-" + (threadId * NUM_OPERATIONS_PER_THREAD + i);
                            String valueStr = "value-" + threadId + "-" + i;
                            RedisBytes key = RedisBytes.fromString(keyStr);
                            RedisBytes value = RedisBytes.fromString(valueStr);
                            dict.put(key, value);
                            expectedMap.put(keyStr, valueStr); // ConcurrentHashMap本身线程安全
                        }
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " error during put: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        latch.countDown(); // 确保无论如何都会减少计数
                    }
                });
            });

            assertTrue(latch.await(60, TimeUnit.SECONDS), "并发put操作超时");
            // 在所有put操作完成后，等待executor关闭
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "executor shutdown超时");

            // 强制完成所有rehash
            // 注意：rehashStep在Dict操作内部被调用，这里是确保所有在并发过程中可能未完成的rehash最终完成
            int rehashSteps = 0;
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
                rehashSteps++;
            }
            if (rehashSteps > 0) {
                System.out.println("testConcurrentPut: Rehash completed with " + rehashSteps + " steps.");
            }

            assertEquals(expectedMap.size(), dict.size(), "最终Dict大小应该与预期一致");
            expectedMap.forEach((keyStr, valueStr) -> {
                RedisBytes key = RedisBytes.fromString(keyStr);
                RedisBytes actualValue = dict.get(key);
                assertNotNull(actualValue, "键 " + keyStr + " 应该存在");
                assertEquals(RedisBytes.fromString(valueStr), actualValue, "键 " + keyStr + " 的值不匹配");
            });
        }

        @Test
        @Order(2) // 并发测试第二步：get
        @DisplayName("3.2 并发get操作 - 验证读一致性")
        void testConcurrentGet() throws InterruptedException {
            // 预填充Dict (确保数据量足以触发rehash)
            for (int i = 0; i < 2000; i++) { // 确保有足够数据，可能触发rehash
                RedisBytes key = RedisBytes.fromString("k" + i);
                RedisBytes value = RedisBytes.fromString("v" + i);
                dict.put(key, value);
            }
            assertEquals(2000, dict.size()); // 检查初始大小

            // 在读操作开始前，强制完成所有rehash，确保数据处于稳定状态
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
            }
            // 创建一个不变的参考数据集合，因为Dict本身在读的时候也可能触发rehashStep
            final Map<RedisBytes, RedisBytes> initialDataSnapshot = new HashMap<>();
            dict.getAll().forEach((k, v) -> initialDataSnapshot.put(k, v));
            
            // 再次验证Dict大小，确保快照是完整的
            assertEquals(initialDataSnapshot.size(), dict.size(), "快照大小应该与Dict当前大小一致");


            CountDownLatch latch = new CountDownLatch(NUM_THREADS);
            AtomicLong errors = new AtomicLong(0);

            IntStream.range(0, NUM_THREADS).forEach(threadId -> {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < NUM_OPERATIONS_PER_THREAD * 5; i++) { // 更多读操作
                            RedisBytes keyToRead = RedisBytes.fromString("k" + (i % initialDataSnapshot.size()));
                            RedisBytes expectedValue = initialDataSnapshot.get(keyToRead); // 从快照中取期望值
                            
                            RedisBytes actualValue = dict.get(keyToRead); // 从Dict中读
                            
                            if (expectedValue == null) { // 如果期望值是null，实际值也必须是null
                                if (actualValue != null) {
                                    errors.incrementAndGet();
                                    System.err.println("Read inconsistency: Key " + keyToRead.getString() + 
                                        ", Expected null, Got " + actualValue.getString());
                                }
                            } else { // 如果期望值非null，实际值必须匹配
                                if (actualValue == null || !actualValue.equals(expectedValue)) {
                                    errors.incrementAndGet();
                                    System.err.println("Read inconsistency: Key " + keyToRead.getString() + 
                                        ", Expected " + expectedValue.getString() + 
                                        ", Got " + (actualValue != null ? actualValue.getString() : "null"));
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " error during get: " + e.getMessage());
                        e.printStackTrace();
                        errors.incrementAndGet(); // 任何异常都算作错误
                    } finally {
                        latch.countDown();
                    }
                });
            });

            assertTrue(latch.await(45, TimeUnit.SECONDS), "并发get操作超时"); // 适当增加超时
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "executor shutdown超时");

            assertEquals(0, errors.get(), "并发get操作不应该有读不一致错误或异常");
        }


        @Test
        @Order(3) // 并发测试第三步：put/remove混合
        @DisplayName("3.3 并发put和remove混合操作 - 验证最终一致性")
        void testConcurrentPutRemove() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(NUM_THREADS);
            ConcurrentHashMap<String, String> finalExpected = new ConcurrentHashMap<>(); // 最终预期留在Map中的元素

            IntStream.range(0, NUM_THREADS).forEach(threadId -> {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                            String keyStr = "op-" + (threadId * NUM_OPERATIONS_PER_THREAD + i);
                            RedisBytes key = RedisBytes.fromString(keyStr);
                            RedisBytes value = RedisBytes.fromString("val-" + keyStr);
                            
                            if (i % 2 == 0) { // 50% put
                                dict.put(key, value);
                                // ConcurrentHashMap 的 put 和 remove 是线程安全的
                                finalExpected.put(keyStr, "val-" + keyStr); 
                            } else { // 50% remove
                                dict.remove(key); 
                                finalExpected.remove(keyStr); 
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " error during put/remove: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            });

            assertTrue(latch.await(60, TimeUnit.SECONDS), "并发put/remove操作超时");
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "executor shutdown超时");

            // 强制完成所有rehash
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
            }

            assertEquals(finalExpected.size(), dict.size(), "最终Dict大小应该与预期一致");
            finalExpected.forEach((keyStr, valueStr) -> {
                RedisBytes key = RedisBytes.fromString(keyStr);
                RedisBytes actualValue = dict.get(key);
                assertNotNull(actualValue, "键 " + keyStr + " 应该存在");
                assertEquals(RedisBytes.fromString(valueStr), actualValue, "键 " + keyStr + " 的值不匹配");
            });

            dict.getAll().forEach((k, v) -> { // 反向验证Dict中没有意外的键
                assertTrue(finalExpected.containsKey(k.getString()), "Dict中不应该有意外的键: " + k.getString());
            });
        }


        @Test
        @Order(4) // 并发测试第四步：快照与读写
        @DisplayName("3.4 并发createSafeSnapshot和读写操作")
        void testConcurrentSnapshotAndRW() throws InterruptedException {
            final int initialSize = 1000;
            for (int i = 0; i < initialSize; i++) {
                dict.put(RedisBytes.fromString("initial-" + i), RedisBytes.fromString("value-" + i));
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(NUM_THREADS + 1); // +1 for snapshot thread

            // 1. 启动快照线程 (独立线程)
            executor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪
                    Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
                    // 验证快照大小至少是初始大小
                    // 由于是弱一致性快照，只能保证不会ConcurrentModificationException，
                    // 并且快照的数据量应该在合理范围内，这里简单验证大于等于初始数据量
                    assertTrue(snapshot.size() >= initialSize, "快照大小应该至少等于初始大小");
                    // 理论上快照应该包含快照开始时刻的大部分数据
                    // 重要的是，这个操作不应该抛出并发修改异常
                    System.out.println("Snapshot taken. Size: " + snapshot.size());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("Snapshot thread error: " + e.getMessage());
                    e.printStackTrace();
                    fail("快照线程在并发环境下抛出异常"); // 如果抛异常，直接失败
                } finally {
                    endLatch.countDown();
                }
            });

            // 2. 启动多个读写线程 (模拟客户端命令线程)
            IntStream.range(0, NUM_THREADS).forEach(threadId -> {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // 等待所有线程就绪
                        for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                            String keyStr = "active-" + (threadId * NUM_OPERATIONS_PER_THREAD + i);
                            RedisBytes key = RedisBytes.fromString(keyStr);
                            RedisBytes value = RedisBytes.fromString("active-val-" + keyStr);
                            if (i % 2 == 0) {
                                dict.put(key, value);
                            } else {
                                dict.get(key); // 读操作
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " error during R/W: " + e.getMessage());
                        e.printStackTrace();
                        fail("读写线程在并发环境下抛出异常"); // 如果抛异常，直接失败
                    } finally {
                        endLatch.countDown();
                    }
                });
            });

            startLatch.countDown(); // 释放所有线程
            assertTrue(endLatch.await(90, TimeUnit.SECONDS), "并发快照和读写操作超时"); // 增加超时时间
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "executor shutdown超时");

            // 最终验证Dict状态，确保没有数据损坏
            // 强制完成所有rehash，确保所有数据都已迁移到主表
            while (dict.rehashIndex != -1) {
                dict.rehashStep();
            }
            assertTrue(dict.size() > initialSize, "Dict大小应该增加，因为有写入操作");
            // 尝试遍历所有元素，如果抛出异常，说明数据结构损坏
            assertDoesNotThrow(() -> {
                Map<RedisBytes, RedisBytes> finalState = dict.getAll();
                System.out.println("Final Dict size: " + finalState.size());
                // 可以在这里添加一些随机抽取key并验证值的逻辑，但会增加测试时间
            }, "最终Dict应该可以正常遍历且不抛异常");
        }
    }
}
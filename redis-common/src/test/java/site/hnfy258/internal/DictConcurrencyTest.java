package site.hnfy258.internal;

import org.junit.jupiter.api.*;
import site.hnfy258.datastructure.RedisBytes;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;


@DisplayName("Dict 并发和 CoW 特性压力测试")
class DictConcurrencyTest {

    private static final int INITIAL_KEYS = 2000;
    private static final int BACKGROUND_READER_THREADS = 5;
    private static final int TOTAL_PUT_OPERATIONS = 5000;
    private static final int SNAPSHOT_TRIGGER_SIZE = 4000;

    private ExecutorService commandExecutor;
    private ExecutorService backgroundExecutor;

    private Dict<RedisBytes, RedisBytes> dict;
    private ConcurrentHashMap<RedisBytes, RedisBytes> expectedFinalState;

    // 使用AtomicReference来同步快照状态
    private AtomicReference<Map<RedisBytes, RedisBytes>> snapshotReference;

    private CountDownLatch startSignal;
    private CountDownLatch snapshotTriggerSignal; // 通知开始准备快照
    private CountDownLatch snapshotReadySignal;   // 确保快照数据已准备好
    private CountDownLatch readersFinishedSignal;
    private AtomicBoolean testFailed;

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        expectedFinalState = new ConcurrentHashMap<>();
        snapshotReference = new AtomicReference<>();

        commandExecutor = Executors.newSingleThreadExecutor();
        backgroundExecutor = Executors.newFixedThreadPool(BACKGROUND_READER_THREADS);

        startSignal = new CountDownLatch(1);
        snapshotTriggerSignal = new CountDownLatch(1);
        snapshotReadySignal = new CountDownLatch(1);
        readersFinishedSignal = new CountDownLatch(BACKGROUND_READER_THREADS);
        testFailed = new AtomicBoolean(false);

        System.out.println("\n--- 开始新测试 ---");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        shutdownExecutor(commandExecutor, "Command Executor");
        shutdownExecutor(backgroundExecutor, "Background Executor");
        System.out.println("测试完成，Dict 最终大小: " + dict.size());
        System.out.println("--------------------\n");
    }

    @Test
    @DisplayName("单线程写 + 多线程并发快照的最终验证")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testSingleWriterWithConcurrentSnapshots() throws Exception {

        // --- 1. 准备并启动后台读取线程 ---
        for (int i = 0; i < BACKGROUND_READER_THREADS; i++) {
            final int threadId = i;
            backgroundExecutor.submit(() -> {
                try {
                    startSignal.await();
                    snapshotTriggerSignal.await(); // 等待触发信号
                    snapshotReadySignal.await();   // 等待快照数据准备好

                    System.out.println("后台线程 " + threadId + ": 开始创建快照...");
                    Map<RedisBytes, RedisBytes> actualSnapshot = dict.createSafeSnapshot();

                    // 获取期望的快照状态
                    Map<RedisBytes, RedisBytes> expectedSnapshot = snapshotReference.get();
                    if (expectedSnapshot == null) {
                        failTest("后台线程 " + threadId + ": 期望快照未设置！");
                        return;
                    }

                    // 验证快照一致性
                    assertMapEquals(expectedSnapshot, actualSnapshot, "后台线程 " + threadId);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    failTest("后台线程 " + threadId + " 出现异常: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    readersFinishedSignal.countDown();
                }
            });
        }

        // --- 2. 准备并启动写入线程 ---
        Future<Void> writerFuture = commandExecutor.submit(() -> {
            startSignal.await();
            for (int i = 0; i < TOTAL_PUT_OPERATIONS; i++) {
                RedisBytes key = RedisBytes.fromString("cmd_key_" + i);
                RedisBytes value = RedisBytes.fromString("cmd_value_" + i);

                dict.put(key, value);
                expectedFinalState.put(key, value);

                // 【关键修复】当达到快照触发点时，暂停写入来创建一致性快照
                if (dict.size() == SNAPSHOT_TRIGGER_SIZE) {
                    System.out.println("写入线程: Dict 大小达到 " + SNAPSHOT_TRIGGER_SIZE + "，创建快照基准...");

                    // 1. 先通知后台线程准备
                    snapshotTriggerSignal.countDown();

                    // 2. 创建当前状态的快照作为基准
                    Map<RedisBytes, RedisBytes> baselineSnapshot = dict.createSafeSnapshot();
                    snapshotReference.set(baselineSnapshot);

                    System.out.println("写入线程: 基准快照已创建(大小: " + baselineSnapshot.size() + ")，通知后台线程开始验证...");

                    // 3. 通知后台线程可以开始验证了
                    snapshotReadySignal.countDown();

                    // 4. 短暂暂停，让后台线程有时间开始快照创建
                    Thread.sleep(50);

                    System.out.println("写入线程: 继续后续写入操作...");
                }
            }
            return null;
        });

        // --- 3. 统一开始，并等待所有任务完成 ---
        startSignal.countDown();
        writerFuture.get();
        System.out.println("写入线程已完成所有 " + TOTAL_PUT_OPERATIONS + " 次 put 操作。");

        if (!readersFinishedSignal.await(15, TimeUnit.SECONDS)) {
            fail("主线程：并非所有后台线程都在规定时间内完成快照验证！");
        }
        System.out.println("所有后台线程已完成快照验证。");

        // --- 4. 最终断言 ---
        assertFalse(testFailed.get(), "并发快照验证过程中出现了错误，请检查上方日志的 ❌ 标记。");
        System.out.println("✅ 并发快照验证通过！");

        // 验证最终状态
        System.out.println("\n--- 最终 Dict 状态一致性验证 ---");
        ensureRehashComplete(dict);
        Map<RedisBytes, RedisBytes> finalContent = dict.createSafeSnapshot();
        assertMapEquals(expectedFinalState, finalContent, "最终Dict状态");
        System.out.println("✅ 最终 Dict 内容与所有写入命令的累积结果完全一致。");
    }

    @Test
    @DisplayName("版本号机制确保快照一致性")
    void testVersionBasedSnapshotConsistency() throws Exception {
        // 先构建一个稳定状态
        for (int i = 0; i < 100; i++) {
            dict.put(RedisBytes.fromString("key_" + i),
                    RedisBytes.fromString("value_" + i));
        }

        // 停止写入，确保状态稳定
        long stableVersion = dict.getCurrentVersion();

        // 现在多个线程同时创建快照 - 这时应该完全一致
        CyclicBarrier barrier = new CyclicBarrier(10);
        ConcurrentHashMap<Integer, Map<RedisBytes, RedisBytes>> snapshots = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    barrier.await(); // 真正的同时开始
                    Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
                    snapshots.put(threadId, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        // 验证：相同版本产生的快照必须一致
        Map<RedisBytes, RedisBytes> reference = snapshots.get(0);
        for (int i = 1; i < 10; i++) {
            Map<RedisBytes, RedisBytes> other = snapshots.get(i);
            assertEquals(reference.size(), other.size());
            assertEquals(reference, other); // 完全相等
        }
    }

    @Test
    @DisplayName("并发写入期间快照的自洽性")
    void testSnapshotSelfConsistency() throws Exception {
        AtomicBoolean stopWriting = new AtomicBoolean(false);
        Set<String> allPossibleKeys = ConcurrentHashMap.newKeySet();

        // 写入线程
        Thread writer = new Thread(() -> {
            int counter = 0;
            while (!stopWriting.get() && counter < 1000) {
                String key = "key_" + counter;
                String value = "value_" + counter;
                allPossibleKeys.add(key);
                dict.put(RedisBytes.fromString(key), RedisBytes.fromString(value));
                counter++;

                if (counter % 10 == 0) {
                    try { Thread.sleep(1); } catch (InterruptedException e) {}
                }
            }
        });

        writer.start();

        // 多个快照线程
        List<Map<RedisBytes, RedisBytes>> snapshots = new CopyOnWriteArrayList<>();
        CountDownLatch snapshotLatch = new CountDownLatch(5);

        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                try {
                    Thread.sleep(100); // 让写入进行一段时间
                    Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
                    snapshots.add(snapshot);
                    // 验证每个快照内部的自洽性
                    validateSnapshotConsistency(snapshot, allPossibleKeys);
                } catch (Exception e) {
                    fail("快照创建失败: " + e.getMessage());
                } finally {
                    snapshotLatch.countDown();
                }
            }).start();
        }

        snapshotLatch.await();
        stopWriting.set(true);
        writer.join();

        // 验证：每个快照都是完整且自洽的
        assertTrue(snapshots.size() == 5);
        for (Map<RedisBytes, RedisBytes> snapshot : snapshots) {
            // 每个快照应该是某个时刻Dict状态的完整表示
            validateSnapshotIntegrity(snapshot);
        }
    }

    private void validateSnapshotConsistency(Map<RedisBytes, RedisBytes> snapshot,
                                             Set<String> allPossibleKeys) {
        // 验证快照的内部一致性
        for (Map.Entry<RedisBytes, RedisBytes> entry : snapshot.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();

            // 如果存在key_N，那么对应的值必须是value_N
            if (key.startsWith("key_")) {
                String expectedValue = key.replace("key_", "value_");
                assertEquals(expectedValue, value, "快照中键值对不匹配");
            }
        }
    }

    /**
     * 验证快照的完整性 - 更实用的版本
     */
    private void validateSnapshotIntegrity(Map<RedisBytes, RedisBytes> snapshot) {
        assertNotNull(snapshot, "快照不能为null");

        // 1. 验证快照的基本属性
        for (Map.Entry<RedisBytes, RedisBytes> entry : snapshot.entrySet()) {
            RedisBytes key = entry.getKey();
            RedisBytes value = entry.getValue();

            assertNotNull(key, "快照中的键不能为null");
            assertNotNull(value, "快照中的值不能为null");

            String keyStr = key.toString();
            String valueStr = value.toString();

            // 验证键值对的逻辑关系
            if (keyStr.startsWith("key_")) {
                String expectedValue = keyStr.replace("key_", "value_");
                assertEquals(expectedValue, valueStr,
                        "键值对逻辑错误: " + keyStr + " -> " + valueStr);
            }
        }

        // 2. 验证快照没有重复的键（这应该由Map保证，但我们还是检查一下）
        Set<RedisBytes> keys = new HashSet<>(snapshot.keySet());
        assertEquals(snapshot.size(), keys.size(), "快照中存在重复的键");

        System.out.println("✅ 快照完整性验证通过，大小: " + snapshot.size());
    }


    // === 辅助方法 ===

    /**
     * 【修复】确保rehash完成 - 兼容新的Dict实现
     */
    private void ensureRehashComplete(Dict<?, ?> dict) throws InterruptedException {
        int maxWaitSteps = 10000;
        int waitCount = 0;

        // 新的Dict实现中，rehash是在put/remove操作中自动进行的
        // 我们通过检查isRehashing状态来确认是否还在rehash过程中
        while (dict.isRehashing() && waitCount < maxWaitSteps) {
            // 通过执行一些轻量级操作来推进rehash
            dict.size(); // 触发内部状态检查
            Thread.sleep(1);
            waitCount++;
        }

        if (dict.isRehashing()) {
            fail("Rehash 过程未能在 " + maxWaitSteps + " 步内完成，可能存在逻辑问题。");
        }

        System.out.println("Rehash 完成确认，等待了 " + waitCount + " 步");
    }

    private void failTest(String message) {
        System.err.println("❌ " + message);
        testFailed.set(true);
    }

    private <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual, String context) {
        if (expected.size() != actual.size()) {
            failTest(context + " 快照大小不匹配！预期: " + expected.size() + ", 实际: " + actual.size());
            return;
        }
        for (Map.Entry<K, V> entry : expected.entrySet()) {
            V actualValue = actual.get(entry.getKey());
            if (actualValue == null || !actualValue.equals(entry.getValue())) {
                failTest(context + " 快照内容不匹配，键: " + entry.getKey() +
                        ", 预期: " + entry.getValue() + ", 实际: " + actualValue);
                return;
            }
        }
    }

    private void shutdownExecutor(ExecutorService executor, String name) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.err.println(name + " 未能在5秒内关闭，强制关闭。");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

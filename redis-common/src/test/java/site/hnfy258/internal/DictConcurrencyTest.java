package site.hnfy258.internal;

import org.junit.jupiter.api.*;
import site.hnfy258.datastructure.RedisBytes;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 【修复版】Dict 并发和 CoW 特性压力测试
 * 已兼容新的Dict实现
 */
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

    // === 另一个更严格的测试方案 ===
    @Test
    @DisplayName("真正的并发快照压力测试 - 持续写入期间的快照一致性")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testTrueConcurrentSnapshots() throws Exception {
        final AtomicBoolean stopWriting = new AtomicBoolean(false);
        final CyclicBarrier snapshotBarrier = new CyclicBarrier(BACKGROUND_READER_THREADS);
        final AtomicReference<Long> snapshotTimestamp = new AtomicReference<>();
        final ConcurrentHashMap<Integer, Map<RedisBytes, RedisBytes>> snapshots = new ConcurrentHashMap<>();

        // 启动持续写入线程
        Future<Void> writerFuture = commandExecutor.submit(() -> {
            int counter = 0;
            while (!stopWriting.get()) {
                RedisBytes key = RedisBytes.fromString("concurrent_key_" + counter);
                RedisBytes value = RedisBytes.fromString("concurrent_value_" + counter);
                dict.put(key, value);
                expectedFinalState.put(key, value);
                counter++;

                if (counter % 100 == 0) {
                    Thread.sleep(1); // 偶尔让出CPU
                }
            }
            return null;
        });

        // 启动多个快照线程
        CountDownLatch allSnapshotsComplete = new CountDownLatch(BACKGROUND_READER_THREADS);
        for (int i = 0; i < BACKGROUND_READER_THREADS; i++) {
            final int threadId = i;
            backgroundExecutor.submit(() -> {
                try {
                    // 让Dict先有一些数据
                    Thread.sleep(200);

                    // 所有线程同时开始快照
                    snapshotBarrier.await();
                    long timestamp = System.nanoTime();
                    snapshotTimestamp.compareAndSet(null, timestamp);

                    System.out.println("快照线程 " + threadId + " 开始创建快照...");
                    Map<RedisBytes, RedisBytes> snapshot = dict.createSafeSnapshot();
                    snapshots.put(threadId, snapshot);
                    System.out.println("快照线程 " + threadId + " 完成快照，大小: " + snapshot.size());

                } catch (Exception e) {
                    failTest("快照线程 " + threadId + " 异常: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    allSnapshotsComplete.countDown();
                }
            });
        }

        // 等待快照完成
        if (!allSnapshotsComplete.await(10, TimeUnit.SECONDS)) {
            fail("快照线程未在规定时间内完成");
        }

        // 停止写入
        stopWriting.set(true);
        writerFuture.get();

        // 验证所有快照的一致性
        System.out.println("\n--- 验证并发快照一致性 ---");
        if (snapshots.size() != BACKGROUND_READER_THREADS) {
            fail("未收集到所有线程的快照");
        }

        Map<RedisBytes, RedisBytes> firstSnapshot = snapshots.get(0);
        for (int i = 1; i < BACKGROUND_READER_THREADS; i++) {
            Map<RedisBytes, RedisBytes> otherSnapshot = snapshots.get(i);
            if (firstSnapshot.size() != otherSnapshot.size()) {
                fail("快照 0 和快照 " + i + " 大小不一致: " + firstSnapshot.size() + " vs " + otherSnapshot.size());
            }

            // 检查内容一致性
            for (Map.Entry<RedisBytes, RedisBytes> entry : firstSnapshot.entrySet()) {
                if (!entry.getValue().equals(otherSnapshot.get(entry.getKey()))) {
                    fail("快照 0 和快照 " + i + " 在键 " + entry.getKey() + " 上的值不一致");
                }
            }
        }

        System.out.println("✅ 所有 " + BACKGROUND_READER_THREADS + " 个并发快照完全一致！(大小: " + firstSnapshot.size() + ")");
        assertFalse(testFailed.get());
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

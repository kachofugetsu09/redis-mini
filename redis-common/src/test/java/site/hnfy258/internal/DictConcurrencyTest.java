package site.hnfy258.internal;

import org.junit.jupiter.api.*;
import site.hnfy258.datastructure.RedisBytes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Dict 并发和 CoW 特性测试")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DictConcurrencyTest {

    private Dict<RedisBytes, RedisBytes> dict;
    private static final int INITIAL_KEYS_FOR_TEST = 2000;
    private static final int COMMAND_EXECUTOR_THREAD_COUNT = 1;
    private static final int BACKGROUND_THREAD_COUNT = 5;
    // 命令执行器只需 Put 到触发点即可，后续 Put 用于验证快照隔离
    private static final int COMMAND_OPERATIONS_PUT_BEFORE_AFTER_SNAPSHOT = 5000; // 例如，put到4000，再put 1000

    private static final int SNAPSHOT_TRIGGER_SIZE = 4000; // 当 Dict 大小达到这个值时触发快照

    private ExecutorService commandExecutor;
    private ExecutorService backgroundExecutor;

    private ConcurrentHashMap<RedisBytes, RedisBytes> expectedCommandState; // 用于最终验证 Dict 状态
    private volatile Map<RedisBytes, RedisBytes> expectedSnapshotState = null; // 用于精确快照的“黄金标准”

    private CountDownLatch snapshotSignalLatch; // 命令执行器发送快照信号
    private CountDownLatch allBackgroundSnapshotsCreatedLatch; // 多个后台线程都创建了快照后计数归零
    private AtomicBoolean snapshotTestFailed; // 用于后台线程标记快照验证是否失败

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        commandExecutor = Executors.newFixedThreadPool(COMMAND_EXECUTOR_THREAD_COUNT);
        backgroundExecutor = Executors.newFixedThreadPool(BACKGROUND_THREAD_COUNT);
        expectedCommandState = new ConcurrentHashMap<>();
        expectedSnapshotState = null;
        snapshotSignalLatch = new CountDownLatch(1); // 每次测试需要新的 latch
        allBackgroundSnapshotsCreatedLatch = new CountDownLatch(BACKGROUND_THREAD_COUNT); // 每个后台线程创建快照后减1
        snapshotTestFailed = new AtomicBoolean(false); // 重置
        System.out.println("\n--- 开始新测试 ---");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        // 强制完成所有 rehash，确保 Dict 最终处于稳定状态
        int rehashSteps = 0;
        Dict.DictState<?, ?> currentState;
        do {
            currentState = dict.state.get();
            if (currentState.rehashIndex == -1) break;
            dict.rehashStep(); // 积极推进 rehash
            rehashSteps++;
            Thread.sleep(1); // 避免过度自旋
        } while (true);

        if (rehashSteps > 0) {
            System.out.println("TearDown: Rehash completed with " + rehashSteps + " steps.");
        }

        if (commandExecutor != null && !commandExecutor.isShutdown()) {
            commandExecutor.shutdownNow();
            commandExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (backgroundExecutor != null && !backgroundExecutor.isShutdown()) {
            backgroundExecutor.shutdownNow();
            backgroundExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        System.out.println("测试完成，Dict 最终大小: " + dict.size());
        System.out.println("--------------------\n");
    }

    @Nested
    @DisplayName("1. 基础功能和 CoW 快照行为 (单线程验证)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class BasicCoWTests {

        @Test
        @Order(1)
        @DisplayName("1.1 put 插入/更新和 remove 删除键")
        void testBasicPutRemove() {
            RedisBytes key1 = RedisBytes.fromString("key1");
            RedisBytes value1 = RedisBytes.fromString("value1");
            RedisBytes key2 = RedisBytes.fromString("key2");
            RedisBytes value2 = RedisBytes.fromString("value2");

            assertNull(dict.put(key1, value1));
            assertEquals(1, dict.size());
            assertEquals(value1, dict.get(key1));
            assertTrue(dict.containsKey(key1));

            assertEquals(value1, dict.put(key1, RedisBytes.fromString("newValue1")));
            assertEquals(1, dict.size());
            assertEquals(RedisBytes.fromString("newValue1"), dict.get(key1));

            assertNull(dict.put(key2, value2));
            assertEquals(2, dict.size());

            assertEquals(value2, dict.remove(key2));
            assertEquals(1, dict.size());
            assertNull(dict.get(key2));
            assertFalse(dict.containsKey(key2));

            assertNull(dict.remove(RedisBytes.fromString("nonExistent")));
            assertEquals(1, dict.size());
        }

        @Test
        @Order(2)
        @DisplayName("1.2 clear 清空 Dict")
        void testClear() {
            dict.put(RedisBytes.fromString("k1"), RedisBytes.fromString("v1"));
            dict.put(RedisBytes.fromString("k2"), RedisBytes.fromString("v2"));
            assertEquals(2, dict.size());
            dict.clear();
            assertEquals(0, dict.size());
            assertTrue(dict.keySet().isEmpty());
        }

        @Test
        @Order(3)
        @DisplayName("1.3 快照 (createSafeSnapshot) 在单线程下的精确内容验证")
        void testCreateSafeSnapshotPrecise() throws InterruptedException {
            for (int i = 0; i < INITIAL_KEYS_FOR_TEST; i++) {
                RedisBytes key = RedisBytes.fromString("snap_key_" + i);
                RedisBytes value = RedisBytes.fromString("snap_value_" + i);
                dict.put(key, value);
                expectedCommandState.put(key, value);
            }
            System.out.println("预填充 " + INITIAL_KEYS_FOR_TEST + " 键完成，Dict 大小: " + dict.size());
            ensureRehashComplete(dict);
            System.out.println("预填充后 Dict 主表容量: " + dict.state.get().ht0.size);

            System.out.println("创建快照...");
            Map<RedisBytes, RedisBytes> snapshotAtFork = dict.createSafeSnapshot();
            System.out.println("快照创建完成，大小: " + snapshotAtFork.size());

            assertEquals(expectedCommandState.size(), snapshotAtFork.size(), "快照大小应与期望状态大小一致");
            expectedCommandState.forEach((key, value) ->
                    assertEquals(value, snapshotAtFork.get(key), "快照中键 " + key.getString() + " 的值应匹配")
            );
            snapshotAtFork.keySet().forEach(key ->
                    assertTrue(expectedCommandState.containsKey(key), "快照中不应有预期之外的键: " + key.getString())
            );
            System.out.println("✅ 快照内容在创建时与 Dict 状态完全一致。");

            System.out.println("修改原始 Dict (添加和删除元素)...");
            int newAdds = 1000;
            int removedCount = 500;
            for (int i = 0; i < newAdds; i++) {
                RedisBytes key = RedisBytes.fromString("new_key_" + i);
                RedisBytes value = RedisBytes.fromString("new_value_" + i);
                dict.put(key, value);
                expectedCommandState.put(key, value);
            }
            for (int i = 0; i < removedCount; i++) {
                RedisBytes key = RedisBytes.fromString("snap_key_" + i);
                dict.remove(key);
                expectedCommandState.remove(key);
            }
            ensureRehashComplete(dict);
            System.out.println("原始 Dict 修改完成，当前大小: " + dict.size());

            assertEquals(INITIAL_KEYS_FOR_TEST, snapshotAtFork.size(), "快照大小不应改变");
            assertFalse(snapshotAtFork.containsKey(RedisBytes.fromString("new_key_0")), "快照不应包含新添加的键");
            assertTrue(snapshotAtFork.containsKey(RedisBytes.fromString("snap_key_0")), "快照应包含已被删除的键");
            System.out.println("✅ 快照 CoW 特性验证通过：大小不变，内容与 'fork' 时刻一致。");

            assertEquals(expectedCommandState.size(), dict.size(), "最终 Dict 大小应与期望状态一致");
            expectedCommandState.forEach((key, value) ->
                    assertEquals(value, dict.get(key), "最终 Dict 中键 " + key.getString() + " 的值应匹配")
            );
            dict.keySet().forEach(key ->
                    assertTrue(expectedCommandState.containsKey(key), "最终 Dict 中不应有意外的键: " + key.getString())
            );
            System.out.println("✅ 原始 Dict 最终状态与期望完全一致。");
        }
    }

    @Nested
    @DisplayName("3. 并发快照和后台读测试 (模拟 AOF 重写/RDB 快照)")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class ConcurrentSnapshotAndReadTests {

        @BeforeEach
        void setupConcurrentTest() throws InterruptedException {
            // 预填充 Dict 到 SNAPSHOT_TRIGGER_SIZE - 初始数量，确保后续 put 能达到触发点
            // 这里初始填充少量，后续命令执行器主要执行 put
            int initialFillCount = INITIAL_KEYS_FOR_TEST; // 保持2000个，确保rehash有数据
            for (int i = 0; i < initialFillCount; i++) {
                RedisBytes key = RedisBytes.fromString("init_key_" + i);
                RedisBytes value = RedisBytes.fromString("init_value_" + i);
                dict.put(key, value);
                expectedCommandState.put(key, value);
            }
            System.out.println("预填充 Dict 完成，大小: " + dict.size());
            ensureRehashComplete(dict);
            System.out.println("初始 Rehash 完成，Dict 主表容量: " + dict.state.get().ht0.size);
        }

        @Test
        @Order(1)
        @DisplayName("3.1 单命令执行器纯写 + 后台多线程精确快照验证")
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testCommandExecutorPureWritesWithBackgroundPreciseSnapshots() throws InterruptedException, java.util.concurrent.ExecutionException {
            CountDownLatch startLatch = new CountDownLatch(1);

            // 1. 启动后台线程 (只负责创建精确快照和验证)
            for (int i = 0; i < BACKGROUND_THREAD_COUNT; i++) {
                int threadId = i;
                backgroundExecutor.submit(() -> {
                    try {
                        startLatch.await(); // 等待所有线程就绪

                        // 等待快照信号
                        snapshotSignalLatch.await();

                        System.out.println("后台线程 " + threadId + ": 收到快照信号，正在创建精确快照...");
                        Map<RedisBytes, RedisBytes> preciseSnapshot = dict.createSafeSnapshot();

                        // **核心验证：快照大小和内容与预期状态的精确比对**
                        boolean currentSnapshotMatches = true;
                        if (preciseSnapshot.size() != SNAPSHOT_TRIGGER_SIZE) { // 直接验证大小是否为4000
                            System.err.println("❌ 后台线程 " + threadId + ": 精确快照大小不匹配！预期: " + SNAPSHOT_TRIGGER_SIZE + ", 实际: " + preciseSnapshot.size());
                            currentSnapshotMatches = false;
                        } else {
                            // 验证快照内容是否与 expectedSnapshotState 完全一致
                            // expectedSnapshotState 是在命令执行器达到4000时复制的，这里做严格比对
                            for (Map.Entry<RedisBytes, RedisBytes> entry : expectedSnapshotState.entrySet()) {
                                if (!preciseSnapshot.containsKey(entry.getKey()) || !preciseSnapshot.get(entry.getKey()).equals(entry.getValue())) {
                                    System.err.println("❌ 后台线程 " + threadId + ": 精确快照内容不匹配或缺失！键: " + entry.getKey().getString());
                                    currentSnapshotMatches = false;
                                    break;
                                }
                            }
                            if (currentSnapshotMatches) {
                                // 反向检查，确保快照没有预期之外的键 (例如，命令执行器在快照后新增的键)
                                for (Map.Entry<RedisBytes, RedisBytes> entry : preciseSnapshot.entrySet()) {
                                    if (!expectedSnapshotState.containsKey(entry.getKey())) { // 仅检查是否存在，值在前面已比对
                                        System.err.println("❌ 后台线程 " + threadId + ": 精确快照包含预期之外的键！键: " + entry.getKey().getString());
                                        currentSnapshotMatches = false;
                                        break;
                                    }
                                }
                            }
                        }

                        if (currentSnapshotMatches) {
                            System.out.println("✅ 后台线程 " + threadId + ": 精确快照内容与预期完全一致！");
                        } else {
                            snapshotTestFailed.set(true); // 标记测试失败
                        }

                        allBackgroundSnapshotsCreatedLatch.countDown(); // 通知主线程本后台线程已完成快照创建和验证

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("后台线程 " + threadId + " 错误: " + e.getMessage());
                        e.printStackTrace();
                        snapshotTestFailed.set(true);
                    }
                });
            }

            // 2. 启动命令执行器线程 (模拟 Redis 主线程 - 纯 Put 操作)
            Future<Void> commandExecutorFuture = commandExecutor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪

                    for (int i = 0; i < COMMAND_OPERATIONS_PUT_BEFORE_AFTER_SNAPSHOT; i++) {
                        RedisBytes key = RedisBytes.fromString("cmd_put_key_" + i); // 纯 Put 新键
                        RedisBytes value = RedisBytes.fromString("cmd_put_value_" + i);

                        dict.put(key, value);
                        expectedCommandState.put(key, value); // 更新最终期望状态

                        dict.rehashStep(); // 推进 rehash

                        // **核心逻辑：在 Dict 大小达到 SNAPSHOT_TRIGGER_SIZE 时触发精确快照**
                        if (dict.size() == SNAPSHOT_TRIGGER_SIZE) { // 精确到4000
                            System.out.println("命令执行器: Dict 大小达到 " + dict.size() + "，准备触发精确快照！");
                            // **重要：在这一刻复制 expectedCommandState 作为快照的“黄金标准”**
                            expectedSnapshotState = new ConcurrentHashMap<>(expectedCommandState);
                            snapshotSignalLatch.countDown(); // 发送信号给所有后台线程
                            System.out.println("命令执行器: 已发送快照信号。等待所有后台线程创建并验证精确快照...");

                            // 等待所有后台线程完成快照创建和验证
                            allBackgroundSnapshotsCreatedLatch.await(15, TimeUnit.SECONDS); // 增加等待时间

                            if (allBackgroundSnapshotsCreatedLatch.getCount() > 0) {
                                System.err.println("❌ 命令执行器: 后台线程未在规定时间内全部创建并验证精确快照！");
                                snapshotTestFailed.set(true);
                            } else {
                                System.out.println("✅ 命令执行器: 所有后台线程已成功创建并验证精确快照。继续执行命令...");
                            }
                        }
                        // 模拟命令执行耗时
                        Thread.sleep(ThreadLocalRandom.current().nextInt(2) + 1);
                    }
                    return null;
                } catch (Exception e) {
                    System.err.println("命令执行器线程错误: " + e.getMessage());
                    e.printStackTrace();
                    snapshotTestFailed.set(true); // 标记测试失败
                    throw e;
                }
            });

            startLatch.countDown(); // 释放所有线程
            commandExecutorFuture.get(); // 等待命令执行器线程完成
            backgroundExecutor.shutdown();
            assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS), "后台线程应在规定时间内完成");

            System.out.println("\n--- 并发操作统计 ---");


            // 断言精确快照测试是否失败
            assertFalse(snapshotTestFailed.get(), "精确快照验证过程中不应出现失败！");


            // 最终 Dict 状态验证 (与 expectedCommandState 比较)
            System.out.println("\n--- 最终 Dict 状态一致性验证 ---");
            ensureRehashComplete(dict);

            Map<RedisBytes, RedisBytes> finalDictContent = dict.createSafeSnapshot();
            assertEquals(expectedCommandState.size(), finalDictContent.size(), "最终 Dict 大小应与命令执行器预期状态一致");

            boolean finalContentMatches = true;
            for (Map.Entry<RedisBytes, RedisBytes> entry : expectedCommandState.entrySet()) {
                if (!finalDictContent.containsKey(entry.getKey()) || !finalDictContent.get(entry.getKey()).equals(entry.getValue())) {
                    System.err.println("❌ 最终验证错误：预期键值对丢失或不匹配！键: " + entry.getKey().getString() +
                            ", 期望值: " + entry.getValue().getString() + ", 实际值: " + finalDictContent.get(entry.getKey()));
                    finalContentMatches = false;
                }
            }
            for (Map.Entry<RedisBytes, RedisBytes> entry : finalDictContent.entrySet()) {
                if (!expectedCommandState.containsKey(entry.getKey()) || !expectedCommandState.get(entry.getKey()).equals(entry.getValue())) {
                    System.err.println("❌ 最终验证错误：Dict 中包含预期之外的键值对！键: " + entry.getKey().getString() +
                            ", 实际值: " + entry.getValue().getString());
                    finalContentMatches = false;
                }
            }
            assertTrue(finalContentMatches, "最终 Dict 内容应与命令执行器预期状态完全一致");
            System.out.println("✅ 最终 Dict 内容与命令执行器预期状态完全一致。");
        }
    }

    private static void ensureRehashComplete(Dict<?, ?> dict) throws InterruptedException {
        int rehashSteps = 0;
        Dict.DictState<?, ?> currentState;
        do {
            currentState = dict.state.get();
            if (currentState.rehashIndex == -1) break;
            dict.rehashStep();
            rehashSteps++;
            Thread.sleep(1);
        } while (true);
        if (rehashSteps > 0) {
            System.out.println("ensureRehashComplete: Rehash completed with " + rehashSteps + " steps.");
        }
    }
}
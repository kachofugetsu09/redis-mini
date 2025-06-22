package site.hnfy258.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class DictCowTest {

    private Dict<String, String> dict;
    private static final int INITIAL_KEY_COUNT = 50; // 初始键的数量
    private static final String INITIAL_KEY_PREFIX = "initialKey_";
    private static final String INITIAL_VALUE_PREFIX = "initialValue_";

    // 写线程的配置
    private static final int WRITER_THREADS = 1; // 单个写线程
    private static final int KEYS_PER_WRITER_THREAD = 5000; // 每个写线程写入的键数量
    private static final String WRITER_KEY_PREFIX = "writerKey_";
    private static final String WRITER_VALUE_PREFIX = "writerValue_";

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        // 预先写入初始键
        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            dict.put(INITIAL_KEY_PREFIX + i, INITIAL_VALUE_PREFIX + i);
        }
    }

    @Test
    void testSnapshotConsistencyDuringConcurrentWrite() throws InterruptedException, ExecutionException {
        System.out.println("--- 测试开始：快照一致性与并发写入 ---");
        System.out.println("初始 Dict 大小: " + dict.size());

        // 使用 CountDownLatch 协调读写线程的启动和结束
        CountDownLatch writerStarted = new CountDownLatch(1); // 写入线程已启动
        CountDownLatch snapshotTaken = new CountDownLatch(1); // 快照已获取
        CountDownLatch writerFinished = new CountDownLatch(WRITER_THREADS); // 写入线程已完成

        // 用于保存快照结果
        final Map<String, String>[] snapshotHolder = new Map[1];

        // 1. 启动快照读取线程
        ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();
        Future<Map<String, String>> snapshotFuture = snapshotExecutor.submit(() -> {
            try {
                // 等待写入线程启动信号，确保快照是在写入发生时获取
                writerStarted.await(); // 确保写线程已经开始写入，或者至少发出信号
                System.out.println("快照线程：收到写入启动信号，正在获取快照...");
                Map<String, String> snapshot = dict.createSafeSnapshot();
                System.out.println("快照线程：快照获取完成，快照大小: " + snapshot.size());
                snapshotHolder[0] = snapshot;
                snapshotTaken.countDown(); // 通知主线程快照已获取
                return snapshot;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("快照线程被中断。");
                return null;
            }
        });

        // 2. 启动写入线程
        ExecutorService writerExecutor = Executors.newFixedThreadPool(WRITER_THREADS);
        System.out.println("主线程：启动 " + WRITER_THREADS + " 个写入线程，每个写入 " + KEYS_PER_WRITER_THREAD + " 个键...");

        for (int i = 0; i < WRITER_THREADS; i++) {
            final int threadId = i;
            writerExecutor.submit(() -> {
                try {
                    writerStarted.countDown(); // 通知快照线程写入已启动
                    // 持续写入大量数据，以保证在快照线程获取快照时有足够的写操作和可能的 rehash 发生
                    for (int j = 0; j < KEYS_PER_WRITER_THREAD; j++) {
                        String key = WRITER_KEY_PREFIX + threadId + "_" + j;
                        String value = WRITER_VALUE_PREFIX + threadId + "_" + j;
                        dict.put(key, value);
                        // 适度引入延迟，模拟真实业务场景，并增加 rehash 触发的可能性
                        // if (j % 100 == 0) Thread.sleep(1); // 可以尝试打开此行以增加并发窗口
                    }
                } finally {
                    writerFinished.countDown(); // 通知主线程写入已完成
                }
            });
        }

        // 3. 等待快照获取完成，并继续等待写入完成
        System.out.println("主线程：等待快照获取...");
        snapshotTaken.await(10, TimeUnit.SECONDS); // 等待快照线程完成快照获取
        Map<String, String> concurrentSnapshot = snapshotHolder[0];
        assertNotNull(concurrentSnapshot, "快照线程应成功获取快照");

        System.out.println("主线程：快照已获取。等待写入线程完成...");
        writerFinished.await(10, TimeUnit.SECONDS); // 等待写入线程完成
        System.out.println("主线程：所有写入任务完成。");

        // 关闭线程池
        writerExecutor.shutdown();
        snapshotExecutor.shutdown();
        writerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        snapshotExecutor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("当前 Dict 最终大小: " + dict.size());

        // 4. 验证快照内容是否一致 (快照应包含初始数据，以及快照点之前写入的并发数据)
        System.out.println("--- 验证快照内容 ---");
        // 快照中至少应包含所有初始键
        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            String key = INITIAL_KEY_PREFIX + i;
            String value = INITIAL_VALUE_PREFIX + i;
            assertTrue(concurrentSnapshot.containsKey(key), "快照应包含初始键: " + key);
            assertEquals(value, concurrentSnapshot.get(key), "快照中初始键的值应正确: " + value);
        }
        System.out.println("快照内容验证成功：所有初始键都在快照中，且值正确。");

        // 验证快照中包含了多少并发写入的键
        long writerKeysInSnapshot = concurrentSnapshot.keySet().stream()
                .filter(k -> k.startsWith(WRITER_KEY_PREFIX))
                .count();

        // 这是一个关键的断言：快照的大小必须小于等于理论上的最终总数
        // 并且快照应该是一个内部一致的视图。
        // 由于快照是在写入过程中获取的，它可能会包含一部分写入的键，但不会包含全部。
        // 它的大小应该是 (初始键数量 + 在快照瞬间已完成写入的并发键数量)。
        // 无法精确断言快照中应该有多少个writerKey，因为它取决于时序。
        // 但我们可以断言：快照大小至少等于初始键数量，且小于或等于最终总数。
        assertTrue(concurrentSnapshot.size() >= INITIAL_KEY_COUNT,
                "快照大小至少应包含初始键数量");
        assertTrue(concurrentSnapshot.size() <= (INITIAL_KEY_COUNT + (WRITER_THREADS * KEYS_PER_WRITER_THREAD)),
                "快照大小不应超过最终总键数量");

        System.out.println("快照中包含 " + writerKeysInSnapshot + " 个并发写入的键（在快照点可见）。");
        System.out.println("快照内容在并发写入期间保持一致，测试通过！"); // 如果到这里，说明没有丢失数据

        // 5. 验证 Dict 本身包含了所有写入的键
        System.out.println("--- 验证 Dict 最终内容 ---");
        Set<String> allKeysInDict = dict.keySet();
        assertEquals(INITIAL_KEY_COUNT + (WRITER_THREADS * KEYS_PER_WRITER_THREAD), allKeysInDict.size(),
                "Dict 最终大小应包含所有初始键和写入键");

        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            assertTrue(allKeysInDict.contains(INITIAL_KEY_PREFIX + i), "Dict 最终应包含初始键: " + INITIAL_KEY_PREFIX + i);
        }
        for (int i = 0; i < WRITER_THREADS; i++) {
            for (int j = 0; j < KEYS_PER_WRITER_THREAD; j++) {
                assertTrue(allKeysInDict.contains(WRITER_KEY_PREFIX + i + "_" + j), "Dict 最终应包含写入键: " + WRITER_KEY_PREFIX + i + "_" + j);
            }
        }
        System.out.println("Dict 最终内容验证成功，包含所有期望的键。");
        System.out.println("--- 测试结束 ---");
    }

    // 可以添加更多重复测试来增加测试的健壮性
    @RepeatedTest(value = 5, name = "{displayName} {currentRepetition}/{totalRepetitions}")
    void repeatedSnapshotConsistencyTest(RepetitionInfo repetitionInfo) throws InterruptedException, ExecutionException {
        System.out.println("\n--- 重复测试 " + repetitionInfo.getCurrentRepetition() + " 开始 ---");
        setUp(); // 每次重复测试前重新初始化 Dict
        testSnapshotConsistencyDuringConcurrentWrite();
    }
}
package site.hnfy258.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class DictCowTest {

    private Dict<String, String> dict;
    private static final int INITIAL_KEY_COUNT = 1; // 初始键的数量
    private static final String INITIAL_KEY_PREFIX = "initialKey_";
    private static final String INITIAL_VALUE_PREFIX = "initialValue_";

    private static final int CONCURRENT_THREADS = 5; // 并发写入线程数
    private static final int KEYS_PER_THREAD = 100; // 每个线程写入的键数量
    private static final String CONCURRENT_KEY_PREFIX = "concurrentKey_";
    private static final String CONCURRENT_VALUE_PREFIX = "concurrentValue_";

    @BeforeEach
    void setUp() {
        dict = new Dict<>();
        // 预先写入一个或多个初始键
        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            dict.put(INITIAL_KEY_PREFIX + i, INITIAL_VALUE_PREFIX + i);
        }
    }

    @Test
    void testSnapshotConsistencyUnderHighConcurrency() throws InterruptedException {
        // 1. 获取初始快照
        System.out.println("--- 测试开始 ---");
        System.out.println("初始 Dict 大小: " + dict.size());
        Map<String, String> initialSnapshot = dict.createSafeSnapshot();
        System.out.println("获取初始快照，快照大小: " + initialSnapshot.size());

        // 验证初始快照的内容
        assertEquals(INITIAL_KEY_COUNT, initialSnapshot.size(), "初始快照大小应与初始写入的键数量一致");
        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            assertTrue(initialSnapshot.containsKey(INITIAL_KEY_PREFIX + i), "初始快照应包含初始键: " + INITIAL_KEY_PREFIX + i);
            assertEquals(INITIAL_VALUE_PREFIX + i, initialSnapshot.get(INITIAL_KEY_PREFIX + i), "初始快照中初始键的值应正确");
        }
        System.out.println("初始快照内容验证成功。");

        // 2. 启动并发写入任务
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
        System.out.println("开始启动 " + CONCURRENT_THREADS + " 个并发写入线程，每个线程写入 " + KEYS_PER_THREAD + " 个键...");

        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < KEYS_PER_THREAD; j++) {
                        String key = CONCURRENT_KEY_PREFIX + threadId + "_" + j;
                        String value = CONCURRENT_VALUE_PREFIX + threadId + "_" + j;
                        dict.put(key, value);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有并发写入任务完成
        latch.await(10, TimeUnit.SECONDS); // 最多等待10秒，防止死锁或无限等待
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS); // 等待线程池终止

        System.out.println("所有并发写入任务完成。");
        System.out.println("当前 Dict 实际大小 (可能包含并发写入的键): " + dict.size());

        // 3. 验证快照内容和大小是否保持不变
        System.out.println("--- 验证快照不变性 ---");
        assertEquals(INITIAL_KEY_COUNT, initialSnapshot.size(),
                "并发写入后，初始快照的大小应该保持不变，仍为 " + INITIAL_KEY_COUNT);
        System.out.println("并发写入后，初始快照的大小应该保持不变，仍为 " + INITIAL_KEY_COUNT);

        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            String key = INITIAL_KEY_PREFIX + i;
            String value = INITIAL_VALUE_PREFIX + i;
            assertTrue(initialSnapshot.containsKey(key), "并发写入后，初始快照应仍然包含键: " + key);
            initialSnapshot.forEach( (k, v) -> System.out.println("快照键: " + k + ", 值: " + v));
            assertEquals(value, initialSnapshot.get(key), "并发写入后，初始快照中初始键的值应保持正确: " + value);
        }

        // 验证快照中不包含任何并发写入的键
        boolean containsConcurrentKey = initialSnapshot.keySet().stream()
                .anyMatch(k -> k.startsWith(CONCURRENT_KEY_PREFIX));
        assertFalse(containsConcurrentKey, "初始快照不应包含任何并发写入的键");

        System.out.println("快照内容和大小在并发写入期间保持不变，测试通过！");

        // (可选) 验证 Dict 本身包含了所有写入的键
        Set<String> allKeysInDict = dict.keySet();
        assertEquals(INITIAL_KEY_COUNT + (CONCURRENT_THREADS * KEYS_PER_THREAD), allKeysInDict.size(),
                "Dict 实际大小应包含所有初始键和并发写入的键");
        for (int i = 0; i < INITIAL_KEY_COUNT; i++) {
            assertTrue(allKeysInDict.contains(INITIAL_KEY_PREFIX + i));
        }
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            for (int j = 0; j < KEYS_PER_THREAD; j++) {
                assertTrue(allKeysInDict.contains(CONCURRENT_KEY_PREFIX + i + "_" + j));
            }
        }
        System.out.println("Dict 实际内容验证成功。");
    }

    // 可以添加更多重复测试来增加测试的健壮性
    @RepeatedTest(value = 5, name = "{displayName} {currentRepetition}/{totalRepetitions}")
    void repeatedSnapshotConsistencyTest(RepetitionInfo repetitionInfo) throws InterruptedException {
        System.out.println("\n--- 重复测试 " + repetitionInfo.getCurrentRepetition() + " 开始 ---");
        setUp(); // 每次重复测试前重新初始化 Dict
        testSnapshotConsistencyUnderHighConcurrency();
    }
}
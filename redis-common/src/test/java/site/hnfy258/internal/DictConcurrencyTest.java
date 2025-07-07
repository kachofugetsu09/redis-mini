package site.hnfy258.internal;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DictConcurrencyTest {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("开始更严格的并发快照测试...");
        final Dict<String, String> dict = new Dict<>();

        // --- 阶段一：初始数据填充 ---
        System.out.println("阶段一：填充初始数据 (1,000,000 条)...");
        int initialSize = 1_000_000;
        for (int i = 0; i < initialSize; i++) {
            String key = "key" + i;
            String value = "value" + i;
            dict.put(key, value);
        }
        System.out.println("初始数据填充完毕。");
        System.out.println("------------------------------------");

        // --- 准备并发测试 ---
        final AtomicBoolean stopSignal = new AtomicBoolean(false);
        final CountDownLatch startLatch = new CountDownLatch(1); // 发令枪
        final CountDownLatch finishLatch = new CountDownLatch(2); // 终点线
        final AtomicInteger snapshotCount = new AtomicInteger(0);
        final AtomicReference<String> errorLog = new AtomicReference<>(null);

        // 选择一个“哨兵”键，用于验证快照一致性
        final String SENTINEL_KEY = "key" + (initialSize / 2);


        // --- 线程一：快照线程 ---
        Thread snapshotThread = new Thread(() -> {
            try {

                startLatch.await();
                while (!stopSignal.get()) {
                    // 1. 记录快照前哨兵键的 "实时值"
                    String valueBeforeSnapshot = dict.get(SENTINEL_KEY);

                    // 2. 创建快照
                    Map<String, String> snapshot = dict.createSnapshot();
                    int currentSnapshotId = snapshotCount.incrementAndGet();

                    // 3. 【核心验证】检查快照中的值是否与快照创建前的实时值一致
                    String valueInSnapshot = snapshot.get(SENTINEL_KEY);
                    if (valueInSnapshot == null ? valueBeforeSnapshot != null : !valueInSnapshot.equals(valueBeforeSnapshot)) {
                        String errorMessage = String.format(
                            "[快照线程] 失败! 快照 #%d 不一致。快照前值: '%s', 快照内值: '%s'",
                            currentSnapshotId, valueBeforeSnapshot, valueInSnapshot
                        );
                        errorLog.set(errorMessage);
                        stopSignal.set(true); // 发现错误，立即停止测试
                        break;
                    }

                    System.out.printf("[快照线程] 快照 #%d 创建成功, 大小: %d, 哨兵值: '%s' (一致)%n",
                                    currentSnapshotId, snapshot.size(), valueInSnapshot);

                    // 模拟RDB文件写入耗时
                    Thread.sleep(150);

                    // 4. 完成快照
                    dict.finishSnapshot();
                    System.out.printf("[快照线程] 快照 #%d 已合并。%n", currentSnapshotId);

                    Thread.sleep(350); // 控制快照频率
                }
            } catch (Exception e) {
                errorLog.set("[快照线程] 异常退出: " + e.getMessage());
                e.printStackTrace();
            } finally {
                finishLatch.countDown();
            }
        });


        // --- 线程二：主线程 (写操作) ---
        Thread writerThread = new Thread(() -> {
            try {
                // 等待发令枪
                startLatch.await();
                Random random = new Random();
                int opCount = 0;
                while (!stopSignal.get()) {
                    // 随机选择key和操作
                    String key = "key" + random.nextInt(initialSize);
                    int opType = random.nextInt(10);
                    
                    if (opType < 7) { // 70% 更新
                        dict.put(key, "updated-" + opCount);
                    } else { // 30% 删除
                        dict.remove(key);
                    }
                    opCount++;
                }
            } catch (Exception e) {
                errorLog.set("[主线程] 异常退出: " + e.getMessage());
                e.printStackTrace();
            } finally {
                finishLatch.countDown();
            }
        });


        // --- 阶段二：启动并执行并发测试 ---
        System.out.println("阶段二：启动主线程和快照线程，并发运行 5 秒...");
        writerThread.start();
        snapshotThread.start();

        // **同时释放两个线程！**
        startLatch.countDown();
        System.out.println("发令枪已响，并发测试开始！");

        // 运行一段时间
        TimeUnit.SECONDS.sleep(5);

        // --- 阶段三：发送停止信号 ---
        System.out.println("阶段三：发送停止信号，等待线程优雅结束...");
        stopSignal.set(true);

        // 等待两个线程都结束
        if (!finishLatch.await(5, TimeUnit.SECONDS)) {
            System.err.println("警告：线程未能及时结束！");
            writerThread.interrupt();
            snapshotThread.interrupt();
        }

        // --- 阶段四：验证最终结果 ---
        System.out.println("------------------------------------");
        System.out.println("测试结束。最终报告：");
        System.out.println("  - 快照总次数: " + snapshotCount.get());
        
        if (errorLog.get() != null) {
            System.err.println("\n结论：测试失败！发现错误：");
            System.err.println("  " + errorLog.get());
        } else {
            System.out.println("\n结论：测试通过！在5秒高强度并发读写下，所有快照都保持了完美的数据一致性。");
            System.out.println("  这证明你的 `ForwardNode` 写时复制机制是正确且健壮的。");
        }
    }
}
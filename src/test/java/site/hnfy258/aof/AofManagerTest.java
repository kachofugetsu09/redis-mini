package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class AofManagerTest {
    
    private static final String TEST_FILE = "test_aof_manager.aof";
    private File aofFile;
    
    @Before
    public void setUp() throws IOException {
        aofFile = new File(TEST_FILE);
        // 确保测试前文件不存在
        if (aofFile.exists()) {
            aofFile.delete();
        }
    }
    
    @After
    public void tearDown() throws IOException {
        // 测试后删除测试文件
        if (aofFile.exists()) {
            aofFile.delete();
        }
    }
    
    /**
     * 测试AOF文件预分配功能
     */
    @Test
    public void testPreallocation() throws Exception {
        // 创建一个使用预分配的AOF管理器
        AofManager aofManager = new AofManager(TEST_FILE, false, true, 100);
        
        // 检查文件是否已创建
        assertTrue("AOF文件应该被创建", aofFile.exists());

        // 检查文件是否被预分配空间 - 应该接近16MB
        long fileSize = aofFile.length();
        System.out.println("预分配的AOF文件大小: " + fileSize + " 字节");
        assertTrue("AOF文件应该被预分配空间", fileSize >= 4*1024*1024);
        
        // 关闭AOF管理器
        aofManager.close();
    }
    
    /**
     * 测试不同刷盘间隔对性能的影响
     */
    @Test
    public void testDifferentFlushIntervals() throws Exception {
        // 测试不同的刷盘间隔
        int[] flushIntervals = {0, 100, 1000}; // 0表示不自动刷盘，100ms和1000ms
        int commandCount = 1000;
        
        for (int interval : flushIntervals) {
            // 创建AOF管理器，使用不同的刷盘间隔
            AofManager aofManager = new AofManager(TEST_FILE, false, true, interval);
            
            // 创建并执行命令
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < commandCount; i++) {
                RespArray setCommand = createSetCommand("key" + i, "value" + i);
                aofManager.append(setCommand);
            }
            
            // 确保所有数据都写入磁盘
            aofManager.flush();
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            System.out.println("刷盘间隔 " + interval + "ms, 写入 " + commandCount + " 个命令耗时: " + duration + "ms");
            System.out.println("每秒处理命令数: " + (int)((double)commandCount / (duration / 1000.0)));
            
            // 关闭AOF管理器并删除文件，准备下一轮测试
            aofManager.close();
            aofFile.delete();
        }
    }
    
    /**
     * 测试高并发下的写入性能和正确性
     */
    @Test
    public void testConcurrentWritePerformance() throws Exception {
        // 创建AOF管理器
        AofManager aofManager = new AofManager(TEST_FILE, false, true, 100);
        
        // 并发线程数
        int threadCount = 10;
        // 每个线程的写入次数
        int commandsPerThread = 1000;
        
        // 总命令计数
        AtomicInteger commandCounter = new AtomicInteger(0);
        
        // 创建栅栏，确保所有线程同时开始
        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        // 创建倒计时锁，等待所有线程完成
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        // 提交任务到线程池
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    // 等待所有线程准备就绪
                    barrier.await();
                    
                    // 执行写入操作
                    for (int j = 0; j < commandsPerThread; j++) {
                        // 创建命令：SET thread{threadId}_key{j} value{j}
                        RespArray setCommand = createSetCommand(
                                "thread" + threadId + "_key" + j, 
                                "value" + j);
                        
                        // 写入AOF
                        aofManager.append(setCommand);
                        
                        // 增加命令计数
                        commandCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 通知主线程该线程已完成
                    latch.countDown();
                }
            });
        }
        
        // 开始计时
        barrier.await(); // 让所有线程同时开始
        long startTime = System.currentTimeMillis();
        
        // 等待所有线程完成
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        
        // 确保所有数据都写入磁盘
        aofManager.flush();
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // 关闭线程池
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        // 验证是否所有线程都完成了
        assertTrue("所有线程应该在规定时间内完成", completed);
        
        // 验证命令数量
        assertEquals("应该写入正确数量的命令", threadCount * commandsPerThread, commandCounter.get());
        
        // 输出性能数据
        System.out.println("并发测试：" + threadCount + "个线程，每个线程" + commandsPerThread + "个命令");
        System.out.println("总共 " + commandCounter.get() + " 个命令，耗时: " + duration + "ms");
        System.out.println("每秒处理命令数: " + (int)((double)commandCounter.get() / (duration / 1000.0)));
        System.out.println("AOF文件大小: " + aofFile.length() + " 字节");
        
        // 关闭AOF管理器
        aofManager.close();
    }
    
    /**
     * 测试背压机制
     */
    @Test
    public void testBackpressureMechanism() throws Exception {
        // 创建AOF管理器，减小刷盘间隔以便更快触发背压
        AofManager aofManager = new AofManager(TEST_FILE, false, true, 500);
        
        // 创建一个线程来快速写入大量命令，以触发背压机制
        final int commandCount = 10000;
        
        // 记录每个命令的写入时间
        long[] appendTimes = new long[commandCount];
        
        // 单线程快速写入
        Thread writer = new Thread(() -> {
            try {
                for (int i = 0; i < commandCount; i++) {
                    long start = System.nanoTime();
                    // 写入一个较大的命令，更容易触发背压
                    StringBuilder value = new StringBuilder();
                    // 随机生成1-10KB的值
                    int valueSize = ThreadLocalRandom.current().nextInt(1, 10) * 1024;
                    for (int j = 0; j < valueSize; j++) {
                        value.append('X');
                    }
                    
                    RespArray setCommand = createSetCommand("backpressure_key" + i, value.toString());
                    aofManager.append(setCommand);
                    
                    long end = System.nanoTime();
                    appendTimes[i] = end - start;
                    
                    // 如果是前1000个命令，快速写入；之后稍微减缓速度，以便观察背压效果
                    if (i >= 1000) {
                        Thread.sleep(1); // 短暂暂停
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        writer.start();
        writer.join(); // 等待写入线程完成
        
        // 确保所有数据都写入磁盘
        aofManager.flush();
        Thread.sleep(500);

        // 分析写入时间模式
        long totalTime = 0;
        long maxTime = 0;
        long minTime = Long.MAX_VALUE;
        
        // 计算前中后阶段的平均写入时间，用于观察背压效果
        long firstStageAvg = 0;
        long middleStageAvg = 0;
        long lastStageAvg = 0;
        
        for (int i = 0; i < commandCount; i++) {
            totalTime += appendTimes[i];
            maxTime = Math.max(maxTime, appendTimes[i]);
            minTime = Math.min(minTime, appendTimes[i]);
            
            // 计算不同阶段的平均时间
            if (i < commandCount / 3) {
                firstStageAvg += appendTimes[i];
            } else if (i < commandCount * 2 / 3) {
                middleStageAvg += appendTimes[i];
            } else {
                lastStageAvg += appendTimes[i];
            }
        }
        
        firstStageAvg /= (commandCount / 3);
        middleStageAvg /= (commandCount / 3);
        lastStageAvg /= (commandCount - 2 * (commandCount / 3));
        
        System.out.println("背压测试结果:");
        System.out.println("总写入命令数: " + commandCount);
        System.out.println("平均每个命令写入时间: " + (totalTime / commandCount) + " ns");
        System.out.println("最长写入时间: " + maxTime + " ns");
        System.out.println("最短写入时间: " + minTime + " ns");
        System.out.println("前1/3阶段平均写入时间: " + firstStageAvg + " ns");
        System.out.println("中1/3阶段平均写入时间: " + middleStageAvg + " ns");
        System.out.println("后1/3阶段平均写入时间: " + lastStageAvg + " ns");
        
        // 背压效果体现：后期写入时间应该明显长于前期
        System.out.println("背压效果倍数: " + ((double)lastStageAvg / firstStageAvg));
        
        // 关闭AOF管理器
        aofManager.close();
    }
    
    /**
     * 测试紧急情况处理（队列积压过多）
     */
    @Test
    public void testEmergencySituation() throws Exception {
        // 创建AOF管理器
        AofManager aofManager = new AofManager(TEST_FILE, false, true, 1000);
        
        // 模拟紧急情况：快速写入大量大命令
        final int largeCommandCount = 500;
        final int smallCommandCount = 1000;
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        // 一个线程写入大命令
        executor.submit(() -> {
            try {
                for (int i = 0; i < largeCommandCount; i++) {
                    // 生成约500KB的大命令
                    StringBuilder value = new StringBuilder();
                    for (int j = 0; j < 500 * 1024; j++) {
                        value.append('X');
                    }
                    
                    RespArray setCommand = createSetCommand("emergency_large_key" + i, value.toString());
                    aofManager.append(setCommand);
                    
                    // 短暂暂停，让另一个线程有机会写入
                    if (i % 10 == 0) {
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 另一个线程写入小命令
        executor.submit(() -> {
            try {
                for (int i = 0; i < smallCommandCount; i++) {
                    RespArray setCommand = createSetCommand("emergency_small_key" + i, "small_value" + i);
                    aofManager.append(setCommand);
                    
                    // 快速写入，不暂停
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 关闭线程池并等待任务完成
        executor.shutdown();
        executor.awaitTermination(120, TimeUnit.SECONDS);
        
        // 确保所有数据都写入磁盘
        aofManager.flush();
        
        // 检查文件是否完整
        assertTrue("AOF文件应该存在", aofFile.exists());
        System.out.println("紧急情况测试：AOF文件大小: " + aofFile.length() + " 字节");
        
        // 关闭AOF管理器
        aofManager.close();
    }
    
    /**
     * 创建SET命令的RESP数组
     */
    private RespArray createSetCommand(String key, String value) {
        Resp[] array = new Resp[3];
        array[0] = new BulkString("SET".getBytes(StandardCharsets.UTF_8));
        array[1] = new BulkString(key.getBytes(StandardCharsets.UTF_8));
        array[2] = new BulkString(value.getBytes(StandardCharsets.UTF_8));
        return new RespArray(array);
    }
} 
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AofWriterTest {
    
    private static final String TEST_FILE = "test_aof.aof";
    private AofManager aofManager;
    private File aofFile;
    
    @Before
    public void setUp() throws IOException {
        aofFile = new File(TEST_FILE);
        // 确保测试前文件不存在
        if (aofFile.exists()) {
            aofFile.delete();
        }
        // 创建新的AOF管理器
        aofManager = new AofManager(TEST_FILE, false, true, 100);
    }
    
    @After
    public void tearDown() throws Exception {
        // 测试后关闭AOF管理器并删除测试文件
        if (aofManager != null) {
            aofManager.close();
        }
        if (aofFile.exists()) {
            aofFile.delete();
        }
    }
    
    /**
     * 测试基本的AOF写入功能
     */
    @Test
    public void testBasicWrite() throws Exception {
        // 创建一个简单的SET命令: SET key1 value1
        RespArray setCommand = createSetCommand("key1", "value1");
        
        // 写入AOF文件
        aofManager.append(setCommand);
        
        // 强制刷盘确保写入
        aofManager.flush();
        Thread.sleep(500);
        
        // 验证文件已创建且有内容
        assertTrue("AOF文件应该被创建", aofFile.exists());
        assertTrue("AOF文件应该有内容", aofFile.length() > 0);
        
        // 读取AOF文件内容进行验证
        String fileContent = readFileContent(aofFile);
        // 检查文件内容是否包含我们写入的命令的基本特征
        assertTrue("文件内容应该包含SET命令", fileContent.contains("SET"));
        assertTrue("文件内容应该包含key1", fileContent.contains("key1"));
        assertTrue("文件内容应该包含value1", fileContent.contains("value1"));
    }
    
    /**
     * 测试批量写入
     */
    @Test
    public void testBatchWrite() throws Exception {
        // 创建多个SET命令
        final int commandCount = 1000;
        CountDownLatch latch = new CountDownLatch(commandCount);
        
        // 并发写入多个命令
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < commandCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    RespArray setCommand = createSetCommand("key" + index, "value" + index);
                    aofManager.append(setCommand);
                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        
        // 等待所有命令写入完成
        assertTrue("所有命令应该在30秒内写入完成", latch.await(30, TimeUnit.SECONDS));
        
        // 关闭线程池并等待所有任务完成
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        // 强制刷盘确保写入
        aofManager.flush();
        
        // 验证文件大小合理
        assertTrue("AOF文件应该有合理的大小", aofFile.length() > 0);
        System.out.println("批量写入后AOF文件大小: " + aofFile.length() + " 字节");
    }
    
    /**
     * 测试大命令处理
     */
    @Test
    public void testLargeCommandWrite() throws Exception {
        // 创建一个大的SET命令，值为1MB大小
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 1024 * 1024 / 10; i++) {
            largeValue.append("0123456789"); // 每次添加10个字符
        }
        
        RespArray largeSetCommand = createSetCommand("bigKey", largeValue.toString());
        
        // 写入AOF文件
        long startTime = System.currentTimeMillis();
        aofManager.append(largeSetCommand);
        long endTime = System.currentTimeMillis();
        
        // 强制刷盘确保写入
        aofManager.flush();
        
        // 验证文件大小
        assertTrue("AOF文件应该包含大命令", aofFile.length() > 1024 * 1024);
        System.out.println("大命令写入耗时: " + (endTime - startTime) + "ms");
        System.out.println("大命令写入后AOF文件大小: " + aofFile.length() + " 字节");
    }
    
    /**
     * 测试高频小命令
     */
    @Test
    public void testHighFrequencySmallCommands() throws Exception {
        // 创建大量小命令并快速写入
        final int commandCount = 10000;
        CountDownLatch latch = new CountDownLatch(commandCount);
        
        long startTime = System.currentTimeMillis();
        
        // 创建专用线程进行高频写入
        Thread writerThread = new Thread(() -> {
            try {
                for (int i = 0; i < commandCount; i++) {
                    RespArray incrCommand = createIncrCommand("counter" + (i % 100));
                    aofManager.append(incrCommand);
                    latch.countDown();
                    
                    // 不要添加Thread.sleep，保持高频写入
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        
        writerThread.start();
        
        // 等待所有命令写入完成
        assertTrue("所有命令应该在30秒内写入完成", latch.await(30, TimeUnit.SECONDS));
        writerThread.join(5000);
        
        long endTime = System.currentTimeMillis();
        
        // 强制刷盘确保写入
        aofManager.flush();
        
        // 验证写入性能
        System.out.println("高频小命令写入耗时: " + (endTime - startTime) + "ms");
        System.out.println("平均每个命令耗时: " + (float)(endTime - startTime) / commandCount + "ms");
        System.out.println("高频小命令写入后AOF文件大小: " + aofFile.length() + " 字节");
    }
    
    /**
     * 测试混合负载（大命令和小命令混合）
     */
    @Test
    public void testMixedWorkload() throws Exception {
        // 创建多线程模拟混合负载
        final int smallCommandCount = 1000;
        final int largeCommandCount = 10;
        CountDownLatch latch = new CountDownLatch(smallCommandCount + largeCommandCount);
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        
        // 提交小命令任务
        for (int i = 0; i < smallCommandCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    RespArray setCommand = createSetCommand("smallKey" + index, "smallValue" + index);
                    aofManager.append(setCommand);
                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        
        // 提交大命令任务
        for (int i = 0; i < largeCommandCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    // 创建100KB大小的命令
                    StringBuilder largeValue = new StringBuilder();
                    for (int j = 0; j < 100 * 1024 / 10; j++) {
                        largeValue.append("0123456789");
                    }
                    
                    RespArray largeSetCommand = createSetCommand("largeKey" + index, largeValue.toString());
                    aofManager.append(largeSetCommand);
                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        
        // 等待所有命令完成
        assertTrue("所有命令应该在60秒内写入完成", latch.await(60, TimeUnit.SECONDS));
        
        // 关闭线程池
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        // 强制刷盘确保写入
        aofManager.flush();
        
        System.out.println("混合负载测试AOF文件大小: " + aofFile.length() + " 字节");
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
    
    /**
     * 创建INCR命令的RESP数组
     */
    private RespArray createIncrCommand(String key) {
        Resp[] array = new Resp[2];
        array[0] = new BulkString("INCR".getBytes(StandardCharsets.UTF_8));
        array[1] = new BulkString(key.getBytes(StandardCharsets.UTF_8));
        return new RespArray(array);
    }
    
    /**
     * 读取文件内容为字符串（仅用于小文件验证）
     */
    private String readFileContent(File file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        byte[] bytes = new byte[(int) raf.length()];
        raf.readFully(bytes);
        raf.close();
        return new String(bytes, StandardCharsets.UTF_8);
    }
} 
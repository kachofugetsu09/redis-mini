package site.hnfy258.aof.writer;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.core.RedisCoreImpl;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Slf4j
public class AofWriterBufferTest {

    private File tempAofFile;
    private AofWriter aofWriter;
    private RedisCore mockRedisCore;

    @Before
    public void setUp() throws IOException {
        // 创建临时的测试AOF文件
        tempAofFile = File.createTempFile("test_aof_buffer", ".aof");

        // 创建模拟的RedisCore对象（不需要完整功能，只需要支持重写）
        mockRedisCore = new RedisCoreImpl(1, null);

        // 创建AofWriter实例
        aofWriter = new AofWriter(tempAofFile, false, 1000, null, mockRedisCore);

        log.info("测试文件创建在: {}", tempAofFile.getAbsolutePath());
    }

    @After
    public void tearDown() throws IOException {
        // 关闭AofWriter
        if (aofWriter != null) {
            aofWriter.close();
        }

        // 删除测试文件
        if (tempAofFile != null && tempAofFile.exists()) {
            tempAofFile.delete();
        }

        // 查找并删除任何临时的重写文件
        String parentDir = tempAofFile.getParent();
        File[] files = new File(parentDir).listFiles((dir, name) -> name.startsWith("redis_aof_rewrite_"));
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
    }

    /**
     * 测试场景：在重写过程中，新的写命令是否被正确缓冲和应用
     */
    @Test
    public void testBufferDuringRewrite() throws Exception {
        // 1. 先写入一些初始数据到AOF文件
        writeSetCommand("initial_key_1", "initial_value_1");
        writeSetCommand("initial_key_2", "initial_value_2");

        // 模拟命令执行：将这些数据添加到Redis内存数据库中
        // 这样在重写时能从内存中读取到初始键
        RedisDB db = mockRedisCore.getDataBases()[0];
        db.put(new RedisBytes(("initial_key_1".getBytes())), new RedisString(new Sds("initial_value_1".getBytes())));
        db.put(new RedisBytes(("initial_key_2".getBytes())), new RedisString(new Sds("initial_value_2".getBytes())));

        // 确保刷盘
        aofWriter.flush();

        // 2. 启动一个倒计时锁，用于协调测试线程和重写线程
        CountDownLatch rewriteStartedLatch = new CountDownLatch(1);
        CountDownLatch commandsWrittenLatch = new CountDownLatch(1);

        // 3. 在单独的线程中启动重写过程
        Thread rewriteThread = new Thread(() -> {
            try {
                // 开始重写
                aofWriter.bgrewrite();

                // 通知主测试线程重写已开始
                rewriteStartedLatch.countDown();

                // 等待主测试线程完成写入命令
                boolean commandsWritten = commandsWrittenLatch.await(5, TimeUnit.SECONDS);
                if (!commandsWritten) {
                    log.error("等待写入命令超时");
                }

                // 重写线程无需等待，主线程会验证结果
            } catch (Exception e) {
                log.error("重写线程发生错误", e);
            }
        });

        rewriteThread.start();

        // 4. 等待重写线程通知重写已开始
        boolean rewriteStarted = rewriteStartedLatch.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("重写应该已经开始", rewriteStarted);

        // 5. 重写开始后，写入一些新命令，这些命令应该被缓冲
        writeSetCommand("buffer_key_1", "buffer_value_1");
        writeSetCommand("buffer_key_2", "buffer_value_2");
        writeSetCommand("buffer_key_3", "buffer_value_3");

        // 同样添加到内存数据库
        db.put(new RedisBytes(("buffer_key_1".getBytes())), new RedisString(new Sds("buffer_value_1".getBytes())));
        db.put(new RedisBytes(("buffer_key_2".getBytes())), new RedisString(new Sds("buffer_value_2".getBytes())));
        db.put(new RedisBytes(("buffer_key_3".getBytes())), new RedisString(new Sds("buffer_value_3".getBytes())));

        // 确保刷盘
        aofWriter.flush();

        // 通知重写线程已写入命令
        commandsWrittenLatch.countDown();

        // 6. 等待重写完成（最多等待10秒）
        for (int i = 0; i < 100; i++) {
            if (!aofWriter.isRewriting()) {
                break;
            }
            Thread.sleep(100);
        }

        Assert.assertFalse("重写应该已经完成", aofWriter.isRewriting());

        // 7. 读取文件内容并验证缓冲的命令是否正确写入
        List<String> fileLines = readFileLines(tempAofFile);
        log.info("重写后的文件内容: {}", fileLines);

        boolean foundInitialKeys = false;
        boolean foundBufferKeys = false;

        for (String line : fileLines) {
            if (line.contains("initial_key_1") || line.contains("initial_key_2")) {
                foundInitialKeys = true;
            }

            if (line.contains("buffer_key_1") || line.contains("buffer_key_2") || line.contains("buffer_key_3")) {
                foundBufferKeys = true;
            }
        }

        Assert.assertTrue("应该找到初始键", foundInitialKeys);
        Assert.assertTrue("应该找到缓冲区键", foundBufferKeys);

        log.info("测试结束 - 缓冲区功能工作正常");
    }

    private void writeSetCommand(String key, String value) throws IOException {
        // 创建标准的Redis SET命令
        List<Resp> commandParts = new ArrayList<>();
        commandParts.add(new BulkString("SET".getBytes(StandardCharsets.UTF_8)));
        commandParts.add(new BulkString(key.getBytes(StandardCharsets.UTF_8)));
        commandParts.add(new BulkString(value.getBytes(StandardCharsets.UTF_8)));

        RespArray setCommand = new RespArray(commandParts.toArray(new Resp[0]));

        // 转换成ByteBuffer
        ByteBuf buf = Unpooled.buffer();
        setCommand.encode(setCommand, buf);
        ByteBuffer buffer = buf.nioBuffer();

        // 写入到AOF文件
        aofWriter.write(buffer);

        // 释放ByteBuf，避免内存泄漏
        buf.release();
    }

    private List<String> readFileLines(File file) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}

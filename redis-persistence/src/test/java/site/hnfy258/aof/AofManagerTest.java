package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("AOF管理器测试")
class AofManagerTest {

    @TempDir
    Path tempDir;

    @Mock
    private RedisCore redisCore;

    private AofManager aofManager;
    private String aofFilePath;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        aofFilePath = tempDir.resolve("test.aof").toString();
        aofManager = new AofManager(aofFilePath, redisCore);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (aofManager != null) {
            aofManager.close();
        }
    }

    @Test
    @DisplayName("测试追加字节数组")
    void testAppendBytes() throws Exception {
        // Given
        byte[] commandBytes = "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n".getBytes(StandardCharsets.UTF_8);

        // When
        aofManager.appendBytes(commandBytes);
        aofManager.flush();

        // Then
        assertTrue(new File(aofFilePath).exists());
        assertTrue(new File(aofFilePath).length() > 0);
    }

    @Test
    @DisplayName("测试追加RESP数组")
    void testAppendRespArray() throws Exception {
        // Given
        Resp[] commands = {
            new BulkString("SET".getBytes(StandardCharsets.UTF_8)),
            new BulkString("key".getBytes(StandardCharsets.UTF_8)),
            new BulkString("value".getBytes(StandardCharsets.UTF_8))
        };
        RespArray respArray = new RespArray(commands);

        // When
        aofManager.append(respArray);
        aofManager.flush();

        // Then
        assertTrue(new File(aofFilePath).exists());
        assertTrue(new File(aofFilePath).length() > 0);
    }

    @Test
    @DisplayName("测试空命令字节数组")
    void testAppendEmptyBytes() {
        // Given
        byte[] emptyBytes = new byte[0];

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> aofManager.appendBytes(emptyBytes));
    }

    @Test
    @DisplayName("测试null命令字节数组")
    void testAppendNullBytes() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> aofManager.appendBytes(null));
    }

    @Test
    @DisplayName("测试null RESP数组")
    void testAppendNullRespArray() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> aofManager.append(null));
    }

    @Test
    @DisplayName("测试刷新缓冲区")
    void testFlushBuffer() throws Exception {
        // Given
        byte[] commandBytes = "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n".getBytes(StandardCharsets.UTF_8);
        aofManager.appendBytes(commandBytes);

        // When
        aofManager.flushBuffer();

        // Then
        assertTrue(new File(aofFilePath).exists());
        assertTrue(new File(aofFilePath).length() > 0);
    }

    @Test
    @DisplayName("测试加载AOF文件")
    void testLoad() throws Exception {
        // Given
        byte[] commandBytes = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes(StandardCharsets.UTF_8);
        aofManager.appendBytes(commandBytes);
        aofManager.flush();

        // When
        aofManager.load();

        // Then
        // 验证文件被加载
        assertTrue(new File(aofFilePath).exists());
    }

    @Test
    @DisplayName("测试关闭AOF管理器")
    void testClose() throws Exception {
        // Given
        byte[] commandBytes = "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n".getBytes(StandardCharsets.UTF_8);
        aofManager.appendBytes(commandBytes);

        // When
        aofManager.close();

        // Then
        assertDoesNotThrow(() -> aofManager.close()); // 重复关闭不应抛出异常
    }
} 
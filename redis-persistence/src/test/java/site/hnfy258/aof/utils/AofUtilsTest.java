package site.hnfy258.aof.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Sds;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AOF工具类测试")
class AofUtilsTest {

    @TempDir
    Path tempDir;

    private File aofFile;
    private FileChannel channel;
    private RandomAccessFile raf;

    @BeforeEach
    void setUp() throws Exception {
        aofFile = tempDir.resolve("test.aof").toFile();
        raf = new RandomAccessFile(aofFile, "rw");
        channel = raf.getChannel();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.close();
        }
        if (raf != null) {
            raf.close();
        }
    }

    @Test
    @DisplayName("测试写入String类型数据")
    void testWriteStringToAof() throws Exception {
        // Given
        RedisBytes key = RedisBytes.fromString("test-key");
        RedisString value = new RedisString(Sds.create("test-value".getBytes()));

        // When
        AofUtils.writeDataToAof(key, value, channel);
        channel.force(true);

        // Then
        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0);
    }

    @Test
    @DisplayName("测试写入List类型数据")
    void testWriteListToAof() throws Exception {
        // Given
        RedisBytes key = RedisBytes.fromString("test-list");
        RedisList value = new RedisList();
        value.getList().add(RedisBytes.fromString("item1"));
        value.getList().add(RedisBytes.fromString("item2"));

        // When
        AofUtils.writeDataToAof(key, value, channel);
        channel.force(true);

        // Then
        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0);
    }

    @Test
    @DisplayName("测试写入Set类型数据")
    void testWriteSetToAof() throws Exception {
        // Given
        RedisBytes key = RedisBytes.fromString("test-set");
        RedisSet value = new RedisSet();
        List<RedisBytes> members = Arrays.asList(
            RedisBytes.fromString("member1"),
            RedisBytes.fromString("member2")
        );
        value.add(members);

        // When
        AofUtils.writeDataToAof(key, value, channel);
        channel.force(true);

        // Then
        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0);
    }

    @Test
    @DisplayName("测试写入Hash类型数据")
    void testWriteHashToAof() throws Exception {
        // Given
        RedisBytes key = RedisBytes.fromString("test-hash");
        RedisHash value = new RedisHash();
        value.put(RedisBytes.fromString("field1"), RedisBytes.fromString("value1"));
        value.put(RedisBytes.fromString("field2"), RedisBytes.fromString("value2"));

        // When
        AofUtils.writeDataToAof(key, value, channel);
        channel.force(true);

        // Then
        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0);
    }

    @Test
    @DisplayName("测试写入ZSet类型数据")
    void testWriteZSetToAof() throws Exception {
        // Given
        RedisBytes key = RedisBytes.fromString("test-zset");
        RedisZset value = new RedisZset();
        value.add(1.0, RedisBytes.fromString("member1"));
        value.add(2.0, RedisBytes.fromString("member2"));

        // When
        AofUtils.writeDataToAof(key, value, channel);
        channel.force(true);

        // Then
        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0);
    }

    @Test
    @DisplayName("测试写入null值")
    void testWriteNullValue() {
        // Given
        RedisBytes key = RedisBytes.fromString("test-key");

        // When & Then
        assertDoesNotThrow(() -> AofUtils.writeDataToAof(key, null, channel));
    }

    @Test
    @DisplayName("测试写入null键")
    void testWriteNullKey() {
        // Given
        RedisString value = new RedisString(Sds.create("test-value".getBytes()));

        // When & Then
        assertThrows(IllegalArgumentException.class, 
            () -> AofUtils.writeDataToAof(null, value, channel));
    }

    @Test
    @DisplayName("测试写入null通道")
    void testWriteNullChannel() {
        // Given
        RedisBytes key = RedisBytes.fromString("test-key");
        RedisString value = new RedisString(Sds.create("test-value".getBytes()));

        // When & Then
        assertThrows(IllegalArgumentException.class, 
            () -> AofUtils.writeDataToAof(key, value, null));
    }
} 
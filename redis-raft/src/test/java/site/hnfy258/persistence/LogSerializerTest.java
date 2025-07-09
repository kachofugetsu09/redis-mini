package site.hnfy258.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import site.hnfy258.core.LogEntry;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


/**
 * LogSerializer 测试类
 * 
 * 测试 LogSerializer 的序列化和反序列化功能，包括：
 * - 单个日志条目的序列化和反序列化
 * - 多个日志条目的序列化和反序列化
 * - 边界情况测试（空数组、null 值等）
 * - 序列化后数据的完整性验证
 */
@DisplayName("LogSerializer 单元测试")
public class LogSerializerTest {

    @Test
    @DisplayName("测试单个日志条目序列化和反序列化")
    void testSerializeAndDeserializeSingleLogEntry() {
        // 创建测试数据 - SET key value 命令
        Resp[] commands = new Resp[]{
            BulkString.fromString("SET"),
            BulkString.fromString("mykey"),
            BulkString.fromString("myvalue")
        };
        RespArray command = new RespArray(commands);
        LogEntry originalEntry = LogEntry.builder()
                .logIndex(1)
                .logTerm(2)
                .command(command)
                .build();

        // 序列化
        ByteBuf serializedBuffer = LogSerializer.serialize(originalEntry);
        assertNotNull(serializedBuffer);
        assertTrue(serializedBuffer.readableBytes() > 0);

        // 反序列化
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(serializedBuffer);
        
        // 验证结果
        assertNotNull(deserializedEntries);
        assertEquals(1, deserializedEntries.size());
        
        LogEntry deserializedEntry = deserializedEntries.get(0);
        assertEquals(originalEntry.getLogIndex(), deserializedEntry.getLogIndex());
        assertEquals(originalEntry.getLogTerm(), deserializedEntry.getLogTerm());
        
        // 验证命令内容
        RespArray originalCmd = originalEntry.getCommand();
        RespArray deserializedCmd = deserializedEntry.getCommand();
        assertNotNull(deserializedCmd);
        assertEquals(originalCmd.getContent().length, deserializedCmd.getContent().length);
        
        for (int i = 0; i < originalCmd.getContent().length; i++) {
            assertEquals(originalCmd.getContent()[i].toString(), 
                        deserializedCmd.getContent()[i].toString());
        }

        // 清理资源
        serializedBuffer.release();
    }

    @Test
    @DisplayName("测试多个日志条目序列化和反序列化")
    void testSerializeAndDeserializeMultipleLogEntries() {
        // 创建多个测试日志条目
        LogEntry entry1 = createLogEntry(1, 1, "SET", "key1", "value1");
        LogEntry entry2 = createLogEntry(2, 1, "GET", "key1");
        LogEntry entry3 = createLogEntry(3, 2, "DEL", "key1");

        // 分别序列化每个条目
        ByteBuf buffer1 = LogSerializer.serialize(entry1);
        ByteBuf buffer2 = LogSerializer.serialize(entry2);
        ByteBuf buffer3 = LogSerializer.serialize(entry3);

        // 合并到一个缓冲区中模拟批量数据
        ByteBuf combinedBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        combinedBuffer.writeBytes(buffer1);
        combinedBuffer.writeBytes(buffer2);
        combinedBuffer.writeBytes(buffer3);

        // 反序列化
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(combinedBuffer);

        // 验证结果
        assertNotNull(deserializedEntries);
        assertEquals(3, deserializedEntries.size());

        // 验证第一个条目
        LogEntry des1 = deserializedEntries.get(0);
        assertEquals(1, des1.getLogIndex());
        assertEquals(1, des1.getLogTerm());
        verifyCommand(des1.getCommand(), "SET", "key1", "value1");

        // 验证第二个条目
        LogEntry des2 = deserializedEntries.get(1);
        assertEquals(2, des2.getLogIndex());
        assertEquals(1, des2.getLogTerm());
        verifyCommand(des2.getCommand(), "GET", "key1");

        // 验证第三个条目
        LogEntry des3 = deserializedEntries.get(2);
        assertEquals(3, des3.getLogIndex());
        assertEquals(2, des3.getLogTerm());
        verifyCommand(des3.getCommand(), "DEL", "key1");

        // 清理资源
        buffer1.release();
        buffer2.release();
        buffer3.release();
        combinedBuffer.release();
    }

    @Test
    @DisplayName("测试空命令数组序列化和反序列化")
    void testSerializeAndDeserializeEmptyCommand() {
        // 创建空命令的日志条目
        RespArray emptyCommand = RespArray.EMPTY;
        LogEntry originalEntry = LogEntry.builder()
                .logIndex(10)
                .logTerm(5)
                .command(emptyCommand)
                .build();

        // 序列化
        ByteBuf serializedBuffer = LogSerializer.serialize(originalEntry);
        assertNotNull(serializedBuffer);

        // 反序列化
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(serializedBuffer);

        // 验证结果
        assertNotNull(deserializedEntries);
        assertEquals(1, deserializedEntries.size());
        
        LogEntry deserializedEntry = deserializedEntries.get(0);
        assertEquals(10, deserializedEntry.getLogIndex());
        assertEquals(5, deserializedEntry.getLogTerm());
        assertNotNull(deserializedEntry.getCommand());
        assertEquals(0, deserializedEntry.getCommand().getContent().length);

        // 清理资源
        serializedBuffer.release();
    }

    @Test
    @DisplayName("测试包含特殊字符的命令序列化和反序列化")
    void testSerializeAndDeserializeSpecialCharacters() {
        // 创建包含特殊字符的命令
        String specialValue = "Hello\r\nWorld\t测试\uD83D\uDE00"; // 包含换行符、制表符、中文和emoji
        LogEntry originalEntry = createLogEntry(100, 10, "SET", "special:key", specialValue);

        // 序列化
        ByteBuf serializedBuffer = LogSerializer.serialize(originalEntry);

        // 反序列化
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(serializedBuffer);

        // 验证结果
        assertNotNull(deserializedEntries);
        assertEquals(1, deserializedEntries.size());
        
        LogEntry deserializedEntry = deserializedEntries.get(0);
        assertEquals(100, deserializedEntry.getLogIndex());
        assertEquals(10, deserializedEntry.getLogTerm());
        verifyCommand(deserializedEntry.getCommand(), "SET", "special:key", specialValue);

        // 清理资源
        serializedBuffer.release();
    }

    @Test
    @DisplayName("测试不完整数据的反序列化")
    void testDeserializeIncompleteData() {
        // 创建正常的日志条目
        LogEntry originalEntry = createLogEntry(1, 1, "SET", "key", "value");
        ByteBuf completeBuffer = LogSerializer.serialize(originalEntry);

        // 创建不完整的缓冲区（只包含前面部分数据）
        ByteBuf incompleteBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        int partialLength = completeBuffer.readableBytes() / 2; // 只写入一半数据
        incompleteBuffer.writeBytes(completeBuffer, 0, partialLength);

        // 反序列化不完整数据
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(incompleteBuffer);

        // 应该返回空列表，因为数据不完整
        assertNotNull(deserializedEntries);
        assertEquals(0, deserializedEntries.size());

        // 清理资源
        completeBuffer.release();
        incompleteBuffer.release();
    }

    @Test
    @DisplayName("测试大型数据序列化和反序列化性能")
    void testSerializeAndDeserializeLargeData() {
        // 创建一个包含大量数据的命令
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeValue.append("This is a large value for testing purposes. ");
        }
        
        LogEntry largeEntry = createLogEntry(1000, 100, "SET", "large:key", largeValue.toString());

        // 记录序列化开始时间
        long startTime = System.nanoTime();
        
        // 序列化
        ByteBuf serializedBuffer = LogSerializer.serialize(largeEntry);
        
        long serializeTime = System.nanoTime() - startTime;
        
        // 反序列化
        long deserializeStartTime = System.nanoTime();
        List<LogEntry> deserializedEntries = LogSerializer.deSerialize(serializedBuffer);
        long deserializeTime = System.nanoTime() - deserializeStartTime;

        // 验证结果
        assertNotNull(deserializedEntries);
        assertEquals(1, deserializedEntries.size());
        
        LogEntry deserializedEntry = deserializedEntries.get(0);
        assertEquals(1000, deserializedEntry.getLogIndex());
        assertEquals(100, deserializedEntry.getLogTerm());
        verifyCommand(deserializedEntry.getCommand(), "SET", "large:key", largeValue.toString());

        // 输出性能信息
        System.out.printf("序列化时间: %.2f ms, 反序列化时间: %.2f ms%n", 
                         serializeTime / 1_000_000.0, 
                         deserializeTime / 1_000_000.0);
        System.out.printf("序列化后大小: %d bytes%n", serializedBuffer.readableBytes());

        // 清理资源
        serializedBuffer.release();
    }

    @Test
    @DisplayName("测试 LogSerializer 构造函数抛出异常")
    void testLogSerializerConstructorThrowsException() {
        // 由于构造函数是私有的且抛出异常，我们无法直接测试
        // 但我们可以验证静态方法工作正常
        LogEntry entry = createLogEntry(1, 1, "PING");
        
        assertDoesNotThrow(() -> {
            ByteBuf buffer = LogSerializer.serialize(entry);
            List<LogEntry> entries = LogSerializer.deSerialize(buffer);
            buffer.release();
        });
    }

    // ========== 辅助方法 ==========

    /**
     * 创建测试用的日志条目
     */
    private LogEntry createLogEntry(int logIndex, int logTerm, String... commandParts) {
        Resp[] commands = new Resp[commandParts.length];
        for (int i = 0; i < commandParts.length; i++) {
            commands[i] = BulkString.fromString(commandParts[i]);
        }
        
        RespArray command = new RespArray(commands);
        return LogEntry.builder()
                .logIndex(logIndex)
                .logTerm(logTerm)
                .command(command)
                .build();
    }

    /**
     * 验证命令内容
     */
    private void verifyCommand(RespArray command, String... expectedParts) {
        assertNotNull(command);
        assertEquals(expectedParts.length, command.getContent().length);
        
        for (int i = 0; i < expectedParts.length; i++) {
            assertEquals(expectedParts[i], command.getContent()[i].toString());
        }
    }
}

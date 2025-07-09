package site.hnfy258.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import site.hnfy258.core.LogEntry;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.List;

public class LogSerializer {

    private LogSerializer(){
        throw new UnsupportedOperationException();
    }

    public static ByteBuf serialize(LogEntry logEntry){
        // 1. logIndex
        int logIndex = logEntry.getLogIndex();
        // 2. LogTerm
        int logTerm = logEntry.getLogTerm();
        // 3. RespArrayCommand
        RespArray command = logEntry.getCommand();

        // 首先编码命令以获取其实际编码长度
        ByteBuf encodedCmdBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        if (command != null) { // 检查 command 是否为 null，因为你的 LogEntry 可以有 null command (例如 dummy entry)
            command.encode(command, encodedCmdBuffer); // 假设这会正确编码 RespArray
        }


        // 4. actualEncodedCmdLength
        int actualEncodedCmdLength = encodedCmdBuffer.readableBytes(); // 获取编码后命令的长度

        ByteBuf writeBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        writeBuffer.writeInt(logIndex);
        writeBuffer.writeInt(logTerm);
        writeBuffer.writeInt(actualEncodedCmdLength); // 写入实际的编码长度
        if (actualEncodedCmdLength > 0) {
            writeBuffer.writeBytes(encodedCmdBuffer);
        }

        encodedCmdBuffer.release();

        return writeBuffer;
    }

    public static List<LogEntry> deSerialize(ByteBuf buffer){
        List<LogEntry> logEntries = new ArrayList<>();
        while(buffer.readableBytes() >=12){ // 至少需要 12 字节来读取 logIndex, logTerm, actualEncodedCmdLength
            buffer.markReaderIndex(); // 标记当前读取位置，以便在数据不足时可以重置

            int logIndex = buffer.readInt();
            int logTerm = buffer.readInt();
            int actualEncodedCmdLength = buffer.readInt();

            if(buffer.readableBytes() < actualEncodedCmdLength){
                System.out.println("日志条目数据不完整，无法反序列化。预期的命令长度: " + actualEncodedCmdLength + ", 可读字节: " + buffer.readableBytes());
                buffer.resetReaderIndex(); // 重置读取位置
                break; // 跳出循环而不是继续处理
            }

            ByteBuf cmdBytes = buffer.readBytes(actualEncodedCmdLength);
            RespArray command = null;
            if (actualEncodedCmdLength > 0) {
                command = (RespArray)RespArray.decode(cmdBytes);
            }
            cmdBytes.release();

            LogEntry logEntry = new LogEntry().builder().
                    logIndex(logIndex).
                    logTerm(logTerm).
                    command(command).
                    build();
            logEntries.add(logEntry);
        }
        return logEntries;
    }

    /**
     * 序列化 LogEntry 列表
     * 格式: [list_size][logEntry1_byte_length][logEntry1_bytes][logEntry2_byte_length][logEntry2_bytes]...
     * @param logEntries 要序列化的 LogEntry 列表
     * @return 包含序列化数据的 ByteBuf
     */
    public static ByteBuf serializeList(List<LogEntry> logEntries) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeInt(logEntries.size()); // 写入列表大小

        for (LogEntry entry : logEntries) {
            ByteBuf entryBuffer = serialize(entry); // 序列化单个 LogEntry
            buffer.writeInt(entryBuffer.readableBytes()); // 写入当前 LogEntry 的字节长度
            buffer.writeBytes(entryBuffer); // 写入 LogEntry 的实际数据
            entryBuffer.release();
        }
        return buffer;
    }

    /**
     * 反序列化 LogEntry 列表
     * @param buffer 包含序列化数据的 ByteBuf
     * @return 反序列化后的 LogEntry 列表
     */
    public static List<LogEntry> deSerializeList(ByteBuf buffer) {
        List<LogEntry> logEntries = new ArrayList<>();
        if (buffer.readableBytes() < 4) { // 至少需要 4 字节来读取列表大小
            System.out.println("日志列表数据不完整，无法反序列化列表大小");
            return logEntries;
        }

        int listSize = buffer.readInt(); // 读取列表大小

        for (int i = 0; i < listSize; i++) {
            if (buffer.readableBytes() < 4) { // 至少需要 4 字节来读取每个 LogEntry 的长度
                System.out.println("日志列表数据不完整，无法读取第 " + (i+1) + " 个日志条目的长度");
                break;
            }
            buffer.markReaderIndex(); // 标记当前读取位置

            int entryLength = buffer.readInt(); // 读取当前 LogEntry 的字节长度

            if (buffer.readableBytes() < entryLength) {
                System.out.println("日志列表数据不完整，无法反序列化第 " + (i+1) + " 个日志条目。预期的条目长度: " + entryLength + ", 可读字节: " + buffer.readableBytes());
                buffer.resetReaderIndex(); // 重置读取位置
                break;
            }

            ByteBuf entryBytes = buffer.readBytes(entryLength); // 读取当前 LogEntry 的实际数据
            // 这里我们调用现有的 deSerialize 方法，但是要注意 deSerialize 是为单个 LogEntry设计的
            // 更好的做法是创建一个 deSerializeSingleEntry(ByteBuf buffer)
            // 为了兼容性，我们可以暂时这样处理，但理想情况下，deSerialize 应该只处理单个 LogEntry 的内容
            List<LogEntry> singleEntryList = deSerialize(entryBytes);
            if (!singleEntryList.isEmpty()) {
                logEntries.add(singleEntryList.getFirst()); // 假设每次只反序列化一个 LogEntry
            }
            entryBytes.release();
        }
        return logEntries;
    }
}
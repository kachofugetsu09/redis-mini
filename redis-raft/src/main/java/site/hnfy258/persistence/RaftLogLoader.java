package site.hnfy258.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.LogEntry;
import site.hnfy258.core.RedisCore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RaftLogLoader {
    private final String fileName;
    private final RedisCore redisCore;
    private FileChannel channel;
    private RandomAccessFile raf;

    public RaftLogLoader(String fileName, RedisCore redisCore)throws Exception {
        this.fileName = fileName;
        this.redisCore = redisCore;
        openFile();
    }
    public void openFile() throws IOException {
        final File file = new File(fileName);
        if (!file.exists() || file.length() == 0) {
            log.info("AOF文件不存在或为空: {}", fileName);
            return;
        }

        try {
            this.raf = new RandomAccessFile(file, "r");
            this.channel = raf.getChannel();
            log.info("RaftLogEntry文件打开成功: {}, 文件大小: {} bytes", fileName, channel.size());
        } catch (IOException e) {
            // 如果打开失败，确保资源被清理
            closeFile();
            throw new IOException("RaftLogEntry打开文件失败: " + fileName, e);
        }
    }

    public List<LogEntry> load() {
        List<LogEntry> loadedEntries = new ArrayList<>();
        loadedEntries.add(new LogEntry(-1,-1,null));
        if (channel == null) {
            log.warn("无法加载 RaftLogEntry，FileChannel 未初始化或已关闭。");
            return loadedEntries;
        }

        ByteBuf fileBuffer = null;
        try {
            long fileSize = channel.size();
            if (fileSize == 0) {
                log.info("Raft 日志文件为空，无需加载。");
                return loadedEntries;
            }
            if (fileSize > Integer.MAX_VALUE) {
                log.error("Raft 日志文件过大 ({} bytes)，无法一次性加载到内存。请考虑分块读取或日志压缩。", fileSize);
                throw new IOException("Raft 日志文件过大，无法加载。");
            }

            int bufferSize = (int) fileSize;

            fileBuffer = Unpooled.buffer(bufferSize);


            int bytesRead = fileBuffer.writeBytes(channel, 0, bufferSize);
            fileBuffer.writerIndex(bytesRead); // 设置 writerIndex 到实际读取的字节数

            log.info("已从文件读取 {} 字节到缓冲区。", bytesRead);

            // 逐个反序列化 LogEntry
            while (fileBuffer.isReadable()) {
                fileBuffer.markReaderIndex(); // 标记读索引，以便在解析失败时回滚

                try {
                    // 尝试反序列化单个 LogEntry
                    LogEntry entry = RaftLogEntrySerializer.deserialize(fileBuffer.array());

                    byte[] entryBytes = new byte[fileBuffer.readableBytes()];
                    fileBuffer.getBytes(fileBuffer.readerIndex(), entryBytes);

                    LogEntry currentEntry = RaftLogEntrySerializer.deserialize(entryBytes);


                    // 重新实现读取逻辑以适配 RaftLogEntrySerializer 的 deserialize 方法
                    // 格式: | LogIndex (4 bytes) | LogTerm (4 bytes) | Command Length (4 bytes) | Command (variable bytes) |
                    if (fileBuffer.readableBytes() < 12) { // 至少需要 LogIndex(4) + LogTerm(4) + Command Length(4)
                        log.warn("Raft 日志文件末尾数据不完整，跳过。剩余字节: {}", fileBuffer.readableBytes());
                        break; // 跳出循环，不再尝试读取
                    }

                    int currentLogIndex = fileBuffer.getInt(fileBuffer.readerIndex()); // 预读索引
                    int currentLogTerm = fileBuffer.getInt(fileBuffer.readerIndex() + 4); // 预读任期
                    int currentCmdLength = fileBuffer.getInt(fileBuffer.readerIndex() + 8); // 预读命令长度

                    int entryTotalLength = 12 + currentCmdLength; // 整个 LogEntry 的总字节长度

                    if (fileBuffer.readableBytes() < entryTotalLength) {
                        log.warn("Raft 日志条目数据不完整，跳过此条目。索引: {}, 任期: {}, 期望总长度: {}, 实际可读: {}",
                                currentLogIndex, currentLogTerm, entryTotalLength, fileBuffer.readableBytes());
                        break; // 数据不完整，跳出循环
                    }

                    // 提取当前 LogEntry 的完整字节数据
                    byte[] singleEntryBytes = new byte[entryTotalLength];
                    fileBuffer.readBytes(singleEntryBytes); // 读取一个完整 LogEntry 的字节

                    LogEntry deserializedEntry = RaftLogEntrySerializer.deserialize(singleEntryBytes);
                    loadedEntries.add(deserializedEntry);
                    log.debug("成功加载 Raft 日志条目: Index={}, Term={}, Command={}",
                            deserializedEntry.getLogIndex(), deserializedEntry.getLogTerm(),
                            deserializedEntry.getCommand() != null ? deserializedEntry.getCommand().toString() : "null");

                } catch (IllegalArgumentException | IOException e) {
                    log.warn("Raft 日志解析错误，尝试跳过此损坏条目: {}", e.getMessage());
                    fileBuffer.resetReaderIndex(); // 回滚到本条目开始
                    skipCorruptedEntry(fileBuffer); // 尝试跳过损坏的条目
                }
            }
            log.info("Raft 日志加载完成，共加载 {} 条命令。", loadedEntries.size());

        } catch (IOException e) {
            log.error("加载 Raft 日志文件失败: {}", fileName, e);
            throw new RuntimeException("加载 Raft 日志文件失败", e);
        } finally {
            if (fileBuffer != null) {
                fileBuffer.release();
            }
            closeFile();
        }
        return loadedEntries;
    }

    /**
     * 尝试跳过一个可能损坏的条目，直到找到下一个有效的命令前缀或文件结束。
     * 这是一个简单的跳过策略，可能无法完美处理所有损坏情况。
     */
    private void skipCorruptedEntry(ByteBuf buffer) {
        int skippedBytes = 0;
        int originalReaderIndex = buffer.readerIndex();

        // 尝试跳过当前行，或者直接跳过一个预设的块大小
        // 由于 Raft LogEntry 是二进制格式，没有明显的行分隔符
        // 最简单的策略是跳过一个固定大小的块，或者尝试读取下一个 LogEntry 的头部
        // 这里我们尝试跳过一个最小的 LogEntry 头部大小，然后继续尝试
        int skipAmount = Math.min(buffer.readableBytes(), 12); // 至少跳过一个 LogEntry 的元数据大小
        if (skipAmount > 0) {
            buffer.skipBytes(skipAmount);
            skippedBytes = buffer.readerIndex() - originalReaderIndex;
            log.debug("跳过 {} 字节的损坏 Raft 日志数据。", skippedBytes);
        } else {
            // 如果没有可读字节，直接跳到文件末尾
            buffer.readerIndex(buffer.writerIndex());
            log.warn("未能跳过损坏的 Raft 日志条目，已跳过剩余所有数据。");
        }
    }

    public void closeFile() {
        try {
            if (channel != null) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (IOException e) {
                    log.warn("关闭FileChannel时发生错误: {}", e.getMessage());
                } finally {
                    channel = null;
                }
            }

            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    log.warn("关闭RandomAccessFile时发生错误: {}", e.getMessage());
                } finally {
                    raf = null;
                }
            }

            log.debug("AOF文件资源已安全关闭");

        } catch (Exception e) {
            log.error("关闭AOF文件时发生意外错误: {}", e.getMessage());
        }
    }

}

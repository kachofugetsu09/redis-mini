package site.hnfy258.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import site.hnfy258.core.LogEntry;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.IOException;

public class RaftLogEntrySerializer {
    private RaftLogEntrySerializer(){
        throw new UnsupportedOperationException();
    }
    //*格式: | LogIndex (4 bytes) | LogTerm (4 bytes) | Command Length (4 bytes) | Command (RespArray encoded bytes) |
    public static ByteBuf serialize(LogEntry entry){
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        
        buf.writeInt(entry.getLogIndex());
        buf.writeInt(entry.getLogTerm());

        ByteBuf cmdBuf = null;
        try{
            if(entry.getCommand() != null){
                cmdBuf = PooledByteBufAllocator.DEFAULT.buffer();
                entry.getCommand().encode(entry.getCommand(), cmdBuf);
                
                int cmdLength = cmdBuf.readableBytes();
                buf.writeInt(cmdLength);

                if(cmdLength > 0){
                    buf.writeBytes(cmdBuf); // 写入命令数据到主缓冲区
                }
            } else {
                buf.writeInt(0); // 命令长度为0
            }

            return buf;
        } catch (Exception e) {
            buf.release(); // 发生异常时释放主缓冲区
            throw new RuntimeException("Failed to serialize LogEntry", e);
        } finally {
            if (cmdBuf != null) {
                cmdBuf.release(); // 释放临时命令缓冲区
            }
        }
    }

    public static LogEntry deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 12) { // 至少需要 LogIndex(4) + LogTerm(4) + Command Length(4)
            throw new IllegalArgumentException("Invalid byte array for LogEntry deserialization.");
        }

        // 使用 Netty 的 ByteBuf 来高效读取字节数组
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes); // 包装现有字节数组，避免拷贝
        try {
            // 1. 读取 LogIndex
            int logIndex = buffer.readInt();

            // 2. 读取 LogTerm
            int logTerm = buffer.readInt();

            // 3. 读取 Command Length
            int cmdLength = buffer.readInt();

            // 4. 读取 Command 字节流并反序列化为 RespArray
            RespArray command = null;
            if (cmdLength > 0) {
                if (buffer.readableBytes() < cmdLength) {
                    throw new IllegalArgumentException("Incomplete command bytes for LogEntry deserialization.");
                }
                ByteBuf cmdBytesBuffer = buffer.readBytes(cmdLength);
                try {
                    Resp decodedResp = Resp.decode(cmdBytesBuffer);
                    if (decodedResp instanceof RespArray) {
                        command = (RespArray) decodedResp;
                    } else {
                        throw new IllegalArgumentException("Decoded command is not a RespArray.");
                    }
                } finally {
                    cmdBytesBuffer.release();
                }
            }

            return new LogEntry(logIndex, logTerm, command);
        } finally {
            // 确保缓冲区被释放
            buffer.release();
        }
    }
}


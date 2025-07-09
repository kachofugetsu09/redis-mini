package site.hnfy258.persistence;

import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.aof.writer.Writer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RaftLogWriter implements Writer {
    String fileName;
    FileChannel channel;
    RandomAccessFile raf;

    public RaftLogWriter(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        this.raf = new RandomAccessFile(fileName, "rw");
        this.channel = raf.getChannel();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        if (channel == null || !channel.isOpen()) {
            throw new IOException("RaftLog Writer 已关闭，无法执行写入操作");
        }
        int written = channel.write(buffer);
        return written;
    }

    @Override
    public void flush() throws IOException {
        if (channel == null || !channel.isOpen()) {
            throw new IOException("RaftLog Writer 已关闭，无法执行刷新操作");
        }
        channel.force(false); // 强制将缓冲区内容写入文件
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (raf != null) {
            raf.close();
        }
    }

    @Override
    public boolean bgrewrite() throws IOException {
        return false;
    }
}

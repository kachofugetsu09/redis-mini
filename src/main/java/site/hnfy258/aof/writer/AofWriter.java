package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
@Slf4j
public class AofWriter implements Writer{
    private File file;
    private FileChannel channel;
    private RandomAccessFile raf;
    private AtomicLong size;
    private boolean isPreallocated;
    private final AtomicLong realSize = new AtomicLong(0);

    private static final int DEFAULT_PREALLOCATE_SIZE = 4 * 1024 * 1024;
    public AofWriter(File file, boolean preallocated, int flushInterval, FileChannel channel) throws FileNotFoundException {
        this.file = file;
        this.isPreallocated = preallocated;

        if(channel == null){
            this.raf = new RandomAccessFile(file,"rw");
            this.channel = raf.getChannel();
            channel = this.channel;
        }else{
            this.channel = channel;
        }

        try{
            this.size = new AtomicLong(channel.size());

            if(isPreallocated){
                preAllocated(DEFAULT_PREALLOCATE_SIZE);
            }

            this.channel.position(this.size.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void preAllocated(int defaultPreallocateSize) throws IOException {
        long currentSize = 0;
        try{
            currentSize = this.channel.size();
        }catch(IOException e){
            log.error("获取文件长度时发生错误",e);
        }
        long newSize = currentSize + defaultPreallocateSize;
        if(this.raf != null){
            this.raf.setLength(newSize);
        }
        else if(this.channel != null){
            this.channel.truncate(newSize);
        }

        this.channel.position(currentSize);
        this.realSize.set(currentSize);
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        int written = writtenFullyTo(channel,buffer);
        size.addAndGet(written);
        realSize.addAndGet(written);
        return written;
    }

    private int writtenFullyTo(FileChannel channel, ByteBuffer buffer) {
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();
        int totalBytes = buffer.remaining();

        try{
            int written = 0;
            while(written < totalBytes){
                written += channel.write(buffer);
            }
            return written;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        finally {
            buffer.position(originalPosition);
            buffer.limit(originalLimit);
        }
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
       try{
           flush();
           channel.truncate(realSize.get());
           log.info("AOF文件已截断到长度{}",realSize.get());
           channel.close();
           if(raf != null) raf.close();
       }catch(IOException e){
           log.error("关闭AOF文件时发生错误",e);
       }
    }
}

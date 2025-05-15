package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class AofWriter implements Writer{
    private File file;
    private FileChannel channel;
    private RandomAccessFile raf;
    private AtomicLong size;
    private boolean isPreallocated;

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
                this.size.set(0);
                preAllocated(DEFAULT_PREALLOCATE_SIZE);
            }

            this.channel.position(this.size.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void preAllocated(int defaultPreallocateSize) throws IOException {
        if(this.raf != null){
            this.raf.setLength(defaultPreallocateSize);
            this.channel.position(0);
        }
        else if(this.channel != null){
            this.channel.truncate(defaultPreallocateSize);
            this.channel.position(0);
        }
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        int written = writtenFullyTo(channel,buffer);
        size.addAndGet(written);
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
        flush();
        channel.close();
        if(raf != null) raf.close();
    }
}

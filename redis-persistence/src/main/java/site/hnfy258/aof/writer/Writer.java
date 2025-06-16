package site.hnfy258.aof.writer;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Writer {
    int write(ByteBuffer buffer) throws IOException;
    void flush() throws IOException;
    void close() throws IOException;
    boolean bgrewrite() throws IOException;
}

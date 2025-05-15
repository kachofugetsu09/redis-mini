package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import site.hnfy258.aof.writer.AofBatchWriter;
import site.hnfy258.aof.writer.AofWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class AofManager {
    private Writer aofWriter;
    private AofBatchWriter batchWriter;
    private String fileName;
    private boolean fileExists;

    private static final int DEFAULT_FLUSH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_PREALLOCATE = true;

    public AofManager(String fileName) throws FileNotFoundException {
        this(fileName,new File(fileName).exists(),DEFAULT_PREALLOCATE,DEFAULT_FLUSH_INTERVAL_MS);
    }

    public AofManager(String fileName,boolean fileExists,boolean preallocated, int flushInterval) throws FileNotFoundException {
        this.fileName = fileName;
        this.fileExists = fileExists;
        this.aofWriter = new AofWriter(new File(fileName), preallocated,flushInterval, null);
        this.batchWriter = new AofBatchWriter(aofWriter,flushInterval);
    }

    public void append(RespArray respArray) throws IOException {
        ByteBuf byteBuf = Unpooled.buffer();
        respArray.encode(respArray, byteBuf);
        batchWriter.write(byteBuf);
    }

    public void close() throws Exception {

        if(batchWriter != null){
            batchWriter.close();
        }
        if(aofWriter != null){
            aofWriter.close();
        }
    }

    public void flush() throws Exception {
        if(batchWriter != null){
            batchWriter.flush();
        }
    }

}

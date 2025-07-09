package site.hnfy258.persistence;

import io.netty.buffer.ByteBuf;
import site.hnfy258.core.LogEntry;
import site.hnfy258.core.RedisCore;
import site.hnfy258.raft.Raft;

import java.util.List;

public class RaftLogManager {
    private RaftLogWriter writer;
    private RaftLogLoader loader;
    private RedisCore redisCore;
    private Raft raft;

    public RaftLogManager(String fileName,RedisCore redisCore,Raft raft) {
        try {
            this.writer = new RaftLogWriter(fileName);
            this.redisCore = redisCore;
            this.loader = new RaftLogLoader(fileName, redisCore);
            this.raft = raft;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RaftLogManager", e);
        }
    }

    public void write(LogEntry entry){
        ByteBuf serialize = RaftLogEntrySerializer.serialize(entry);
        try {
            writer.write(serialize.nioBuffer());
            writer.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write log entry", e);
        } finally {
            serialize.release(); // 确保释放ByteBuf
        }
    }

    public void load(){
        try{
            List<LogEntry> result = loader.load();
            raft.setLog(result);
        }catch (Exception e){
            throw new RuntimeException("Failed to load log entries", e);
        } finally {
            try {
                loader.closeFile();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close log file", e);
            }
        }
    }

    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            if (loader != null) {
                loader.closeFile();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close RaftLogManager", e);
        }
    }
}

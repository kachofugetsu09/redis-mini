package site.hnfy258.cluster.replication;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.StringReader;

@Slf4j
@Getter
@Setter
public class ReplBackLog {

    private static final int DEFAULT_BACKLOG_SIZE = 1024*1024; // 默认缓冲区大小1MB

    private final byte[] buffer;
    private final int bufferSize;

    private long writeIndex; // 写入索引
    private volatile long backlogOffset; //缓冲区最早偏移量
    private volatile long backloghistlen; //缓冲区最晚偏移量

    private String masterRunId; // 主节点运行ID
    private boolean hasWrapped; // 是否已环绕

    private volatile long lastPartialSyncFailedTime;// 最近一次部分同步失败的时间戳
    private long partialSyncSuccessCount; // 部分同步成功次数
    private long partialSyncFailedCount; // 部分同步失败次数

    private long baseOffset; // 基础偏移量，用于计算相对偏移量

    public ReplBackLog(int bufferSize) {
        this.buffer = new byte[bufferSize];
        this.bufferSize = bufferSize;
        this.writeIndex = 0;
        this.backlogOffset = 1;
        this.backloghistlen = 0;
        this.hasWrapped = false;
        this.lastPartialSyncFailedTime = 0;
        this.partialSyncSuccessCount = 0;
        this.partialSyncFailedCount = 0;
        this.baseOffset = 0;
    }

    public ReplBackLog(String masterRunId) {
        this(DEFAULT_BACKLOG_SIZE);
        this.masterRunId = masterRunId;
    }

    public ReplBackLog() {
        this(DEFAULT_BACKLOG_SIZE);
    }

    public synchronized long addCommand(byte[] command){
        //1.检查命令是否合规
        if(command == null || command.length == 0) {
            return backloghistlen;
        }

        long commandLength = command.length;
        if(commandLength > bufferSize){
            log.warn("命令长度超过缓冲区大小，无法添加到缓冲区");
            return backloghistlen;
        }
        //2.如果是第一次添加，初始化偏移量
        if(backloghistlen ==0){
            if(baseOffset >0){
                baseOffset = backlogOffset;
                backloghistlen = baseOffset;
                log.info("第一次添加命令，设置基础偏移量为: {}", baseOffset);
            }else{
                backloghistlen = backlogOffset;
            }
        }

        if(writeIndex+ commandLength >bufferSize){
            long firstPart = bufferSize - writeIndex;
            long secondPart = commandLength - firstPart;

            System.arraycopy(command, 0, buffer, (int) writeIndex, (int) firstPart);
            System.arraycopy(command, (int) firstPart, buffer, 0, (int) secondPart);
            writeIndex = secondPart;
            hasWrapped = true;

            long totalDataInBuffer = Math.min(backloghistlen - backlogOffset + commandLength, bufferSize);
            backlogOffset = backloghistlen+commandLength - totalDataInBuffer +1;

            log.debug("缓冲区已环绕，新的写入索引: {}, 新的偏移量: {}", writeIndex, backlogOffset);
        }else{
            System.arraycopy(command, 0, buffer, (int) writeIndex, (int) commandLength);
            writeIndex += commandLength;
            if(hasWrapped){
                long totalDataInBuffer = Math.min(backloghistlen - backlogOffset + commandLength, bufferSize);
                if(totalDataInBuffer >=bufferSize){
                    backlogOffset = backloghistlen +commandLength - bufferSize + 1;
                }
            }
        }

        backloghistlen += commandLength;
        log.debug("添加命令成功，新的写入索引: {}, 新的偏移量: {}, 当前缓冲区长度: {}", writeIndex, backlogOffset, backloghistlen);
        return backloghistlen;
    }

    public synchronized byte[] getRangeCommand(long start, long end){
        if(start <0 || end>backloghistlen){
            throw new IllegalArgumentException("偏移量超出范围");
        }
        if(start > end){
            throw new IllegalArgumentException("开始偏移量不能大于结束偏移量");
        }
        if(start < backlogOffset){
            throw new IllegalArgumentException("开始偏移量不能小于当前缓冲区最早偏移量");
        }

        long length = end - start;
        if(length <= 0){
            return new byte[0]; // 如果长度为0，返回空数组
        }

        if(length > bufferSize){
            throw new IllegalArgumentException("请求的范围超过缓冲区大小");
        }

        byte[] result = new byte[(int) length];

        long relativeStart = start - backlogOffset; // 计算相对偏移量
        long earliestDataPos;
        if(hasWrapped){
            earliestDataPos = writeIndex;
        }
        else{
            earliestDataPos = 0;
        }

        long actualStartPos = (earliestDataPos +relativeStart)% bufferSize; // 计算实际起始位置

        if(actualStartPos + length <= bufferSize){
            System.arraycopy(buffer, (int) actualStartPos, result, 0, (int) length);
        } else {
            // 分段复制
            long firstPartLength = bufferSize - actualStartPos;

            System.arraycopy(buffer, (int) actualStartPos, result, 0, (int) firstPartLength);
            System.arraycopy(buffer, 0, result, (int) firstPartLength, (int) (length - firstPartLength));
        }

        log.debug("获取范围命令成功，环绕复制，起始位置: {}, 长度: {}", actualStartPos, length);
        return result;
    }

    public byte[] getCommandSince(long offset) {
        if (offset < 0 || offset > backloghistlen) {
            throw new IllegalArgumentException("偏移量超出范围");
        }

        if(offset < backlogOffset){
            throw new IllegalArgumentException("偏移量不能小于当前缓冲区最早偏移量");
        }

       if(offset == backlogOffset){
           return new byte[0]; // 如果偏移量等于当前偏移量，返回空数组
       }

       return getRangeCommand(offset, backloghistlen);
    }

    public synchronized void clear() {
        writeIndex = 0;
        backlogOffset = 1;
        backloghistlen = 0;
        hasWrapped = false;
        lastPartialSyncFailedTime = 0;
        partialSyncSuccessCount = 0;
        partialSyncFailedCount = 0;
        baseOffset = 0;
        log.info("已清空缓冲区");
    }

    public boolean canPartialSync(String masterId, long offset) {
        //1.检查运行id
        if(masterRunId == null || !masterRunId.equals(masterId)) {
            log.warn("主节点运行ID不匹配，无法进行部分同步");
            partialSyncFailedCount++;
            lastPartialSyncFailedTime = System.currentTimeMillis();
            return false;
        }

        if(offset <backlogOffset || offset > backloghistlen) {
            log.warn("偏移量不在缓冲区范围内，无法进行部分同步: 当前偏移量={}, 请求偏移量={}", backlogOffset, offset);
            partialSyncFailedCount++;
            lastPartialSyncFailedTime = System.currentTimeMillis();
            return false;
        }

        partialSyncSuccessCount++;
        log.info("可以进行部分同步，主节点ID: {}, 偏移量: {}", masterId, offset);
        return true;
    }

    public synchronized void reset(){
        writeIndex = 0;
        backlogOffset = 1;
        backloghistlen = 0;
        hasWrapped = false;
        log.info("缓冲区已重置");
    }

    public synchronized void failoverReset(){
        writeIndex = 0;
        hasWrapped = false;
        log.info("缓冲区已重置，准备进行故障转移");
    }

    public void setBaseOffset(long prevOffset) {
        this.baseOffset = prevOffset;
    }
}

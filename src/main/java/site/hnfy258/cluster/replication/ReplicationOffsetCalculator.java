package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.RespArray;
@Slf4j
public final class ReplicationOffsetCalculator {

    private static final int INITAL_BUFFER_SIZE = 256;
    private static final int MAX_COMMAND_SIZE = 16 * 1024* 1024;

    private ReplicationOffsetCalculator() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    public static long calculateCommandOffset(RespArray command){
        if(command == null){
            log.warn("无法计算命令偏移量，命令为 null");
            return 0;
        }
        ByteBuf buffer = null;
        try{
            buffer = Unpooled.buffer(INITAL_BUFFER_SIZE);

            command.encode(command, buffer);

            int commandSize = buffer.readableBytes();

            if(commandSize <= 0 ){
                log.warn("命令大小为0，无法计算偏移量");
                return 0;
            }
            if(commandSize > MAX_COMMAND_SIZE){
                log.warn("命令大小超过最大限制 {}，无法计算偏移量", MAX_COMMAND_SIZE);
                return 0;
            }

            return commandSize;
        }catch(Exception e){
            log.error("计算命令偏移量时发生异常: {}", e.getMessage(), e);
            return 0;
        }
        finally {
            if(buffer != null && buffer.refCnt() > 0){
                buffer.release();
            }
        }
    }
    public static boolean validateOffset(long correctOffset, String str) {
        if(correctOffset < 0) {
            log.warn("偏移量不合理，当前值: {}", correctOffset);
            return false;
        }
        if(str == null || str.isEmpty()) {
            log.warn("字符串不能为空，无法验证偏移量");
            return false;
        }

        log.debug("偏移量验证通过，值: {}", correctOffset);
        return true;
    }

    public static long calculatePostRdbOffset(long masterBaseOffset, int length) {
        if(masterBaseOffset <0){
            log.warn("主节点偏移量不能为负数，当前值: {}", masterBaseOffset);
            return 0;
        }

        if(length <0){
            log.warn("长度不能为负数，当前值: {}", length);
            return masterBaseOffset;
        }
        log.debug("使用主节点基准偏移量作为rdb后偏移量");
        return masterBaseOffset;
    }
}

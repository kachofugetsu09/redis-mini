package site.hnfy258.aof.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.datastructure.*;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

@Slf4j
public class AofUtils {
    public static void writeDataToAof(RedisBytes key, RedisData value, FileChannel channel) {
        if(value == null){
            return;
        }
        switch(value.getClass().getSimpleName()){
            case "RedisString":
                writeStringToAof(key, (RedisString)value, channel);
                break;
            case "RedisList":
                writeListToAof(key, (RedisList)value, channel);
                break;
            case "RedisSet":
                writeSetToAof(key, (RedisSet)value, channel);
                break;
            case "RedisHash":
                writeHashToAof(key, (RedisHash)value, channel);
                break;
            case "RedisZset":
                writeZSetToAof(key, (RedisZset)value, channel);
                break;
            default:
                log.warn("Unsupported data type: " + value.getClass().getSimpleName());
        }
    }

    private static void writeStringToAof(RedisBytes key, RedisString data, FileChannel channel) {
        data.setKey(key);
        List<Resp> resps = data.convertToResp();
        if(!resps.isEmpty()){
            writeCommandsToChannel(resps, channel);
            log.info("已重写字符串类型数据到AOF文件，key:{}", key);
        }
    }

    private static void writeCommandsToChannel(List<Resp> resps, FileChannel channel) {
        if(!resps.isEmpty()){
            for(Resp command: resps){
                ByteBuf buf = Unpooled.buffer();
                command.encode(command, buf);
                ByteBuffer byteBuffer = buf.nioBuffer();
                int written = 0;
                while(written < byteBuffer.remaining()){
                    try {
                        written += channel.write(byteBuffer);
                    } catch (Exception e) {
                        log.error("Error writing to AOF file", e);
                    }
                }
                buf.release();
            }
        }
    }

    private static void writeListToAof(RedisBytes key, RedisList data, FileChannel channel) {
        data.setKey(key);
        List<Resp> resps = data.convertToResp();
        if(!resps.isEmpty()){
            writeCommandsToChannel(resps, channel);
            log.info("已重写list类型数据到AOF文件，key:{}", key);
        }
    }

    private static void writeSetToAof(RedisBytes key, RedisSet data, FileChannel channel) {
        data.setKey(key);
        List<Resp> resps = data.convertToResp();
        if(!resps.isEmpty()){
            writeCommandsToChannel(resps, channel);
            log.info("已重写set类型到AOF文件，key:{}", key);
        }
    }

    private static void writeHashToAof(RedisBytes key, RedisHash data, FileChannel channel) {
        data.setKey(key);
        List<Resp> resps = data.convertToResp();
        if(!resps.isEmpty()){
            writeCommandsToChannel(resps, channel);
            log.info("已重写哈希类型到AOF文件，key:{}", key);
        }
    }

    private static void writeZSetToAof(RedisBytes key, RedisZset data, FileChannel channel) {
        data.setKey(key);
        List<Resp> resps = data.convertToResp();
        if(!resps.isEmpty()){
            writeCommandsToChannel(resps, channel);
            log.info("已重写zset类型数据到AOF文件，key:{}", key);
        }
    }
}

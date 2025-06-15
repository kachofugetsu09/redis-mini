package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.core.RedisCore;

/**
 * GETRANGE命令实现 - 展现SDS二进制安全和高效字节访问
 * 
 * <p>SDS优势体现：</p>
 * <ul>
 *   <li><strong>二进制安全</strong>: 支持包含\0的任意字节序列</li>
 *   <li><strong>O(1)长度检查</strong>: 快速边界验证</li>
 *   <li><strong>零拷贝访问</strong>: 直接访问内部字节数组</li>
 * </ul>
 */
@Slf4j
public class Getrange implements Command {
    private RedisBytes key;
    private int start;
    private int end;
    private RedisContext redisContext;

    public Getrange(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.GETRANGE;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length != 4) {
            throw new IllegalStateException("GETRANGE命令需要3个参数");
        }
        key = ((BulkString) array[1]).getContent();
        start = Integer.parseInt(((BulkString) array[2]).getContent().getString());
        end = Integer.parseInt(((BulkString) array[3]).getContent().getString());
    }

    @Override
    public Resp handle() {
        RedisData data = redisContext.get(key);
        
        if (data == null) {
            // 键不存在，返回空字符串
            return new BulkString(new byte[0]);
        }
        
        if (!(data instanceof RedisString)) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        
        RedisString redisString = (RedisString) data;
        Sds sds = redisString.getSds();
        
        // SDS的O(1)长度获取用于边界检查
        int length = sds.length();
        
        if (length == 0) {
            return new BulkString(new byte[0]);
        }
        
        // 处理负索引
        int startIndex = start < 0 ? Math.max(0, length + start) : Math.min(start, length);
        int endIndex = end < 0 ? Math.max(-1, length + end) : Math.min(end, length - 1);
        
        if (startIndex > endIndex || startIndex >= length) {
            return new BulkString(new byte[0]);
        }
        
        // 直接从SDS获取字节数组进行范围截取
        byte[] fullBytes = sds.getBytes();
        int rangeLength = endIndex - startIndex + 1;
        byte[] rangeBytes = new byte[rangeLength];
        
        System.arraycopy(fullBytes, startIndex, rangeBytes, 0, rangeLength);
        
        log.debug("GETRANGE key:{} start:{} end:{} result_length:{}", 
                 key.getString(), start, end, rangeLength);
        
        return new BulkString(rangeBytes);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.core.RedisCore;

/**
 * STRLEN命令实现 - 展现SDS O(1)长度获取的性能优势
 * 
 * <p>SDS vs Java String长度获取性能对比：</p>
 * <ul>
 *   <li><strong>SDS</strong>: O(1) - 直接返回预存储的len字段</li>
 *   <li><strong>Java String</strong>: O(1) - 但有对象创建和方法调用开销</li>
 *   <li><strong>C字符串</strong>: O(n) - 需要遍历到\0结束符</li>
 * </ul>
 * 
 * <p>在高并发场景下，SDS避免了不必要的计算和内存访问。</p>
 */
@Slf4j
public class Strlen implements Command {
    private RedisBytes key;
    private RedisContext redisContext;

    public Strlen(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.STRLEN;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length != 2) {
            throw new IllegalStateException("STRLEN命令需要1个参数");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData data = redisContext.get(key);
        
        if (data == null) {
            // 键不存在，返回0
            return new RespInteger(0);
        }
        
        if (!(data instanceof RedisString)) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
          RedisString redisString = (RedisString) data;
        
        // SDS的O(1)长度获取 - 核心优势展示
        int length = redisString.getSds().length();
        
        log.debug("STRLEN key:{} length:{}", key.getString(), length);
        return new RespInteger(length);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

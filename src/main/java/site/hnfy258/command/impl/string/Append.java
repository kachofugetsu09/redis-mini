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
import site.hnfy258.server.core.RedisCore;

/**
 * APPEND命令实现 - 展现SDS追加操作的性能优势
 * 
 * <p>相比Java String的每次创建新对象，SDS的append操作：</p>
 * <ul>
 *   <li>原地修改，避免对象创建开销</li>
 *   <li>智能预分配，减少内存重分配次数</li>
 *   <li>O(1)长度更新，无需重新计算</li>
 * </ul>
 */
@Slf4j
public class Append implements Command {
    private RedisBytes key;
    private RedisBytes value;
    private RedisCore redisCore;

    public Append(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.APPEND;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length != 3) {
            throw new IllegalStateException("APPEND命令需要2个参数");
        }
        key = ((BulkString) array[1]).getContent();
        value = ((BulkString) array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData data = redisCore.get(key);
        
        if (data == null) {
            // 键不存在，创建新的RedisString
            Sds sds = new Sds(value.getBytes());
            redisCore.put(key, new RedisString(sds));
            return new RespInteger(value.getBytes().length);
        }
        
        if (!(data instanceof RedisString)) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
          RedisString redisString = (RedisString) data;
        
        // 使用SDS的高效append操作
        redisString.getSds().append(value.getBytes());
        
        // 返回追加后的总长度 - SDS的O(1)长度获取
        return new RespInteger(redisString.getSds().length());
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

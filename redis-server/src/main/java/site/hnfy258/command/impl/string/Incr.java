package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.context.RedisContext;


/**
 * INCR命令实现 - 将key中储存的数字值增一
 * 语法: INCR key
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
@Slf4j
public class Incr implements Command {
    
    private RedisBytes key;
    private RedisContext redisContext;

    public Incr(final RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.INCR;
    }

    @Override
    public void setContext(final Resp[] array) {
        if (array.length < 2) {
            throw new IllegalStateException("参数不足");
        }
        this.key = ((BulkString) array[1]).getContent();
    }    @Override
    public Resp handle() {
        try {
            // 1. 获取或创建RedisString对象
            RedisData redisData = redisContext.get(key);
            RedisString redisString;
              if (redisData == null) {
                // 2. 键不存在，创建新的RedisString，值为0
                redisString = new RedisString(Sds.create("0".getBytes()));
                redisContext.put(key, redisString);
            } else if (redisData instanceof RedisString) {
                // 3. 键存在且为字符串类型
                redisString = (RedisString) redisData;
            } else {
                // 4. 键存在但不是字符串类型
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            
            // 5. 使用RedisString内置的高效递增方法
            final long newValue = redisString.incr();
            
            // 6. 返回新值
            return new SimpleString(String.valueOf(newValue));
            
        } catch (final IllegalStateException e) {
            return new Errors("ERR value is not an integer or out of range");
        } catch (final Exception e) {
            return new Errors("ERR " + e.getMessage());
        }
    }    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

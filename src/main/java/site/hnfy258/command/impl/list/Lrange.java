package site.hnfy258.command.impl.list;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.core.RedisCore;

import java.util.List;

/**
 * LRANGE命令实现 - 返回列表中指定区间内的元素
 * 语法: LRANGE key start stop
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
@Slf4j
public class Lrange implements Command {

    private RedisContext redisContext;
    private RedisBytes key;
    private int start;
    private int stop;

    public Lrange(final RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.LRANGE;
    }

    @Override
    public void setContext(final Resp[] array) {
        if (array.length < 4) {
            throw new IllegalStateException("参数不足：LRANGE需要key、start和stop参数");
        }
        
        try {
            key = ((BulkString) array[1]).getContent();
            start = Integer.parseInt(((BulkString) array[2]).getContent().getString());
            stop = Integer.parseInt(((BulkString) array[3]).getContent().getString());
        } catch (final NumberFormatException e) {
            throw new IllegalStateException("start和stop参数必须是有效的整数");
        }
    }

    @Override
    public Resp handle() {
        try {
            // 1. 获取列表数据
            final RedisData redisData = redisContext.get(key);
            
            if (redisData == null) {
                // 2. 键不存在，返回空数组
                return new RespArray(new Resp[0]);
            }
            
            if (!(redisData instanceof RedisList)) {
                // 3. 键存在但不是列表类型
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            final RedisList redisList = (RedisList) redisData;
            
            // 4. 获取指定范围的元素
            final List<RedisBytes> rangeElements = redisList.lrange(start, stop);
            
            // 5. 转换为响应数组
            final Resp[] respArray = new Resp[rangeElements.size()];
            for (int i = 0; i < rangeElements.size(); i++) {
                respArray[i] = new BulkString(rangeElements.get(i));
            }
            
            return new RespArray(respArray);
            
        } catch (final Exception e) {
            log.error("LRANGE命令执行失败", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

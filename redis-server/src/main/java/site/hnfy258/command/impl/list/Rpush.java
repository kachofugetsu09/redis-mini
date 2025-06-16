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
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;


import java.util.ArrayList;
import java.util.List;

/**
 * RPUSH命令实现 - 将一个或多个值插入到列表尾部
 * 语法: RPUSH key value1 [value2 ...]
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
@Slf4j
public class Rpush implements Command {

    private RedisContext redisContext;
    private final List<RedisBytes> elements = new ArrayList<>();
    private RedisBytes key;

    public Rpush(final RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.RPUSH;
    }

    @Override
    public void setContext(final Resp[] array) {
        if (array.length < 3) {
            throw new IllegalStateException("参数不足：RPUSH需要至少一个值");
        }
        
        // 1. 解析键名
        key = ((BulkString) array[1]).getContent();
        
        // 2. 解析要插入的元素
        elements.clear();
        for (int i = 2; i < array.length; i++) {
            elements.add(((BulkString) array[i]).getContent());
        }
    }

    @Override
    public Resp handle() {
        try {
            // 1. 获取或创建列表
            final RedisData redisData = redisContext.get(key);
            final RedisList redisList;

            if (redisData == null) {
                redisList = new RedisList();
            } else if (redisData instanceof RedisList) {
                redisList = (RedisList) redisData;
            } else {
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            // 2. 批量插入元素到列表尾部
            final RedisBytes[] elementArray = elements.toArray(new RedisBytes[0]);
            redisList.rpush(elementArray);
            
            // 3. 保存列表
            redisContext.put(key, redisList);
            
            // 4. 返回列表长度
            return new RespInteger(redisList.size());
            
        } catch (final Exception e) {
            log.error("RPUSH命令执行失败", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.core.RedisBatchOptimizer;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.context.RedisContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MSET命令实现 - 同时设置一个或多个键值对
 * 语法: MSET key1 value1 [key2 value2 ...]
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
@Slf4j
public class Mset implements Command {

    private final Map<RedisBytes, RedisBytes> keyValuePairs = new LinkedHashMap<>();
    private RedisContext redisContext;

    public Mset(final RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.MSET;
    }

    @Override
    public void setContext(final Resp[] array) {
        // 1. 检查参数数量（至少需要一对key-value，所以总数至少为3）
        if (array.length < 3 || (array.length - 1) % 2 != 0) {
            throw new IllegalStateException("参数数量错误：MSET需要偶数个键值对参数");
        }

        // 2. 解析键值对并保存到Map中
        keyValuePairs.clear();
        for (int i = 1; i < array.length; i += 2) {
            final RedisBytes key = ((BulkString) array[i]).getContent();
            final RedisBytes value = ((BulkString) array[i + 1]).getContent();
            
            keyValuePairs.put(key, value);
        }
    }

    @Override
    public Resp handle() {        
        try {
            // 1. 使用批处理优化器进行批量设置，通过RedisContext获取RedisCore
            RedisBatchOptimizer.batchSetStrings(redisContext.getRedisCore(), keyValuePairs);
            
            // 2. 返回成功响应
            return SimpleString.OK;
            
        } catch (final Exception e) {
            log.error("MSET命令执行失败", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

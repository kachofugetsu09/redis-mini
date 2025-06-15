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
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.core.RedisCore;

/**
 * RPOP命令实现 - 移除并返回列表的最后一个元素
 * 语法: RPOP key
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
@Slf4j
public class Rpop implements Command {

    private RedisContext redisContext;
    private RedisBytes key;

    public Rpop(final RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.RPOP;
    }

    @Override
    public void setContext(final Resp[] array) {
        if (array.length < 2) {
            throw new IllegalStateException("参数不足：RPOP需要一个键参数");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        try {
            // 1. 获取列表数据
            final RedisData redisData = redisContext.get(key);

            if (redisData == null) {
                // 2. 键不存在，返回null
                return new BulkString((RedisBytes) null);
            }
            
            if (!(redisData instanceof RedisList)) {
                // 3. 键存在但不是列表类型
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            final RedisList redisList = (RedisList) redisData;
            
            // 4. 从列表尾部弹出元素
            final RedisBytes poppedElement = redisList.rpop();
            
            if (poppedElement == null) {
                // 5. 列表为空
                return new BulkString((RedisBytes) null);
            }

            // 6. 更新列表状态
            if (redisList.size() == 0) {
                // 7. 如果列表变空，删除键
                redisContext.put(key, null);
            } else {
                // 8. 保存更新后的列表
                redisContext.put(key, redisList);
            }

            return new BulkString(poppedElement);
            
        } catch (final Exception e) {
            log.error("RPOP命令执行失败", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

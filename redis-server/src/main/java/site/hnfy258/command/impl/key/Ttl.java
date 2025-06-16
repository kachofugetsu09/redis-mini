package site.hnfy258.command.impl.key;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

public class Ttl implements Command {
    private final RedisContext context;
    private Resp[] array;
    private RedisBytes key;

    public Ttl(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.TTL;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.key = ((BulkString) array[1]).getContent();
        }
    }

    @Override
    public Resp handle() {
        if (key == null) {
            return new RespInteger(-2); // key不存在返回-2
        }

        RedisData data = context.get(key);
        if (data == null) {
            return new RespInteger(-2); // key不存在返回-2
        }

        long timeout = data.timeout();
        if (timeout == -1) {
            return new RespInteger(-1); // key永不过期返回-1
        }

        // 计算剩余过期时间（秒）
        long now = System.currentTimeMillis();
        if (now >= timeout) {
            return new RespInteger(-2); // key已过期返回-2
        }
        
        long ttl = (timeout - now) / 1000;
        // 如果ttl大于Integer.MAX_VALUE，返回Integer.MAX_VALUE
        return new RespInteger(ttl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)ttl);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
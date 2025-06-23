package site.hnfy258.command.impl.zset;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisZset;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

public class Zcard implements Command {
    private final RedisContext context;
    private Resp[] array;
    private RedisBytes key;

    public Zcard(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZCARD;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.key = RedisBytes.fromString(((BulkString) array[1]).toString());
        }
    }    @Override
    public Resp handle() {
        if (key == null) {
            return RespInteger.ZERO;
        }

        RedisData data = context.get(key);
        if (data == null) {
            return RespInteger.ZERO;
        }

        if (!(data instanceof RedisZset)) {
            throw new IllegalArgumentException("Key is not a sorted set");
        }

        return RespInteger.valueOf((int) ((RedisZset) data).size());
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
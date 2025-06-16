package site.hnfy258.command.impl.list;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisList;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

public class Llen implements Command {
    private final RedisContext context;
    private Resp[] array;
    private RedisBytes key;

    public Llen(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.LLEN;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.key = RedisBytes.fromString(((BulkString) array[1]).toString());
        }
    }

    @Override
    public Resp handle() {
        if (key == null) {
            return new RespInteger(0);
        }

        RedisData data = context.get(key);
        if (data == null) {
            return new RespInteger(0);
        }

        if (!(data instanceof RedisList)) {
            throw new IllegalArgumentException("Key is not a list");
        }

        return new RespInteger(((RedisList) data).size());
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
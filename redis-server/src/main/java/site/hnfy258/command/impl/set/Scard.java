package site.hnfy258.command.impl.set;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisSet;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

public class Scard implements Command {
    private final RedisContext context;
    private Resp[] array;
    private RedisBytes key;

    public Scard(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.SCARD;
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

        if (!(data instanceof RedisSet)) {
            throw new IllegalArgumentException("Key is not a set");
        }

        return RespInteger.valueOf(((RedisSet) data).size());
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
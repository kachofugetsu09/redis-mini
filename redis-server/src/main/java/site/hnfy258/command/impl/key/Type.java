package site.hnfy258.command.impl.key;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.*;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

public class Type implements Command {
    private final RedisContext context;
    private Resp[] array;
    private RedisBytes key;

    public Type(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.TYPE;
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
            return new SimpleString("none");
        }

        RedisData data = context.get(key);
        if (data == null) {
            return new SimpleString("none");
        }

        if (data instanceof RedisString) {
            return new SimpleString("string");
        } else if (data instanceof RedisList) {
            return new SimpleString("list");
        } else if (data instanceof RedisSet) {
            return new SimpleString("set");
        } else if (data instanceof RedisZset) {
            return new SimpleString("zset");
        } else if (data instanceof RedisHash) {
            return new SimpleString("hash");
        } else {
            return new SimpleString("none");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
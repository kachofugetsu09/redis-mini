package site.hnfy258.command.impl.hash;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;


public class Hset implements Command {
    private RedisContext redisContext;
    private RedisBytes key;
    private RedisBytes field;
    private RedisBytes value;

    public Hset(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.HSET;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length == 4){
            key = ((BulkString)array[1]).getContent();
            field = ((BulkString)array[2]).getContent();
            value = ((BulkString)array[3]).getContent();
        }
        else{
            throw new IllegalStateException("参数错误");
        }

    }

    @Override
    public Resp handle() {
        RedisData redisData = redisContext.get(key);
        if(redisData == null){
            RedisHash hash = new RedisHash();
            int put = hash.put(field, value);
            redisContext.put(key, hash);
            return new RespInteger(put);
        }
        else if(redisData instanceof RedisHash){
            RedisHash hash = (RedisHash) redisData;
            int put = hash.put(field, value);
            redisContext.put(key, hash);
            return new RespInteger(put);
        }
        return new Errors("参数错误");
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

package site.hnfy258.command.impl.hash;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.server.core.RedisCore;

public class Hget implements Command {
    private RedisCore redisCore;
    private RedisBytes key;
    private RedisBytes field;

    public Hget(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.HGET;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length !=3){
            throw new IllegalStateException("参数不足");
        }
        key = ((BulkString)array[1]).getContent();
        field = ((BulkString)array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null) return new BulkString((RedisBytes)null);
        if(redisData instanceof RedisHash){
            RedisHash redisHash = (RedisHash) redisData;
            RedisBytes value = redisHash.getHash().get(field);
            return new BulkString(value);
        }
        return new Errors("key not hash");
    }
}

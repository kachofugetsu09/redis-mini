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
import site.hnfy258.server.core.RedisCore;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Hdel implements Command {
    private RedisContext redisContext;
    private RedisBytes key;
    private List<RedisBytes> fields;

    public Hdel(RedisContext redisContext) {
        this.redisContext = redisContext;
    }
    @Override
    public CommandType getType() {
        return CommandType.HDEL;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length < 3){
            throw new IllegalStateException("参数不足");
        }
        key = ((BulkString)array[1]).getContent();
        fields = Stream.of(array).skip(2).map(BulkString.class::cast).map( BulkString::getContent).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisContext.get(key);
        if(redisData == null){
            return new BulkString(new RedisBytes("0".getBytes()));
        }
        if(redisData instanceof RedisHash){
            int delete = ((RedisHash) redisData).del(fields);
            return new RespInteger(delete);
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

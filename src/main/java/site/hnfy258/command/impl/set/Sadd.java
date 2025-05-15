package site.hnfy258.command.impl.set;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.core.RedisCore;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sadd implements Command {
    private RedisCore redisCore;
    private RedisBytes key;
    private List<RedisBytes> members;

    public Sadd(RedisCore redisCore) {
        this.redisCore = redisCore;

    }
    @Override
    public CommandType getType() {
        return CommandType.SADD;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString)array[1]).getContent();
        members = Stream.of(array).skip(2).map(BulkString.class::cast).map(BulkString::getContent).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisSet redisSet = null;
        RedisData redisData = redisCore.get(key);
        if(redisData == null) redisSet = new RedisSet();
        if(redisData instanceof RedisSet){
            redisSet = (RedisSet) redisData;
        }
        int add = redisSet.add(members);
        redisCore.put(key, redisSet);
        return new RespInteger(add);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

package site.hnfy258.command.impl.set;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sadd implements Command {
    private RedisContext redisContext;
    private RedisBytes key;
    private List<RedisBytes> members;

    public Sadd(RedisContext redisContext) {
        this.redisContext = redisContext;

    }
    @Override
    public CommandType getType() {
        return CommandType.SADD;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString)array[1]).getContent();
        members = Stream.of(array).skip(2).map(BulkString.class::cast).map(BulkString::getContent).collect(Collectors.toList());
    }    @Override
    public Resp handle() {
        RedisSet redisSet = null;
        RedisData redisData = redisContext.get(key);
        
        if (redisData == null) {
            // 1. 键不存在，创建新的Set
            redisSet = new RedisSet();
        } else if (redisData instanceof RedisSet) {
            // 2. 键存在且类型正确
            redisSet = (RedisSet) redisData;
        } else {
            // 3. 键存在但类型错误
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        
        // 4. 执行添加操作
        int addCount = redisSet.add(members);
        redisContext.put(key, redisSet);
        return new RespInteger(addCount);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

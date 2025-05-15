package site.hnfy258.command.impl.set;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.core.RedisCore;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@Slf4j
public class Srem implements Command {
    private RedisCore redisCore;
    private RedisBytes key;
    private List<RedisBytes> members;
    public Srem(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.SREM;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString)array[1]).getContent();
        members = Stream.of(array).skip(2).map(BulkString.class::cast).map(BulkString::getContent).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null) return new Errors("ERR no such key");
        int count=0;
        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;
            for(RedisBytes member : members){
                count+=redisSet.remove(member);
                log.info("remove {} from set {}", member.getString(), key.getString());
            }
        }
        return new RespInteger(count);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

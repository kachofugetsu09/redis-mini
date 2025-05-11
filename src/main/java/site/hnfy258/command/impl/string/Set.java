package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.core.RedisCore;
@Slf4j
public class Set implements Command {
    private RedisBytes key;
    private RedisBytes value;
    private RedisCore redisCore;

    public Set(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.SET;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length < 3){
            throw new IllegalStateException("参数不足");
        }
        key = ((BulkString)array[1]).getContent();
        value = ((BulkString)array[2]).getContent();
    }

    @Override
    public Resp handle() {
        if(redisCore.get(key) != null){
            RedisData data = redisCore.get(key);
            if(data instanceof RedisString){
                RedisString redisString = (RedisString) data;
                redisString.setSds(new Sds(value.getBytes()));
                return new SimpleString("OK");
            }
        }
        redisCore.put(key, new RedisString(new Sds(value.getBytes())));
        log.info("set key:{} value:{}", key, value);

        return new SimpleString("OK");
    }
}

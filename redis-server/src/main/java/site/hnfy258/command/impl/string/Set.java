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
import site.hnfy258.server.context.RedisContext;
@Slf4j
public class Set implements Command {
    private RedisBytes key;
    private RedisBytes value;
    private RedisContext redisContext;

    public Set(RedisContext redisContext) {
        this.redisContext = redisContext;
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
        if(redisContext.get(key) != null){            RedisData data = redisContext.get(key);
            if(data instanceof RedisString){
                RedisString redisString = (RedisString) data;
                redisString.setSds(Sds.create(value.getBytesUnsafe()));
                return SimpleString.OK;
            }
        }
        redisContext.put(key, new RedisString(Sds.create(value.getBytesUnsafe())));
//        log.info("set key:{} value:{}", key, value);

        return SimpleString.OK;
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

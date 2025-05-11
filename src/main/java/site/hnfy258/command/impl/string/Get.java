package site.hnfy258.command.impl.string;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.server.core.RedisCore;

import static site.hnfy258.protocal.BulkString.NULL_BYTES;

@Slf4j
public class Get implements Command {
    private RedisCore redisCore;
    private RedisBytes key;

    public Get(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.GET;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString)array[1]).getContent();
    }

    @Override
    public Resp handle() {
        try{
            RedisData data = redisCore.get(key);
            if(data == null){
                return new BulkString(NULL_BYTES);
            }
            if(data instanceof RedisString){
                RedisString redisString = (RedisString) data;
                return new BulkString(redisString.getValue());
            }
        }catch(Exception e){
            log.error("handle error", e);
            return new Errors("ERR internal server error");
        }
        return new Errors("ERR unknown error");
    }
}

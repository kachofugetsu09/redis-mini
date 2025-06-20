package site.hnfy258.command.impl.set;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.context.RedisContext;


import java.util.List;

import static site.hnfy258.protocal.BulkString.NULL_BYTES;

public class Spop implements Command {
    private RedisContext redisContext;
    private RedisBytes key;
    private int count = 1;

    public Spop(RedisContext redisContext) {
        this.redisContext = redisContext;
    }
    @Override
    public CommandType getType() {
        return CommandType.SPOP;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString)array[1]).getContent();
        if(array.length > 2){
            try{
                count = Integer.parseInt(((BulkString)array[2]).getContent().getString());
            }catch (Exception e){
               count=1;
            }
        }
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisContext.get(key);        if(redisData == null){
            if(count == 1){
                return new BulkString((RedisBytes)null);
            }
            return RespArray.EMPTY;
        }

        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;            if(redisSet.size() == 0){
                if(count == 1) return new BulkString((RedisBytes)null);
                return RespArray.EMPTY;
            }

            List<RedisBytes> poppedElements = redisSet.pop(count);            if(poppedElements.isEmpty()){
                return RespArray.EMPTY;
            }
            redisContext.put(key,redisSet);
            
            if(count == 1 && poppedElements.size() == 1) {
                return new BulkString(poppedElements.get(0));
            }
            
            return new RespArray(poppedElements.stream().map(BulkString::new).toArray(Resp[]::new));
        }
        return new Errors("命令执行失败");
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

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
import site.hnfy258.server.core.RedisCore;

import java.util.List;

import static site.hnfy258.protocal.BulkString.NULL_BYTES;

public class Spop implements Command {
    private RedisCore redisCore;
    private RedisBytes key;
    private int count = 1;

    public Spop(RedisCore redisCore) {
        this.redisCore = redisCore;
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
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            if(count == 1){
                return new BulkString((RedisBytes)null);
            }
            return new RespArray(new Resp[0]);
        }

        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;

            if(redisSet.size() == 0){
                if(count == 1) return new BulkString((RedisBytes)null);
                return new RespArray(new Resp[0]);
            }

            List<RedisBytes> poppedElements = redisSet.pop(count);
            if(poppedElements.isEmpty()){
                return new RespArray(new Resp[0]);
            }
            redisCore.put(key,redisSet);
            
            if(count == 1 && poppedElements.size() == 1) {
                return new BulkString(poppedElements.get(0));
            }
            
            return new RespArray(poppedElements.stream().map(BulkString::new).toArray(Resp[]::new));
        }
        return new Errors("命令执行失败");
    }
}

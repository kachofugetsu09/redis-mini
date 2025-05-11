package site.hnfy258.command.impl.list;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.server.core.RedisCore;

public class Lpop implements Command {
    private RedisCore redisCore;
    private RedisBytes key;

    public Lpop(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LPOP;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length <2){
            throw new RuntimeException("参数错误");
        }
        key = ((BulkString)array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);

        if(redisData == null) return new BulkString((RedisBytes)null);
        if(redisData instanceof RedisList){
            RedisList list = (RedisList) redisData;
            RedisBytes lpop = list.lpop();

            redisCore.put(key, list);
            return new BulkString(lpop);
        }
        return new Errors("命令执行失败");


    }
}

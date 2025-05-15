package site.hnfy258.command.impl.zset;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.core.RedisCore;

import java.util.ArrayList;
import java.util.List;

public class Zadd implements Command {
    private RedisCore redisCore;
    private RedisBytes key;
    private List<Double> scores;
    private List<Object> members;

    public Zadd(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.ZADD;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length < 4 || (array.length - 2)%2 != 0){
            throw new IllegalStateException("参数不足");
        }

        key = ((BulkString)array[1]).getContent();
        scores = new ArrayList<>();
        members = new ArrayList<>();

        for(int i = 2; i <array.length;i+=2){
            RedisBytes bytes = ((BulkString)array[i]).getContent();
            scores.add(Double.parseDouble(bytes.getString()));
            members.add(((BulkString)array[i+1]).getContent());
        }

    }

    @Override
    public Resp handle() {
        try{
            RedisZset zset = (RedisZset) redisCore.get(key);
            if(zset == null){
                zset = new RedisZset();
                redisCore.put(key,zset);
            }
            int count=0;
            for(int i=0;i<scores.size();i++){
                if(zset.add(scores.get(i),members.get(i))){
                    count++;
                }
            }
            return new RespInteger(count);
        }catch(Exception e){
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

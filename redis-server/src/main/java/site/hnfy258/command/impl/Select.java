package site.hnfy258.command.impl;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;


public class Select implements Command {
    private RedisContext redisContext;  // 新增：RedisContext支持
    private int dbIndex;

    public Select(RedisContext redisContext) {
        this.redisContext = redisContext;
    }
    @Override
    public CommandType getType() {
        return CommandType.SELECT;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length == 2){
            dbIndex = Integer.parseInt(((BulkString)array[1]).getContent().getString());
        }
        else{
            throw new IllegalStateException("参数错误");
        }
    }

    @Override
    public Resp handle() {
        try{
            redisContext.selectDB(dbIndex);
            return new RespInteger(1);
        }catch(Exception e){
            return new Errors("参数错误");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}

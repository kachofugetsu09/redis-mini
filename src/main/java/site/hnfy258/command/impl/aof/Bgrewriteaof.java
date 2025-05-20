package site.hnfy258.command.impl.aof;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.core.RedisCore;

public class Bgrewriteaof implements Command {
    private RedisCore redisCore;

    public Bgrewriteaof(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.BGREWRITEAOF;
    }

    @Override
    public void setContext(Resp[] array) {

    }

    @Override
    public Resp handle() {
        try{
            boolean result = redisCore.getServer().getAofManager().getAofWriter().bgrewrite();
            if(result){
                return new SimpleString("Background append only file rewriting started");
            }
            return new Errors("Background append only file rewriting failed");
        }catch(Exception e){
            return new Errors("Background append only file rewriting failed");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

package site.hnfy258.command.impl;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.RedisServer;
import site.hnfy258.rdb.RdbManager;

public class Bgsave implements Command {
    private RedisCore redisCore;

    public Bgsave(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.BGSAVE;
    }

    @Override
    public void setContext(Resp[] array) {

    }

    @Override
    public Resp handle() {
        boolean result = redisCore.getServer().getRdbManager().saveRdb();
        if(result) {
            return new SimpleString("OK");
        }
        return new Errors("RDB持久化失败");
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

package site.hnfy258.command.impl.server;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.server.context.RedisContext;

public class Dbsize implements Command {
    private final RedisContext context;

    public Dbsize(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.DBSIZE;
    }

    @Override
    public void setContext(Resp[] array) {
        // No arguments needed for DBSIZE command
    }

    @Override
    public Resp handle() {
        return new RespInteger(context.keys().size());
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
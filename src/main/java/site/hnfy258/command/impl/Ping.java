package site.hnfy258.command.impl;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class Ping implements Command {
    @Override
    public CommandType getType() {
        return CommandType.PING;
    }

    @Override
    public void setContext(Resp[] array) {
        //不需要内容
    }

    @Override
    public Resp handle() {
        return new SimpleString("PONG");
    }
}

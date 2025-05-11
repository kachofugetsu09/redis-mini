package site.hnfy258.server.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

@Slf4j
@Getter
@Sharable
public class RespCommandHandler extends SimpleChannelInboundHandler<Resp> {

    private final RedisCore redisCore;
    public RespCommandHandler(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) throws Exception {
        if(msg instanceof RespArray){
            RespArray respArray = (RespArray) msg;
            Resp response = processCommand(respArray);

            if(response!=null){
                ctx.channel().writeAndFlush(response);
            }
        }else{
            ctx.channel().writeAndFlush(new Errors("不支持的命令"));
        }
    }

    private Resp processCommand(RespArray respArray) {
        if(respArray.getContent().length==0){
            return new Errors("命令不能为空");
        }

        try{
            Resp[] array = respArray.getContent();
            RedisBytes cmd = ((BulkString)array[0]).getContent();
            String commandName = cmd.getString().toUpperCase();
            CommandType commandType;

            try{
                commandType = CommandType.valueOf(commandName);
            }catch (IllegalArgumentException e){
                return new Errors("命令不存在");
            }

            Command command = commandType.getSupplier().apply(redisCore);
            command.setContext(array);
            Resp result = command.handle();

            return result;
            }catch (Exception e){
                log.error("命令执行失败",e);
                return new Errors("命令执行失败");
            }
        }
}


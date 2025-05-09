package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.Resp;
@Slf4j
public class RespEncoder extends MessageToByteEncoder<Resp> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Resp msg, ByteBuf out) throws Exception {
        try{
            msg.encode(msg, out);
        }catch(Exception e){
            log.error("encode error");
            ctx.channel().close();
        }
    }
}

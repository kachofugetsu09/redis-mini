package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.Resp;

@Slf4j
public class RespEncoder extends MessageToByteEncoder<Resp> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Resp msg, ByteBuf out) throws Exception {
        try {
            // 这里不需要释放out，因为它是由Netty管理的
            msg.encode(msg, out);
        } catch(Exception e) {
            log.error("编码错误: {}", e.getMessage());
            // 在发生异常时，确保消息被正确释放
            ReferenceCountUtil.release(msg);
            ctx.channel().close();
        }
    }

}

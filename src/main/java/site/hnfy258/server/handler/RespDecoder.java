package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.Resp;

import java.util.List;
@Slf4j
public class RespDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try{
            if(in.readableBytes() >0){
                in.markReaderIndex();
            }

            if(in.readableBytes() < 4){
                return;
            }

            try{
                Resp resp = Resp.decode(in);
                if(resp != null){
                    log.debug("decode resp:{}", resp);
                    out.add(resp);
                }
            }catch(Exception e){
                log.error("decode error");
                in.resetReaderIndex();
                return;
            }
        }catch(Exception e){
            log.error("decode error", e);
        }
    }
}

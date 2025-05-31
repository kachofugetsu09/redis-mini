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
            while(in.readableBytes() >0){
                in.markReaderIndex();
                if(in.readableBytes() < 4){
                    return;
                }

                try{
                    byte firstByte = in.getByte(in.readerIndex());
                    if(firstByte == '\n' || firstByte == '\r'){
                        in.readByte();
                        log.debug("跳过无效换行符");
                        continue;
                    }
                    if(firstByte != '+' &&firstByte != '-'&&firstByte != ':' && firstByte != '$' && firstByte != '*' ){
                        in.readByte();
                        log.debug("跳过无效RESP类型标识符: {}", (char) firstByte);
                        continue;
                    }

                    Resp resp = Resp.decode(in);
                    log.debug("解码到RESP对象: {}", resp);
                    out.add(resp);
                    return;
                }catch(Exception e){
                    in.resetReaderIndex();
                    in.readByte();
                    log.debug("解码失败，可能是RESP格式错误，跳过一个字节: {}", e.getMessage());
                }

            }
        }catch(Exception e){
            log.error("decode error", e);
        }
    }
}

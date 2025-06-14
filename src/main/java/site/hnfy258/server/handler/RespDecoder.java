package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.Resp;

import java.util.List;

@Slf4j
public class RespDecoder extends ByteToMessageDecoder {
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            while (in.readableBytes() > 0) {
                in.markReaderIndex();
                
                // 确保至少有一个字节可读
                if (in.readableBytes() < 1) {
                    return;
                }

                byte firstByte = in.getByte(in.readerIndex());
                
                // 跳过无效字符
                if (firstByte == '\n' || firstByte == '\r') {
                    in.skipBytes(1);
                    continue;
                }

                // 验证RESP类型标识符
                if (!isValidRespType(firstByte)) {
                    in.skipBytes(1);
                    log.debug("跳过无效RESP类型标识符: {}", (char) firstByte);
                    continue;
                }

                try {
                    // 尝试解码RESP对象
                    Resp resp = Resp.decode(in);
                    if (resp != null) {
                        out.add(resp);
                        return;
                    }
                } catch (Exception e) {
                    // 解码失败时重置读取位置并跳过一个字节
                    in.resetReaderIndex();
                    in.skipBytes(1);
                    log.debug("解码失败，跳过一个字节: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("解码过程发生错误: {}", e.getMessage());
            // 发生异常时释放ByteBuf
            ReferenceCountUtil.release(in);
            ctx.close();
        }
    }

    private boolean isValidRespType(byte b) {
        return b == '+' || b == '-' || b == ':' || b == '$' || b == '*';
    }

}

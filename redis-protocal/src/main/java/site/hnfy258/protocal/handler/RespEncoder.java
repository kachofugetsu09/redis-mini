package site.hnfy258.protocal.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

/**
 * RESP协议编码器 - 零拷贝优化版本
 *
 * @author hnfy258
 */
@Slf4j
public class RespEncoder extends MessageToByteEncoder<Resp> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Resp msg, ByteBuf out) throws Exception {
        try {
            // 预估并确保 ByteBuf 有足够容量
            final int estimatedSize = estimateMessageSize(msg);
            out.ensureWritable(estimatedSize);

            // 直接编码到输出 ByteBuf，避免额外拷贝
            msg.encode(msg, out);
            log.debug("成功编码RESP响应: {} (大小: {} bytes)",
                    msg.getClass().getSimpleName(), out.readableBytes());
        } catch (Exception e) {
            log.error("编码错误: {}", e.getMessage(), e);
            // 在发生异常时，确保消息被正确释放
            ReferenceCountUtil.release(msg);
            ctx.channel().close();
        }
    }

    /**
     * 估算 RESP 消息编码后的大小，用于 ByteBuf 预分配优化
     *
     * @param msg RESP 消息
     * @return 估算的编码大小（字节数）
     */
    private static int estimateMessageSize(final Resp msg) {
        if (msg instanceof BulkString) {
            final BulkString bulkString = (BulkString) msg;
            if (bulkString.getContent() != null) {
                return bulkString.getContent().length() + 20; // 内容 + 协议开销
            } else {
                return 10; // "$-1\r\n"
            }
        } else if (msg instanceof RespArray) {
            final RespArray array = (RespArray) msg;
            final Resp[] content = array.getContent();
            int totalSize = 10; // '*' + length + '\r\n'
            for (final Resp element : content) {
                totalSize += estimateMessageSize(element);
            }
            return totalSize;
        } else {
            return 64; // 其他类型的默认估算
        }
    }

    /**
     * 处理编码异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("RespEncoder异常: {}", cause.getMessage(), cause);
        ctx.close();
    }
}

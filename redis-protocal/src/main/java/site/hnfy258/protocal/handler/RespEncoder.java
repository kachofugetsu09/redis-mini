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
 * RESP协议编码器
 * 
 * <p>负责将Redis数据类型编码为RESP协议格式。基于Netty的MessageToByteEncoder设计，
 * 实现高效的编码功能，支持零拷贝优化和内存预分配策略。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>高效编码 - 支持所有RESP数据类型的编码</li>
 *     <li>内存优化 - 使用ByteBuf预分配和零拷贝技术</li>
 *     <li>资源管理 - 安全的ByteBuf释放机制</li>
 *     <li>异常处理 - 完善的错误处理和资源清理</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>大小预估 - 通过estimateMessageSize预分配合适的缓冲区</li>
 *     <li>零拷贝 - 直接写入ByteBuf避免中间复制</li>
 *     <li>资源池化 - 使用Netty的ByteBuf池化分配器</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
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
     * 估算RESP消息编码后的大小
     * 
     * <p>根据不同的RESP类型预估编码后的字节数，用于ByteBuf的预分配优化。
     * 通过准确的大小预估，减少ByteBuf的扩容操作，提升编码性能。
     * 
     * @param msg RESP消息对象
     * @return 估算的编码大小（字节数）
     */
    private static int estimateMessageSize(final Resp msg) {
        if (msg == null) {
            return 5; // "*-1\r\n" for null array
        }
        
        if (msg instanceof BulkString) {
            final BulkString bulkString = (BulkString) msg;
            if (bulkString.getContent() != null) {
                return bulkString.getContent().length() + 20; // 内容 + 协议开销
            } else {
                return 10; // "$-1\r\n"
            }
        } else if (msg instanceof RespArray) {
            final RespArray array = (RespArray) msg;
            if (array == RespArray.NULL) {
                return 5; // "*-1\r\n"
            }
            final Resp[] content = array.getContent();
            if (content == null) {
                return 5; // "*-1\r\n"
            }
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

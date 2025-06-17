package site.hnfy258.protocal.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.BulkString;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * RESP协议解码器，支持标准RESP格式和INLINE命令格式
 *
 * @author hnfy258
 */
@Slf4j
public class RespDecoder extends ByteToMessageDecoder {

    private static final int MAX_INLINE_LENGTH = 64 * 1024; // 64KB 最大内联命令长度

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            while (in.readableBytes() > 0) {
                in.markReaderIndex();

                // 1. 确保至少有一个字节可读
                if (in.readableBytes() < 1) {
                    in.resetReaderIndex();
                    return;
                }

                byte firstByte = in.getByte(in.readerIndex());

                // 2. 跳过前导的换行符
                if (firstByte == '\n' || firstByte == '\r') {
                    in.skipBytes(1);
                    continue;
                }

                // 3. 判断是RESP格式还是INLINE格式
                if (isValidRespType(firstByte)) {
                    // 标准RESP协议格式
                    try {
                        Resp resp = Resp.decode(in);
                        if (resp != null) {
                            out.add(resp);
                            log.debug("成功解码RESP对象: {}", resp.getClass().getSimpleName());
                            break; // 成功解码一个消息，退出循环
                        } else {
                            // 数据不完整，等待更多数据
                            in.resetReaderIndex();
                            return;
                        }
                    } catch (Exception e) {
                        in.resetReaderIndex();
                        in.skipBytes(1);
                        log.debug("RESP解码失败，跳过一个字节: {}", e.getMessage());
                    }
                } else {
                    // 4. 尝试解码INLINE格式命令
                    Resp inlineResp = decodeInlineCommand(in);
                    if (inlineResp != null) {
                        out.add(inlineResp);
                        log.debug("成功解码INLINE命令");
                        break; // 成功解码一个消息，退出循环
                    } else {
                        // 数据不完整或无效，等待更多数据或跳过
                        in.resetReaderIndex();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            log.error("解码过程发生错误: {}", e.getMessage(), e);
            ctx.close();
        }
    }

    /**
     * 解码INLINE格式命令（如：PING\r\n）
     *
     * @param in 输入缓冲区
     * @return 解码后的RESP对象，如果数据不完整返回null
     */
    private Resp decodeInlineCommand(ByteBuf in) {
        int startIndex = in.readerIndex();
        int currentIndex = startIndex;

        // 1. 查找行结束符 \r\n 或 \n
        while (currentIndex < in.writerIndex()) {
            byte b = in.getByte(currentIndex);
            if (b == '\r') {
                // 检查是否有\n跟随
                if (currentIndex + 1 < in.writerIndex() && in.getByte(currentIndex + 1) == '\n') {
                    // 找到\r\n
                    return parseInlineCommand(in, startIndex, currentIndex, 2);
                }
            } else if (b == '\n') {
                // 找到单独的\n
                return parseInlineCommand(in, startIndex, currentIndex, 1);
            }
            currentIndex++;

            // 2. 防止过长的命令
            if (currentIndex - startIndex > MAX_INLINE_LENGTH) {
                log.warn("INLINE命令过长，跳过");
                in.skipBytes(1);
                return null;
            }
        }

        // 3. 没有找到完整的行，等待更多数据
        return null;
    }

    /**
     * 解析INLINE命令内容
     *
     * @param in            输入缓冲区
     * @param startIndex    命令开始位置
     * @param endIndex      命令结束位置（不包含换行符）
     * @param lineEndLength 换行符长度（1或2）
     * @return 解析后的RESP数组对象
     */
    private Resp parseInlineCommand(ByteBuf in, int startIndex, int endIndex, int lineEndLength) {
        // 1. 提取命令字符串
        int commandLength = endIndex - startIndex;
        if (commandLength <= 0) {
            in.skipBytes(lineEndLength); // 跳过空行
            return null;
        }

        // 直接从 ByteBuf 读取，避免临时数组分配
        String commandLine;
        if (in.hasArray()) {
            // 对于堆内存 ByteBuf，直接使用底层数组
            final byte[] array = in.array();
            final int offset = in.arrayOffset() + startIndex;
            commandLine = new String(array, offset, commandLength, StandardCharsets.UTF_8).trim();
        } else {
            // 对于直接内存 ByteBuf，仍需要拷贝
            byte[] commandBytes = new byte[commandLength];
            in.getBytes(startIndex, commandBytes);
            commandLine = new String(commandBytes, StandardCharsets.UTF_8).trim();
        }

        // 2. 优化的分割：使用更高效的分割方法
        BulkString[] bulkStrings = parseCommandParts(commandLine);
        if (bulkStrings == null || bulkStrings.length == 0) {
            in.skipBytes(commandLength + lineEndLength);
            return null;
        }

        // 3. 移动读取位置
        in.skipBytes(commandLength + lineEndLength);

        log.debug("解析INLINE命令: {}", commandLine);
        return new RespArray(bulkStrings);
    }

    /**
     *  高效解析命令部分，避免正则表达式的开销
     *
     * @param commandLine 命令行
     * @return BulkString 数组
     */
    private BulkString[] parseCommandParts(String commandLine) {
        if (commandLine.isEmpty()) {
            return null;
        }

        // 手工分割
        final char[] chars = commandLine.toCharArray();
        final int length = chars.length;
        final java.util.List<String> parts = new java.util.ArrayList<>(8); // 预估容量

        StringBuilder current = new StringBuilder();
        boolean inWhitespace = true;

        for (int i = 0; i < length; i++) {
            final char c = chars[i];
            if (Character.isWhitespace(c)) {
                if (!inWhitespace && current.length() > 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                    inWhitespace = true;
                }
            } else {
                current.append(c);
                inWhitespace = false;
            }
        }

        // 添加最后一个部分
        if (current.length() > 0) {
            parts.add(current.toString());
        }

        if (parts.isEmpty()) {
            return null;
        }

        // 转换为 BulkString 数组
        final BulkString[] result = new BulkString[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            result[i] = new BulkString(parts.get(i).getBytes(StandardCharsets.UTF_8));
        }

        return result;
    }

    /**
     * 处理解码异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("RespDecoder异常: {}", cause.getMessage(), cause);
        ctx.close();
    }

    /**
     * 检查是否为有效的RESP类型标识符
     *
     * @param b 字节
     * @return 是否为有效的RESP类型标识符
     */
    private boolean isValidRespType(byte b) {
        return b == '+' || b == '-' || b == ':' || b == '$' || b == '*';
    }
}

package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

/**
 * Redis 协议批量字符串类型实现
 * 负责 RESP 批量字符串类型的编码和解码操作
 */
@Getter
public class BulkString extends Resp {
    public static final byte[] NULL_BYTES = "$-1\r\n".getBytes();
    public static final byte[] EMPTY_BULK = "$0\r\n\r\n".getBytes();
    private final RedisBytes content;

    /**
     * 构造函数：通过 RedisBytes 创建 BulkString
     * 
     * @param content RedisBytes 内容
     */
    public BulkString(final RedisBytes content) {
        this.content = content;
    }

    /**
     * 构造函数：通过字节数组创建 BulkString
     * 
     * @param content 字节数组内容
     */
    public BulkString(final byte[] content) {
        this.content = content == null ? null : new RedisBytes(content);
    }

    /**
     * 零拷贝工厂方法：用于高性能内部路径
     * 
     * <p>警告：调用者必须保证 bytes 数组不会被修改！</p>
     * 
     * @param trustedBytes 受信任的字节数组
     * @return BulkString 实例
     */
    public static BulkString wrapTrusted(final byte[] trustedBytes) {
        if (trustedBytes == null) {
            return new BulkString((RedisBytes) null);
        }
        return new BulkString(RedisBytes.wrapTrusted(trustedBytes));
    }

    /**
     * 将 BulkString 编码到 ByteBuf，使用优化的零拷贝方法
     *
     * @param resp 响应对象（接口要求但未使用）
     * @param byteBuf 写入编码数据的目标缓冲区
     */
    @Override
    public void encode(final Resp resp, final ByteBuf byteBuf) {
        if (content == null) {
            // 使用预分配的 NULL 常量，避免重复分配
            byteBuf.writeBytes(NULL_BYTES);
            return;
        }

        // 使用零拷贝路径获取字节数组
        final byte[] bytes = content.getBytesUnsafe();
        final int length = bytes.length;

        if (length == 0) {
            // 使用预分配的空字符串常量，避免重复分配
            byteBuf.writeBytes(EMPTY_BULK);
            return;
        }

        // 性能优化：预分配缓冲区空间以减少扩容操作
        final int totalSize = estimateEncodedSize(length);
        byteBuf.ensureWritable(totalSize);

        // 1. 写入 BulkString 标识符
        byteBuf.writeByte('$');
        
        // 2. 写入内容长度
        writeIntegerAsBytes(byteBuf, length);
        
        // 3. 写入分隔符
        byteBuf.writeBytes(CRLF);
        
        // 4. 零拷贝写入内容数据
        byteBuf.writeBytes(bytes);
        
        // 5. 写入结束分隔符
        byteBuf.writeBytes(CRLF);
    }

    /**
     * 估算编码后的大小，用于 ByteBuf 预分配优化
     *
     * @param contentLength 内容长度
     * @return 估算的编码大小
     */
    private static int estimateEncodedSize(final int contentLength) {
        // 计算长度数字的位数
        final int digitLength = contentLength < 10 ? 1 : 
                               contentLength < 100 ? 2 :
                               contentLength < 1000 ? 3 :
                               String.valueOf(contentLength).length();
        
        // '$' + 长度数字 + '\r\n' + 内容 + '\r\n'
        return 1 + digitLength + 2 + contentLength + 2;
    }    /**
     * 获取字符串内容
     *
     * @return 字符串内容，如果内容为空则返回 null
     */
    @Override
    public String toString() {
        return content != null ? content.getString() : null;
    }
}


package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

@Getter
public class BulkString extends Resp {
    public static final byte[] NULL_BYTES = "$-1\r\n".getBytes();
    public static final byte[] EMPTY_BULK = "$0\r\n\r\n".getBytes();
    private final RedisBytes content;

    /**
     * 
     * @param content RedisBytes 内容
     */
    public BulkString(final RedisBytes content) {
        this.content = content;
    }

    /**
     * 
     * @param content 字节数组内容
     */
    public BulkString(final byte[] content) {
        this.content = content == null ? null : new RedisBytes(content);
    }

    /**
     *  零拷贝工厂方法：用于高性能内部路径
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

    @Override
    public void encode(final Resp resp, final ByteBuf byteBuf) {
        if (content == null) {
            byteBuf.writeBytes(NULL_BYTES);
            return;
        }

        // 🚀 使用零拷贝路径获取字节数组
        final byte[] bytes = content.getBytesUnsafe();
        final int length = bytes.length;

        if (length == 0) {
            byteBuf.writeBytes(EMPTY_BULK);
            return;
        }

        byteBuf.writeByte('$');
        writeIntegerAsBytes(byteBuf, length);
        byteBuf.writeBytes(CRLF);
        byteBuf.writeBytes(bytes);
        byteBuf.writeBytes(CRLF);
    }

    /**
     * 返回 BulkString 的字符串内容
     * 
     * @return 字符串内容，如果内容为 null 则返回 null
     */
    @Override
    public String toString() {
        return content != null ? content.getString() : null;
    }
}


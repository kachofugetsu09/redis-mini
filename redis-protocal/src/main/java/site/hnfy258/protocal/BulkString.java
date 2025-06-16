package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

@Getter
public class BulkString extends Resp {
    public static final byte[] NULL_BYTES = "$-1\r\n".getBytes();
    public static final byte[] EMPTY_BULK = "$0\r\n\r\n".getBytes();
    private final RedisBytes content;

    public BulkString(RedisBytes content) {
        this.content = content;
    }

    public BulkString(byte[] content) {
        this.content = content == null ? null : new RedisBytes(content);
    }

    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        if (content == null) {
            byteBuf.writeBytes(NULL_BYTES);
            return;
        }

        byte[] bytes = content.getBytes();
        int length = bytes.length;

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
}


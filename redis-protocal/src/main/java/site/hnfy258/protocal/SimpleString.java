package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

@Getter
public class SimpleString extends Resp {
    private final String content;
    private final RedisBytes contentBytes; // 缓存字节表示

    public SimpleString(String content) {
        this.content = content;
        this.contentBytes = RedisBytes.fromString(content);
    }

    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte('+');
        final SimpleString simpleString = (SimpleString) resp;
        byteBuf.writeBytes(simpleString.contentBytes.getBytesUnsafe());
        byteBuf.writeBytes(CRLF);
    }
}

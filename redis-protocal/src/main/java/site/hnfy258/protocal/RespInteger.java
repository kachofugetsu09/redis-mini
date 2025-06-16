package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class RespInteger extends Resp {
    private int content;

    public RespInteger(int content) {
        this.content = content;
    }    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte(':');
        writeIntegerAsBytes(byteBuf, ((RespInteger) resp).getContent());
        byteBuf.writeBytes(CRLF);
    }
}

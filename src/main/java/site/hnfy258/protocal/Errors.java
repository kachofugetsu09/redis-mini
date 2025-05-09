package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class Errors extends Resp {
    private final String content;

    public Errors(String content) {
        this.content = content;
    }
    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte('-');
        byteBuf.writeBytes(((Errors) resp).getContent().getBytes());
        byteBuf.writeBytes(CRLF);
    }
}

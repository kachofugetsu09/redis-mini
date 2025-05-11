package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class RespArray extends Resp{
    private final Resp[] content;

    public RespArray(Resp[] content) {
        this.content = content;
    }


    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte('*');
        byteBuf.writeBytes(Integer.toString(content.length).getBytes());
        byteBuf.writeBytes(CRLF);
        for(Resp r : content){
            r.encode(r, byteBuf);
        }
    }
}

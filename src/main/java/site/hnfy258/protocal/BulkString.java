package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class BulkString extends Resp{
    private static final byte[] NULL_BYTES = "-1\r\n".getBytes();
    private static final byte[] EMPTY_BYTES = "0\r\n\r\n".getBytes();
    private final byte[] content;

    public BulkString(byte[] content) {
        this.content = content;
    }
    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        //BulkString "$6\r\nfoobar\r\n"
        byteBuf.writeByte('$');
        if(content == null){
            byteBuf.writeBytes(NULL_BYTES);
        }
        else{
            int length = content.length;

            if(length == 0){
                byteBuf.writeBytes(EMPTY_BYTES);
            }
            else{
                byteBuf.writeBytes(String.valueOf(length).getBytes());
                byteBuf.writeBytes(content);
                byteBuf.writeBytes(CRLF);
            }
            }
        }
    }


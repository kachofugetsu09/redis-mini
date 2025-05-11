package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

@Getter
public class BulkString extends Resp{
    public static final byte[] NULL_BYTES = "-1\r\n".getBytes();
    public static final byte[] EMPTY_BYTES = "0\r\n\r\n".getBytes();
    private final RedisBytes content;

    public BulkString(RedisBytes content) {
        this.content = content;
    }

    public BulkString(byte[] content) {
        this.content = content == null ? null : new RedisBytes(content);
    }
    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        //BulkString "$6\r\nfoobar\r\n"
        byteBuf.writeByte('$');
        if(content == null){
            byteBuf.writeBytes(NULL_BYTES);
        }
        else{
            int length  = content.getBytes().length;

            if(length == 0){
                byteBuf.writeBytes(EMPTY_BYTES);
            }
            else{
                byteBuf.writeBytes(String.valueOf(length).getBytes());
                byteBuf.writeBytes(CRLF);
                byteBuf.writeBytes(content.getBytes());
                byteBuf.writeBytes(CRLF);
            }
            }
        }
    }


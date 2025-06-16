package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Resp {
    public static final byte[] CRLF = "\r\n".getBytes();
    protected static final byte[][] NUMBERS = new byte[512][]; // 缓存数字的字节表示
    protected static final int MAX_CACHED_NUMBER = 255;
    
    // 静态初始化数字缓存
    static {
        for (int i = 0; i <= MAX_CACHED_NUMBER; i++) {
            NUMBERS[i] = String.valueOf(i).getBytes();
        }
        // 缓存负数
        for (int i = 1; i <= MAX_CACHED_NUMBER; i++) {
            NUMBERS[i + 256] = String.valueOf(-i).getBytes();
        }
    }
    
    // 写入数字的优化方法
    protected static void writeIntegerAsBytes(ByteBuf buf, int value) {
        if (value >= 0 && value <= MAX_CACHED_NUMBER) {
            buf.writeBytes(NUMBERS[value]);
        } else if (value < 0 && value >= -MAX_CACHED_NUMBER) {
            buf.writeBytes(NUMBERS[-value + 256]);
        } else {
            buf.writeBytes(String.valueOf(value).getBytes());
        }
    }

    //SimpleString "+OK\r\n"
    //Errors "-Error message\r\n"
    //RedisInteger :0\r\n
    //BulkString "$6\r\nfoobar\r\n"
    //RespArray "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    public static Resp decode(ByteBuf buffer){
        //判断是不是一个完整的命令
        if(buffer.readableBytes() <=0){
            throw new RuntimeException("没有一个完整的命令");
        }

        //拿到符号
        char c = (char)buffer.readByte();
        switch (c){
            case '+':
                return new SimpleString(getString(buffer));
            case '-':
                return new Errors(getString(buffer));
            case ':':
                return new RespInteger(getNumber(buffer));
            case '$':
                int length = getNumber(buffer);
                if(buffer.readableBytes() < length+2){
                    throw new IllegalStateException("没有找到换行符");
                }

                byte[] content;
                if(length == -1){
                    content = null;
                }else{
                    content = new byte[length];
                    buffer.readBytes(content);
                }
                if(buffer.readByte() != '\r' || buffer.readByte() != '\n'){
                    throw new IllegalStateException("没有找到换行符");
                }

                return new BulkString(content);
            case '*':
                int number = getNumber(buffer);
                Resp[] array = new Resp[number];
                for(int i=0;i<number;i++){
                    array[i] = decode(buffer);
                }
                return new RespArray(array);

            default:
                throw new IllegalStateException("不是Resp类型");
        }
    }

    public abstract void encode(Resp resp, ByteBuf byteBuf);

    //Errors "-Error message\r\n"
    static String getString(ByteBuf buffer){
        char c;
        StringBuilder result = new StringBuilder();
        while((c = (char)buffer.readByte()) != '\r' && buffer.readableBytes()>0){
            result.append(c);
        }
        if(buffer.readableBytes()<=0 || buffer.readByte() != '\n'){
            throw new IllegalStateException("没有找到换行符");
        }
        return result.toString();
    }

    static int getNumber(ByteBuf buffer){
        char c;
        c = (char)buffer.readByte();
        boolean positive = true;
        int value = 0;
        if(c == '-'){
            positive = false;
        }
        else{
            value = c - '0';
        }
        while((c = (char)buffer.readByte()) != '\r' && buffer.readableBytes()>0){
            value = value*10 + (c - '0');
        }
        if(buffer.readableBytes()<=0 || buffer.readByte() != '\n'){
            throw new IllegalStateException("没有找到换行符");
        }
        if(!positive){
            value = -value;
        }
        return value;

    }
}
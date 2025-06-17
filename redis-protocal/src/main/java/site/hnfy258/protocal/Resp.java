package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis 协议基础类
 * 负责 RESP 协议的基础功能实现，包括编码、解码和工具方法
 */
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
    
    /**
     * 优化的数字写入方法，使用缓存避免重复的字节数组创建
     * 
     * @param buf 目标缓冲区
     * @param value 要写入的整数值
     */
    protected static void writeIntegerAsBytes(ByteBuf buf, int value) {
        if (value >= 0 && value <= MAX_CACHED_NUMBER) {
            buf.writeBytes(NUMBERS[value]);
        } else if (value < 0 && value >= -MAX_CACHED_NUMBER) {
            buf.writeBytes(NUMBERS[-value + 256]);
        } else {
            buf.writeBytes(String.valueOf(value).getBytes());
        }
    }

    /**
     * RESP 协议解码方法
     * 支持的类型：
     * - SimpleString "+OK\r\n"
     * - Errors "-Error message\r\n"
     * - RedisInteger ":0\r\n"
     * - BulkString "$6\r\nfoobar\r\n"
     * - RespArray "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
     * 
     * @param buffer 输入缓冲区
     * @return 解码后的 Resp 对象
     */
    public static Resp decode(ByteBuf buffer){
        // 判断是不是一个完整的命令
        if(buffer.readableBytes() <=0){
            throw new RuntimeException("没有一个完整的命令");
        }

        // 拿到符号
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
                if (length == -1) {
                    // NULL BulkString
                    if (buffer.readableBytes() < 2) {
                        throw new IllegalStateException("没有找到换行符");
                    }
                    if (buffer.readByte() != '\r' || buffer.readByte() != '\n') {
                        throw new IllegalStateException("没有找到换行符");
                    }
                    return new BulkString((byte[]) null);
                }
                
                if (buffer.readableBytes() < length + 2) {
                    throw new IllegalStateException("数据不完整");
                }

                byte[] content;
                // 零拷贝优化：直接从 ByteBuf 获取数组引用
                if (buffer.hasArray() && length > 0) {
                    // 1. 如果 ByteBuf 有底层数组，使用系统级拷贝
                    final int startIndex = buffer.arrayOffset() + buffer.readerIndex();
                    final byte[] backingArray = buffer.array();
                    
                    // 2. 使用 System.arraycopy（JVM 内建优化）
                    content = new byte[length];
                    System.arraycopy(backingArray, startIndex, content, 0, length);
                    buffer.skipBytes(length);
                } else if (buffer.isDirect() && length >= 1024) {
                    // 3. 对于大的直接内存 ByteBuf，使用 NIO 优化
                    content = new byte[length];
                    buffer.readBytes(content);
                } else {
                    // 4. 回退到标准读取方式（小数据或堆内存）
                    content = new byte[length];
                    buffer.readBytes(content);
                }
                
                // 验证结尾的 CRLF
                if (buffer.readByte() != '\r' || buffer.readByte() != '\n') {
                    throw new IllegalStateException("没有找到换行符");
                }

                // 使用零拷贝 BulkString 构造
                return BulkString.wrapTrusted(content);
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

    /**
     * 抽象编码方法，由子类实现具体的编码逻辑
     * 
     * @param resp 响应对象
     * @param byteBuf 输出缓冲区
     */
    public abstract void encode(Resp resp, ByteBuf byteBuf);

    /**
     * 从缓冲区读取字符串直到遇到 \r\n
     * 用于解析 SimpleString 和 Error 类型
     * 
     * @param buffer 输入缓冲区
     * @return 解析出的字符串
     */
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

    /**
     * 从缓冲区读取数字直到遇到 \r\n
     * 用于解析 Integer 类型和长度信息
     * 
     * @param buffer 输入缓冲区
     * @return 解析出的数字
     */
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
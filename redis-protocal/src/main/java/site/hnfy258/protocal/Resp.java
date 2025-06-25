package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis协议基础类
 * 
 * <p>负责Redis RESP协议的核心实现，提供基础的编码和解码功能。
 * 作为所有RESP数据类型的基类，定义统一的接口和共享的工具方法。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>协议解析 - 支持所有RESP数据类型的解码</li>
 *     <li>协议编码 - 提供统一的编码接口</li>
 *     <li>性能优化 - 数字缓存和零拷贝实现</li>
 *     <li>错误处理 - 完善的异常处理机制</li>
 * </ul>
 * 
 * <p>支持的数据类型：
 * <ul>
 *     <li>简单字符串 - 以"+"开头</li>
 *     <li>错误消息 - 以"-"开头</li>
 *     <li>整数 - 以":"开头</li>
 *     <li>批量字符串 - 以"$"开头</li>
 *     <li>数组 - 以"*"开头</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>数字缓存 - 预缓存常用数字的字节表示</li>
 *     <li>零拷贝 - 支持高效的内存操作</li>
 *     <li>缓冲区管理 - 智能的ByteBuf使用策略</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public abstract class Resp {
    /** 行结束符 */
    public static final byte[] CRLF = "\r\n".getBytes();
    
    /** 数字的字节表示缓存 */
    protected static final byte[][] NUMBERS = new byte[512][];
    
    /** 最大缓存数字 */
    protected static final int MAX_CACHED_NUMBER = 255;

    private static final int PROTO_MAX_BULK_LEN = 512 * 1024 * 1024; // 最大 512MB
    private static final int PROTO_MAX_ARRAY_LEN = 1024 * 1024;
    
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
    }    /**
     * RESP 协议解码方法
     * 支持的类型：
     * - SimpleString "+OK\r\n"
     * - Errors "-Error message\r\n"
     * - RedisInteger ":0\r\n"
     * - BulkString "$6\r\nfoobar\r\n"
     * - RespArray "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
     * 
     * @param buffer 输入缓冲区
     * @return 解码后的 Resp 对象，如果数据不完整返回null
     * @throws IllegalArgumentException 当数据格式不符合RESP协议规范时
     */
    public static Resp decode(ByteBuf buffer) {
        // 1. 判断是不是有数据可读
        if (buffer.readableBytes() <= 0) {
            return null; // 数据不完整，返回null而不是抛异常
        }

        // 2. 保存初始读索引，以便出错时回滚
        final int initialIndex = buffer.readerIndex();
        try {
            // 3. 拿到类型标识符
            byte typeIndicator = buffer.readByte();
            char c = (char) typeIndicator;
            
            switch (c) {
                case '+': // 简单字符串
                    return new SimpleString(getString(buffer));
                    
                case '-': // 错误消息
                    return new Errors(getString(buffer));
                      case ':': // 整数
                    return RespInteger.valueOf(getNumber(buffer));
                    case '$': // 批量字符串
                    int length = getNumber(buffer);
                        if (length > PROTO_MAX_BULK_LEN) {
                            throw new IllegalArgumentException("协议错误：批量字符串的长度超过最大限制 " + PROTO_MAX_BULK_LEN);
                        }
                    if (length == -1) {
                        // NULL BulkString - getNumber已经消费了完整的"-1\r\n"
                        return BulkString.create((byte[]) null);
                    }
                    
                    if (buffer.readableBytes() < length + 2) {
                        throw new IllegalStateException("数据不完整：BulkString内容长度不足");
                    }

                    byte[] content;
                    boolean isZeroCopy = false;
                    
                    // 零拷贝优化：直接从 ByteBuf 获取数组引用
                    if (buffer.hasArray() && length > 0) {
                        // 如果 ByteBuf 有底层数组，可以完全零拷贝
                        final int startIndex = buffer.arrayOffset() + buffer.readerIndex();
                        final byte[] backingArray = buffer.array();
                        
                        // 优化: 创建新数组但使用直接拷贝（仍比标准实现快很多）
                        content = new byte[length];
                        System.arraycopy(backingArray, startIndex, content, 0, length);
                        buffer.skipBytes(length);
                        
                        // 我们可以使用零拷贝模式，因为这个数组是我们控制的
                    } else {
                        // 回退到标准读取方式（小数据或堆内存）
                        content = new byte[length];
                        buffer.readBytes(content);
                        // 我们可以使用零拷贝模式，因为这个数组是我们控制的
                    }
                        isZeroCopy = true;
                        // 验证结尾的 CRLF
                    if (buffer.readByte() != '\r' || buffer.readByte() != '\n') {
                        throw new IllegalArgumentException("BulkString格式错误：期望\\r\\n结尾");
                    }

                    // 使用零拷贝方式创建 BulkString，因为我们能控制 content 数组的生命周期
                    return isZeroCopy 
                            ? BulkString.wrapTrusted(content)
                            : BulkString.create(content);
                      case '*': // 数组
                    int number = getNumber(buffer);
                          if (number > PROTO_MAX_ARRAY_LEN) {
                              throw new IllegalArgumentException("协议错误：数组的元素数量超过最大限制 " + PROTO_MAX_ARRAY_LEN);
                          }
                    if (number < 0) {
                        // NULL Array - 使用缓存实例
                        return RespArray.NULL;
                    }
                    
                    if (number == 0) {
                        // Empty Array - 使用缓存实例
                        return RespArray.EMPTY;
                    }
                    
                    Resp[] array = new Resp[number];
                    for (int i = 0; i < number; i++) {
                        if (buffer.readableBytes() <= 0) {
                            throw new IllegalStateException("数组元素数据不完整");
                        }
                        Resp element = decode(buffer);
                        if (element == null) {
                            throw new IllegalStateException("数组元素数据不完整");
                        }
                        array[i] = element;
                    }
                    return new RespArray(array);                default:
                    // 4. 处理无法识别的类型标记
                    log.warn("无法识别的RESP类型标识: '{}' (字节值: {})", c, typeIndicator & 0xFF);
                    throw new IllegalArgumentException("不是有效的RESP类型标识: " + c);
            }
        } catch (IllegalStateException e) {
            // 5. 数据不完整异常，回滚读索引
            buffer.readerIndex(initialIndex);
            return null; // 返回null表示需要更多数据
        } catch (Exception e) {
            // 6. 其他异常（格式错误等），回滚读索引并重新抛出
            buffer.readerIndex(initialIndex);
            throw e;
        }
    }

    /**
     * 抽象编码方法，由子类实现具体的编码逻辑
     * 
     * @param resp 响应对象
     * @param byteBuf 输出缓冲区
     */
    public abstract void encode(Resp resp, ByteBuf byteBuf);    /**
     * 从缓冲区读取字符串直到遇到 \r\n
     * 用于解析 SimpleString 和 Error 类型
     * 
     * @param buffer 输入缓冲区
     * @return 解析出的字符串
     * @throws IllegalStateException 如果数据不完整或格式错误
     */
    static String getString(ByteBuf buffer) {
        // 1. 查找CRLF的位置
        final int startIndex = buffer.readerIndex();
        final int endIndex = buffer.indexOf(startIndex, buffer.writerIndex(), (byte) '\r');
        
        if (endIndex < 0) {
            throw new IllegalStateException("数据不完整：没有找到换行符");
        }
        
        // 2. 确保有足够的数据读取\r\n
        if (endIndex + 1 >= buffer.writerIndex()) {
            throw new IllegalStateException("数据不完整：缺少\\n");
        }
        
        // 3. 计算字符串长度并一次性读取
        final int length = endIndex - startIndex;
        String result;
        if (buffer.hasArray()) {
            // 对于堆缓冲区，直接从底层数组读取
            result = new String(buffer.array(), buffer.arrayOffset() + startIndex, length);
            buffer.readerIndex(endIndex);
        } else {
            // 对于直接缓冲区，使用临时数组
            byte[] bytes = new byte[length];
            buffer.getBytes(startIndex, bytes);
            result = new String(bytes);
            buffer.readerIndex(endIndex);
        }
        
        // 4. 校验并跳过\r\n
        if (buffer.readByte() != '\r' || buffer.readByte() != '\n') {
            throw new IllegalArgumentException("格式错误：期望\\r\\n但找到了其他字符");
        }
        
        return result;
    }    /**
     * 从缓冲区读取数字直到遇到 \r\n
     * 用于解析 Integer 类型和长度信息
     * 
     * @param buffer 输入缓冲区
     * @return 解析出的数字
     * @throws IllegalStateException 如果数据不完整或格式错误
     */
    static int getNumber(ByteBuf buffer) {
        // 1. 查找CRLF位置
        final int startIndex = buffer.readerIndex();
        final int endIndex = buffer.indexOf(startIndex, buffer.writerIndex(), (byte) '\r');
        
        if (endIndex < 0) {
            throw new IllegalStateException("数据不完整：没有找到换行符");
        }
        
        // 2. 确保有足够的数据读取\r\n
        if (endIndex + 1 >= buffer.writerIndex()) {
            throw new IllegalStateException("数据不完整：缺少\\n");
        }
          // 3. 一次性读取并解析数字
        final int length = endIndex - startIndex;
        int value = 0;
        
        if (length == 0) {
            throw new IllegalArgumentException("数字解析错误：长度为0");
        }
        
        // 检查是否以'-'开头（负数）
        if (buffer.getByte(startIndex) == '-') {
            if (length == 1) {
                throw new IllegalArgumentException("数字解析错误：只有负号");
            }
            // 直接从第二个字符开始解析数字
            for (int i = startIndex + 1; i < endIndex; i++) {
                byte b = buffer.getByte(i);
                if (b < '0' || b > '9') {
                    throw new IllegalArgumentException("数字解析错误: 包含非数字字符");
                }
                value = value * 10 + (b - '0');
            }
            value = -value;
        } else {
            // 解析正数
            for (int i = startIndex; i < endIndex; i++) {
                byte b = buffer.getByte(i);
                if (b < '0' || b > '9') {
                    throw new IllegalArgumentException("数字解析错误: 包含非数字字符");
                }
                value = value * 10 + (b - '0');
            }
        }
        
        // 4. 更新读指针并验证CRLF
        buffer.readerIndex(endIndex);
        if (buffer.readByte() != '\r' || buffer.readByte() != '\n') {
            throw new IllegalArgumentException("格式错误：期望\\r\\n但找到了其他字符");
        }
        
        return value;
    }
}
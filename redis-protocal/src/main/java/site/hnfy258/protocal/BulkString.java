package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

/**
 * Redis批量字符串类型
 * 
 * <p>负责Redis RESP协议中批量字符串类型的实现，提供高效的编码和解码功能。
 * 基于RedisBytes设计，支持零拷贝优化和内存复用，适用于大规模数据处理。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>零拷贝实现 - 通过RedisBytes提供高效的内存管理</li>
 *     <li>常量池优化 - 预分配常用命令的字符串实例</li>
 *     <li>安全性保证 - 提供数据安全的创建方式</li>
 *     <li>性能优化 - 支持直接内存访问和缓冲区复用</li>
 * </ul>
 * 
 * <p>使用建议：
 * <ul>
 *     <li>优先使用工厂方法而非构造函数</li>
 *     <li>对于常用命令，使用预定义常量</li>
 *     <li>内部解码使用wrapTrusted提升性能</li>
 *     <li>外部数据使用create确保安全性</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
public class BulkString extends Resp {
    /** 空值的RESP编码 */
    public static final byte[] NULL_BYTES = "$-1\r\n".getBytes();
    
    /** 空字符串的RESP编码 */
    public static final byte[] EMPTY_BULK = "$0\r\n\r\n".getBytes();
    
    /** 预分配的常用命令实例 */
    public static final BulkString SET = BulkString.wrapTrusted("SET".getBytes());
    public static final BulkString GET = BulkString.wrapTrusted("GET".getBytes());
    public static final BulkString DEL = BulkString.wrapTrusted("DEL".getBytes());
    public static final BulkString PING = BulkString.wrapTrusted("PING".getBytes());
    public static final BulkString PONG = BulkString.wrapTrusted("PONG".getBytes());
    public static final BulkString OK = BulkString.wrapTrusted("OK".getBytes());
    
    /** 字符串内容的字节表示 */
    private final RedisBytes content;

    /**
     * RedisBytes构造函数
     * 
     * @param content RedisBytes 内容
     */
    public BulkString(final RedisBytes content) {
        this.content = content;
    }

    /**
     * 创建BulkString的工厂方法：安全模式
     * 该方法会复制输入的字节数组，确保安全性
     * 
     * @param content 字节数组内容
     * @return 安全的BulkString实例
     */
    public static BulkString create(final byte[] content) {
        if (content == null) {
            return new BulkString((RedisBytes) null);
        }
        
        // 创建一个新的RedisBytes并复制内容，确保安全性
        return new BulkString(new RedisBytes(content));
    }

    /**
     * 创建BulkString的工厂方法：基于RedisBytes
     * 
     * @param content RedisBytes内容
     * @return BulkString实例
     */
    public static BulkString create(final RedisBytes content) {
        return new BulkString(content);
    }

    /**
     * 零拷贝工厂方法：用于高性能内部路径
     * 
     * <p>警告：调用者必须保证bytes数组不会被修改！该方法不会复制输入数组。</p>
     * <p>仅在内部解码器、序列化器等可信代码中使用。</p>
     * 
     * @param trustedBytes 受信任的字节数组，不会被调用者修改
     * @return 零拷贝的BulkString实例
     */
    public static BulkString wrapTrusted(final byte[] trustedBytes) {
        if (trustedBytes == null) {
            return new BulkString((RedisBytes) null);
        }
        return new BulkString(RedisBytes.wrapTrusted(trustedBytes));
    }

    /**
     * 从字符串创建BulkString的便捷工厂方法
     * 
     * @param str 字符串内容
     * @return BulkString实例
     */
    public static BulkString fromString(final String str) {
        if (str == null) {
            return new BulkString((RedisBytes) null);
        }
        return new BulkString(RedisBytes.fromString(str));
    }

    /**
     * 字节数组构造函数
     * 
     * <p>注意：此构造函数会复制输入的字节数组以确保安全性。</p>
     * <p>如果你能够保证字节数组不会被外部修改，推荐使用 {@link #wrapTrusted(byte[])} 以获得更好的性能。</p>
     * 
     * @param content 字节数组内容，会被复制
     */
    public BulkString(final byte[] content) {
        this.content = content == null ? null : new RedisBytes(content);
    }

    /**
     * 将 BulkString 编码到 ByteBuf，使用优化的零拷贝方法
     *
     * @param resp 响应对象（接口要求但未使用）
     * @param byteBuf 写入编码数据的目标缓冲区
     */
    @Override
    public void encode(final Resp resp, final ByteBuf byteBuf) {
        if (content == null) {
            // 使用预分配的 NULL 常量，避免重复分配
            byteBuf.writeBytes(NULL_BYTES);
            return;
        }

        // 使用零拷贝路径获取字节数组
        final byte[] bytes = content.getBytesUnsafe();
        final int length = bytes.length;

        if (length == 0) {
            // 使用预分配的空字符串常量，避免重复分配
            byteBuf.writeBytes(EMPTY_BULK);
            return;
        }

        // 性能优化：预分配缓冲区空间以减少扩容操作
        final int totalSize = estimateEncodedSize(length);
        byteBuf.ensureWritable(totalSize);

        // 1. 写入 BulkString 标识符
        byteBuf.writeByte('$');
        
        // 2. 写入内容长度
        writeIntegerAsBytes(byteBuf, length);
        
        // 3. 写入分隔符
        byteBuf.writeBytes(CRLF);
        
        // 4. 零拷贝写入内容数据
        byteBuf.writeBytes(bytes);
        
        // 5. 写入结束分隔符
        byteBuf.writeBytes(CRLF);
    }

    /**
     * 估算编码后的大小，用于 ByteBuf 预分配优化
     *
     * @param contentLength 内容长度
     * @return 估算的编码大小
     */
    private static int estimateEncodedSize(final int contentLength) {
        // 计算长度数字的位数
        final int digitLength = contentLength < 10 ? 1 : 
                               contentLength < 100 ? 2 :
                               contentLength < 1000 ? 3 :
                               String.valueOf(contentLength).length();
        
        // '$' + 长度数字 + '\r\n' + 内容 + '\r\n'
        return 1 + digitLength + 2 + contentLength + 2;
    }
    
    /**
     * 获取字符串内容
     *
     * @return 字符串内容，如果内容为空则返回 null
     */
    @Override
    public String toString() {
        return content != null ? content.getString() : null;
    }
}


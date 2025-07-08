package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.io.Serializable;

/**
 * Redis数组类型
 * 
 * <p>负责Redis RESP协议中数组类型的实现，提供高效的数组操作和编码功能。
 * 基于享元模式设计，通过预分配常用数组实例来优化性能和内存使用。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>数组编码 - 高效的RESP格式编码</li>
 *     <li>内存优化 - 预分配常用数组实例</li>
 *     <li>大小预估 - 智能的缓冲区分配</li>
 *     <li>空值处理 - 统一的null数组处理</li>
 * </ul>
 * 
 * <p>预定义实例：
 * <ul>
 *     <li>EMPTY - 空数组，对应"*0\r\n"</li>
 *     <li>NULL - null数组，对应"*-1\r\n"</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>常量复用 - 使用预定义字节数组</li>
 *     <li>大小预估 - 准确的缓冲区预分配</li>
 *     <li>内存管理 - 高效的数组元素编码</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
public class RespArray extends Resp implements Serializable {
    /** null数组的RESP编码 */
    private static final byte[] NULL_ARRAY_BYTES = "*-1\r\n".getBytes();
    
    /** 空数组的RESP编码 */
    private static final byte[] EMPTY_ARRAY_BYTES = "*0\r\n".getBytes();
    
    /** 预定义的空数组实例 */
    public static final RespArray EMPTY = new RespArray(new Resp[0]);
    
    /** 预定义的null数组实例 */
    public static final RespArray NULL = new RespArray((Resp[]) null);
    
    /** 数组内容 */
    private final Resp[] content;

    /**
     * 构造函数
     * 
     * @param content 数组内容
     */
    public RespArray(final Resp[] content) {
        this.content = content;
    }
    
    /**
     * 工厂方法：获取 RespArray 实例
     * 
     * <p>对于常用的数组（空数组、null数组），返回缓存的实例；
     * 对于其他数组，创建新实例。</p>
     * 
     * @param content 数组内容
     * @return RespArray 实例
     */
    public static RespArray valueOf(final Resp[] content) {
        if (content == null) {
            return NULL;
        }
        if (content.length == 0) {
            return EMPTY;
        }
        return new RespArray(content);
    }
    
    /**
     * 将 RespArray 编码到 ByteBuf，使用预分配优化
     *
     * @param resp 响应对象（接口要求但未使用）
     * @param byteBuf 写入编码数据的目标缓冲区
     */
    @Override
    public void encode(final Resp resp, final ByteBuf byteBuf) {
        final RespArray respArray = (RespArray) resp;
        final Resp[] arrayContent = respArray.getContent();
          // 1. 处理null数组
        if (arrayContent == null) {
            byteBuf.writeBytes(NULL_ARRAY_BYTES);
            return;
        }
        
        // 2. 处理空数组 - 使用预分配的字节数组避免重复计算
        if (arrayContent.length == 0) {
            byteBuf.writeBytes(EMPTY_ARRAY_BYTES);
            return;
        }
        
        // 3. 预估所需容量并预分配
        final int estimatedSize = estimateEncodedSize(arrayContent);
        byteBuf.ensureWritable(estimatedSize);
        
        // 4. 写入数组标识符
        byteBuf.writeByte('*');
        
        // 5. 写入数组长度
        writeIntegerAsBytes(byteBuf, arrayContent.length);
        
        // 6. 写入分隔符
        byteBuf.writeBytes(CRLF);
        
        // 7. 编码所有数组元素
        for (final Resp element : arrayContent) {
            element.encode(element, byteBuf);
        }
    }
    
    /**
     * 估算编码后的大小，用于 ByteBuf 预分配优化
     *
     * @param arrayContent 数组内容
     * @return 估算的编码大小（字节数）
     */
    private static int estimateEncodedSize(final Resp[] arrayContent) {
        if (arrayContent == null) {
            return 6; // "*-1\r\n"
        }
        
        if (arrayContent.length == 0) {
            return 5; // "*0\r\n"
        }
        
        // 计算数组长度数字的位数
        final int arrayLengthDigits = arrayContent.length < 10 ? 1 : 
                                     arrayContent.length < 100 ? 2 :
                                     String.valueOf(arrayContent.length).length();
        
        // '*' + 数组长度 + '\r\n'
        int totalSize = 1 + arrayLengthDigits + 2;
        
        // 估算每个元素的大小
        for (final Resp element : arrayContent) {
            if (element instanceof BulkString) {
                final BulkString bulkString = (BulkString) element;
                if (bulkString.getContent() != null) {
                    // 内容长度 + 协议开销估算
                    totalSize += bulkString.getContent().length() + 10;
                } else {
                    // "$-1\r\n" 的长度
                    totalSize += 5;
                }
            } else {
                // 其他类型的默认估算
                totalSize += 50;
            }
        }
        
        return totalSize;
    }
}

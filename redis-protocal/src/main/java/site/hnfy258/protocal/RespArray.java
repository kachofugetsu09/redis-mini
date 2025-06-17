package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * Redis 协议数组类型实现
 * 负责 RESP 数组类型的编码和解码操作
 */
@Getter
public class RespArray extends Resp {
    private final Resp[] content;

    public RespArray(Resp[] content) {
        this.content = content;
    }

    /**
     * 将 RespArray 编码到 ByteBuf，使用预分配优化
     *
     * @param resp 响应对象（接口要求但未使用）
     * @param byteBuf 写入编码数据的目标缓冲区
     */
    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        // 预估所需容量并预分配
        final int estimatedSize = estimateEncodedSize();
        byteBuf.ensureWritable(estimatedSize);
        
        // 1. 写入数组标识符
        byteBuf.writeByte('*');
        
        // 2. 写入数组长度
        writeIntegerAsBytes(byteBuf, content.length);
        
        // 3. 写入分隔符
        byteBuf.writeBytes(CRLF);
        
        // 4. 编码所有数组元素
        for (final Resp element : content) {
            element.encode(element, byteBuf);
        }
    }

    /**
     * 估算编码后的大小，用于 ByteBuf 预分配优化
     *
     * @return 估算的编码大小（字节数）
     */
    private int estimateEncodedSize() {
        // 计算数组长度数字的位数
        final int arrayLengthDigits = content.length < 10 ? 1 : 
                                     content.length < 100 ? 2 :
                                     String.valueOf(content.length).length();
        
        // '*' + 数组长度 + '\r\n'
        int totalSize = 1 + arrayLengthDigits + 2;
        
        // 估算每个元素的大小
        for (final Resp element : content) {
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

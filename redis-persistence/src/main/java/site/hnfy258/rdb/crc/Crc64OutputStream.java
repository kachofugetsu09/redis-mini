package site.hnfy258.rdb.crc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * CRC64计算输出流包装器
 * 
 * <p>该类包装DataOutputStream，在数据写入的同时计算CRC64校验和。
 * 与Redis官方实现保持一致，支持增量计算校验和，确保RDB文件的
 * 数据完整性验证功能正常工作。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>透明的CRC64计算 - 所有写入操作都会自动更新校验和</li>
 *     <li>校验和隔离 - 写入校验和本身时不会影响CRC64值</li>
 *     <li>Redis兼容 - 使用与Redis相同的CRC64算法</li>
 *     <li>流式处理 - 支持大文件的增量校验和计算</li>
 * </ul>
 *
 * 
 * @author hnfy258
 * @since 1.0
 */
public class Crc64OutputStream extends OutputStream {
    
    /** 包装的DataOutputStream实例 */
    private final DataOutputStream dataOutputStream;
    
    /** 底层输出流引用，用于校验和写入时绕过CRC64计算 */
    private final OutputStream underlyingOutputStream;
    
    /** 当前计算的CRC64校验和值 */
    private long crc64 = 0L;
    
    /**
     * 构造函数
     * 
     * @param outputStream 底层输出流
     */
    public Crc64OutputStream(OutputStream outputStream) {
        this.underlyingOutputStream = outputStream;
        this.dataOutputStream = new DataOutputStream(outputStream);
    }

    /**
     * 获取包装的DataOutputStream
     * 
     * <p>返回一个新的DataOutputStream实例，该实例会通过当前对象
     * 进行写入，确保所有数据都被计入CRC64计算。
     * 
     * @return DataOutputStream实例
     */
    public DataOutputStream getDataOutputStream() {
        return new DataOutputStream(this);
    }

    /**
     * 获取当前计算的CRC64值
     * 
     * @return CRC64校验和
     */
    public long getCrc64() {
        return crc64;
    }

    /**
     * 写入单个字节
     * 
     * @param b 要写入的字节
     * @throws IOException 如果发生IO错误
     */
    @Override
    public void write(int b) throws IOException {
        // 更新CRC64
        byte[] bytes = {(byte) b};
        crc64 = Crc64.crc64(crc64, bytes, 0, 1);
          // 写入底层流
        dataOutputStream.write(b);
    }

    /**
     * 写入字节数组
     * 
     * @param b 要写入的字节数组
     * @throws IOException 如果发生IO错误
     */
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * 写入字节数组的指定部分
     * 
     * @param b 要写入的字节数组
     * @param off 起始偏移量
     * @param len 写入长度
     * @throws IOException 如果发生IO错误
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // 更新CRC64
        crc64 = Crc64.crc64(crc64, b, off, len);
          // 写入底层流
        dataOutputStream.write(b, off, len);
    }

    /**
     * 刷新输出流
     * 
     * @throws IOException 如果发生IO错误
     */

    @Override
    public void flush() throws IOException {
        dataOutputStream.flush();
    }

    /**
     * 关闭输出流
     * 
     * @throws IOException 如果发生IO错误
     */
    @Override
    public void close() throws IOException {
        dataOutputStream.close();
    }

    /**
     * 写入CRC64校验和到流中（小端序，8字节）
     * 
     * <p>注意：这个方法写入的校验和不会被计入CRC64计算中，
     * 与Redis官方实现保持一致。
     * 
     * @throws IOException 如果发生IO错误
     */
    public void writeCrc64Checksum() throws IOException {
        // 直接写入到底层流，不更新CRC64
        for (int i = 0; i < 8; i++) {
            underlyingOutputStream.write((int) (crc64 >>> (i * 8)) & 0xFF);
        }
    }
}

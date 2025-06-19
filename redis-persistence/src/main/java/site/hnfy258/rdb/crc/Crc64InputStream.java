package site.hnfy258.rdb.crc;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * CRC64计算输入流包装器
 * 
 * <p>该类包装DataInputStream，在数据读取的同时计算CRC64校验和。
 * 与Redis官方实现保持一致，支持增量计算校验和，用于RDB文件
 * 加载过程中的数据完整性验证。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>透明的CRC64计算 - 所有读取操作都会自动更新校验和</li>
 *     <li>校验和隔离 - 读取校验和本身时不会影响CRC64值</li>
 *     <li>Redis兼容 - 使用与Redis相同的CRC64算法</li>
 *     <li>流式处理 - 支持大文件的增量校验和验证</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public class Crc64InputStream extends InputStream {
    
    /** 包装的DataInputStream实例 */
    private final DataInputStream dataInputStream;
    
    /** 底层输入流引用，用于校验和读取时绕过CRC64计算 */
    private final InputStream underlyingInputStream;
    
    /** 当前计算的CRC64校验和值 */
    private long crc64 = 0L;
    
    /**
     * 构造函数
     * 
     * @param inputStream 底层输入流
     */
    public Crc64InputStream(InputStream inputStream) {
        this.underlyingInputStream = inputStream;
        this.dataInputStream = new DataInputStream(inputStream);
    }

    /**
     * 获取包装的DataInputStream
     * 
     * <p>返回一个新的DataInputStream实例，该实例会通过当前对象
     * 进行读取，确保所有数据都被计入CRC64计算。
     * 
     * @return DataInputStream实例
     */
    public DataInputStream getDataInputStream() {
        return new DataInputStream(this);
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
     * 读取单个字节
     * 
     * @return 读取的字节，如果到达流末尾则返回-1
     * @throws IOException 如果发生IO错误
     */
    @Override
    public int read() throws IOException {
        int b = dataInputStream.read();
        if (b >= 0) {
            // 更新CRC64
            byte[] bytes = {(byte) b};
            crc64 = Crc64.crc64(crc64, bytes, 0, 1);
        }
        return b;
    }

    /**
     * 读取字节数组
     * 
     * @param b 目标字节数组
     * @return 实际读取的字节数，如果到达流末尾则返回-1
     * @throws IOException 如果发生IO错误
     */
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * 读取字节数组的指定部分
     * 
     * @param b 目标字节数组
     * @param off 起始偏移量
     * @param len 读取长度
     * @return 实际读取的字节数，如果到达流末尾则返回-1
     * @throws IOException 如果发生IO错误
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = dataInputStream.read(b, off, len);
        if (bytesRead > 0) {
            // 更新CRC64
            crc64 = Crc64.crc64(crc64, b, off, bytesRead);
        }        return bytesRead;
    }

    /**
     * 跳过指定数量的字节
     * 
     * @param n 要跳过的字节数
     * @return 实际跳过的字节数
     * @throws IOException 如果发生IO错误
     */
    @Override
    public long skip(long n) throws IOException {
        return dataInputStream.skip(n);
    }

    /**
     * 获取可读取的字节数
     * 
     * @return 可读取的字节数
     * @throws IOException 如果发生IO错误
     */
    @Override
    public int available() throws IOException {
        return dataInputStream.available();
    }

    /**
     * 关闭输入流
     * 
     * @throws IOException 如果发生IO错误
     */
    @Override
    public void close() throws IOException {
        dataInputStream.close();
    }

    /**
     * 读取CRC64校验和（小端序，8字节）
     * 
     * <p>注意：这个方法读取的校验和不会被计入CRC64计算中，
     * 与Redis官方实现保持一致。
     * 
     * @return 读取的CRC64校验和
     * @throws IOException 如果发生IO错误
     */
    public long readCrc64Checksum() throws IOException {
        long checksum = 0L;
        // 直接从底层流读取，不更新CRC64
        for (int i = 0; i < 8; i++) {
            int b = underlyingInputStream.read();
            if (b < 0) {
                throw new IOException("文件意外结束，无法读取完整的CRC64校验和");
            }
            checksum |= ((long) (b & 0xFF)) << (i * 8);
        }
        return checksum;
    }
}

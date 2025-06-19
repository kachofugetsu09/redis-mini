package site.hnfy258.rdb.crc;

/**
 * Redis兼容的CRC64校验和算法实现
 * 
 * <p>实现与Redis官方完全兼容的CRC64算法，使用相同的多项式和查找表优化。
 * 该实现遵循ECMA-182标准，确保与Redis RDB文件格式的完全兼容性，
 * 提供高效的数据完整性校验功能。
 * 
 * <p>核心特性包括：
 * <ul>
 *     <li>查找表优化 - 预计算256个值，O(1)时间复杂度处理每个字节</li>
 *     <li>增量计算 - 支持流式数据处理，无需缓存完整数据</li>
 *     <li>Redis兼容 - 使用相同的多项式和算法实现</li>
 *     <li>线程安全 - 静态方法设计，无状态实现</li>
 * </ul>
 * 
 * <p>使用示例：
 * <pre>{@code
 * // 计算字符串的CRC64
 * long crc = Crc64.crc64("Hello Redis");
 * 
 * // 增量计算
 * long crc = Crc64.INITIAL_CRC;
 * crc = Crc64.crc64(crc, data1, 0, data1.length);
 * crc = Crc64.crc64(crc, data2, 0, data2.length);
 * }</pre>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public final class Crc64 {

    /** Redis官方使用的CRC64多项式 */
    private static final long POLY = 0x95ac9329ac4bc9b5L;

    /** 预计算的查找表，用于快速CRC64计算 */
    private static final long[] TABLE = new long[256];

    /** CRC64计算的初始值 */
    public static final long INITIAL_CRC = 0L;

    static {
        initializeTable();
    }

    /**
     * 初始化CRC64查找表
     * 
     * <p>预计算256个可能字节值对应的CRC64值，
     * 将运行时的8次位运算优化为一次查表操作。
     */
    private static void initializeTable() {
        for (int i = 0; i < 256; i++) {
            long crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) != 0) {
                    crc = (crc >>> 1) ^ POLY;
                } else {
                    crc >>>= 1;
                }
            }
            TABLE[i] = crc;
        }
    }

    /**
     * 计算数据的CRC64校验和
     * 
     * <p>使用Redis兼容的算法计算CRC64值。支持增量计算，
     * 可以多次调用来处理大型数据流。
     * 
     * @param crc 初始CRC值，首次计算使用{@link #INITIAL_CRC}
     * @param data 要计算校验和的数据
     * @param offset 数据起始偏移量
     * @param length 要处理的数据长度
     * @return 更新后的CRC64值
     * @throws IllegalArgumentException 如果参数无效
     */
    public static long crc64(long crc, byte[] data, int offset, int length) {
        validateParameters(data, offset, length);

        // 使用按位取反避免算法的线性特性
        crc = ~crc;

        // 查表法快速计算CRC64
        for (int i = offset; i < offset + length; i++) {
            crc = TABLE[(int)((crc ^ (data[i] & 0xFF)) & 0xFF)] ^ (crc >>> 8);
        }

        return ~crc;
    }

    /**
     * 计算字节数组的CRC64校验和
     * 
     * @param data 要计算校验和的数据
     * @return CRC64校验和
     */
    public static long crc64(byte[] data) {
        return crc64(INITIAL_CRC, data, 0, data.length);
    }

    /**
     * 计算字符串的CRC64校验和
     * 
     * @param str 要计算校验和的字符串
     * @return CRC64校验和
     * @throws IllegalArgumentException 如果字符串为null
     */
    public static long crc64(String str) {
        if (str == null) {
            throw new IllegalArgumentException("字符串不能为null");
        }
        return crc64(str.getBytes());
    }

    /**
     * 验证方法参数的有效性
     */
    private static void validateParameters(byte[] data, int offset, int length) {
        if (data == null) {
            throw new IllegalArgumentException("数据不能为null");
        }
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("无效的偏移量或长度参数");
        }
    }

    /**
     * 私有构造函数，防止工具类被实例化
     */
    private Crc64() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }
}

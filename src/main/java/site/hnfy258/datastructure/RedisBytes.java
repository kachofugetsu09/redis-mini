package site.hnfy258.datastructure;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 不可变的字节数组封装，用于高效的Redis键值存储
 * 特点：
 * 1. 不可变性保证线程安全
 * 2. 缓存hashCode避免重复计算
 * 3. 直接基于byte[]进行比较，避免String转换
 */
public final class RedisBytes {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    
    private final byte[] bytes;          // 存储的字节数组
    private final int hashCode;          // 缓存的哈希值
    private String stringValue;          // 延迟初始化的字符串值

    public RedisBytes(byte[] bytes) {
        // 创建副本以保证不可变性
        this.bytes = bytes == null ? null : bytes.clone();
        // 在构造时计算并缓存hashCode
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    /**
     * 从字符串创建RedisBytes
     * @param str 源字符串
     * @return RedisBytes实例
     */
    public static RedisBytes fromString(String str) {
        if (str == null) {
            return null;
        }
        RedisBytes redisBytes = new RedisBytes(str.getBytes(CHARSET));
        redisBytes.stringValue = str; // 直接缓存原始字符串
        return redisBytes;
    }

    /**
     * 获取底层字节数组的副本
     * @return 字节数组的副本
     */
    public byte[] getBytes() {
        return bytes == null ? null : bytes.clone();
    }

    /**
     * 获取底层字节数组的直接引用（仅在确保不会修改时使用）
     * @return 字节数组的直接引用
     */
    public byte[] getBytesUnsafe() {
        return bytes;
    }

    /**
     * 获取字符串表示
     * @return 字符串
     */
    public String getString() {
        if (stringValue == null && bytes != null) {
            stringValue = new String(bytes, CHARSET);
        }
        return stringValue;
    }

    /**
     * 比较两个字节数组是否相等（不区分大小写）
     * @param other 要比较的字节数组
     * @return 如果内容相等（不区分大小写）返回true
     */
    public boolean equalsIgnoreCase(byte[] other) {
        if (bytes == other) return true;
        if (bytes == null || other == null) return false;
        if (bytes.length != other.length) return false;

        for (int i = 0; i < bytes.length; i++) {
            if (Character.toLowerCase(bytes[i]) != Character.toLowerCase(other[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 比较两个RedisBytes是否相等（不区分大小写）
     * @param other 要比较的RedisBytes
     * @return 如果内容相等（不区分大小写）返回true
     */
    public boolean equalsIgnoreCase(RedisBytes other) {
        if (this == other) return true;
        if (other == null) return false;
        return equalsIgnoreCase(other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisBytes that = (RedisBytes) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode; // 返回缓存的哈希值
    }

    @Override
    public String toString() {
        return getString();
    }

    /**
     * 获取字节数组的长度
     * @return 字节数组的长度
     */
    public int length() {
        return bytes == null ? 0 : bytes.length;
    }

    /**
     * 检查是否为空
     * @return 如果字节数组为null或长度为0返回true
     */
    public boolean isEmpty() {
        return bytes == null || bytes.length == 0;
    }
}

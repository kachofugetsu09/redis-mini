package site.hnfy258.internal;

import site.hnfy258.datastructure.RedisBytes;

/**
 * Simple Dynamic String (SDS) - Redis风格的动态字符串实现
 * 
 * <p>SDS 提供了比传统字符串更高效的内存管理和操作性能：</p>
 * <ul>
 *   <li><strong>O(1) 长度获取</strong>: 通过预存储长度信息避免遍历计算</li>
 *   <li><strong>二进制安全</strong>: 支持包含空字符的任意二进制数据</li>
 *   <li><strong>内存预分配</strong>: 智能的空间分配策略减少重分配次数</li>
 *   <li><strong>空间效率</strong>: 精确的内存使用，避免不必要的空间浪费</li>
 * </ul>
 * 
 * <p>预分配策略：</p>
 * <ul>
 *   <li>小字符串（≤1MB）: 分配2倍空间，最小8字节</li>
 *   <li>大字符串（>1MB）: 分配原长度+1MB额外空间</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
public class Sds {
    /** 字符串内容存储 */
    private byte[] bytes;
    /** 已使用的字节长度 */
    private int len;
    /** 分配的总字节长度 */
    private int alloc;

    /** 最大预分配大小：1MB */
    private static final int SDS_MAX_PREALLOC = 1024 * 1024; // 1M

    /**
     * 构造 SDS 实例
     * 
     * @param bytes 初始字符串内容
     */
    public Sds(byte[] bytes) {
        // 1. 设置实际长度
        this.len = bytes.length;
        // 2. 计算预分配大小
        this.alloc = calculateAlloc(bytes.length);
        // 3. 分配内存空间
        this.bytes = new byte[this.alloc];
        // 4. 复制原始数据
        System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
    }

    /**
     * 计算预分配大小
     * 
     * @param length 需要的最小长度
     * @return 预分配的总大小
     */
    private int calculateAlloc(final int length) {
        if (length <= SDS_MAX_PREALLOC) {
            // 小字符串：分配2倍空间，最小8字节
            return Math.max(length * 2, 8);
        }
        // 大字符串：分配原长度+1MB
        return length + SDS_MAX_PREALLOC;
    }

    /**
     * 转换为字符串表示
     * 
     * @return UTF-8 编码的字符串
     */
    public String toString() {
        return new String(this.bytes, 0, this.len, RedisBytes.CHARSET);
    }

    /**
     * 获取字符串长度
     * 时间复杂度：O(1)
     * 
     * @return 字符串的字节长度
     */
    public int length() {
        return len;
    }

    /**
     * 设置字符串内容（危险操作，慎用）
     * 
     * @param bytes 新的字符串内容
     */
    public void setBytes(final byte[] bytes) {
        this.bytes = bytes;
        this.len = bytes.length;
        this.alloc = bytes.length;
    }

    /**
     * 清空字符串内容
     * 注意：不释放已分配的内存空间，以便后续复用
     */
    public void clear() {
        this.len = 0;
    }

    /**
     * 追加字节数组到字符串末尾
     * 
     * <p>采用智能扩容策略：</p>
     * <ul>
     *   <li>如果当前空间足够，直接追加</li>
     *   <li>如果空间不足，按预分配策略扩容</li>
     * </ul>
     * 
     * @param extra 要追加的字节数组
     * @return 当前 SDS 实例（支持链式调用）
     */
    public Sds append(final byte[] extra) {
        final int newLen = len + extra.length;
        if (newLen > this.alloc) {
            // 1. 计算新的分配大小
            final int newAlloc = calculateAlloc(newLen);
            // 2. 创建新的字节数组
            final byte[] newBytes = new byte[newAlloc];
            // 3. 复制原有数据
            System.arraycopy(bytes, 0, newBytes, 0, len);
            // 4. 更新内部状态
            bytes = newBytes;
            this.alloc = newAlloc;
        }
        // 5. 追加新数据
        System.arraycopy(extra, 0, bytes, len, extra.length);
        this.len = newLen;
        return this;
    }

    /**
     * 追加字符串到末尾
     * 
     * @param str 要追加的字符串
     * @return 当前 SDS 实例（支持链式调用）
     */
    public Sds append(final String str) {
        return append(str.getBytes(RedisBytes.CHARSET));
    }

    /**
     * 获取字符串的字节数组副本
     * 返回的数组长度等于实际内容长度，不包含预分配的空间
     * 
     * @return 字符串内容的字节数组副本
     */
    public byte[] getBytes() {
        final byte[] result = new byte[len];
        System.arraycopy(bytes, 0, result, 0, len);
        return result;
    }
}


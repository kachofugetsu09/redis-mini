package site.hnfy258.internal;

import site.hnfy258.datastructure.RedisBytes;

import java.util.Arrays;

public class Sds {
    // 字符串内容存储，这是可变的，可以动态扩容
    private byte[] buf;
    // 实际已用长度
    private int len;
    // 已分配空间长度 (buf.length)
    private int alloc;

    // 最大预分配大小：1MB
    private static final int SDS_MAX_PREALLOC = 1024 * 1024;
    // 初始最小分配空间
    private static final int SDS_INITIAL_CAPACITY = 8;

    // 私有构造函数，只允许通过静态工厂方法创建
    private Sds(final byte[] initialBytes) {
        this.len = initialBytes.length;
        this.alloc = calculateAllocGreedy(this.len); // 初始时可以进行一次贪婪预分配
        this.buf = new byte[this.alloc];
        System.arraycopy(initialBytes, 0, this.buf, 0, this.len);
    }

    // --- 工厂方法 ---
    public static Sds create(final byte[] bytes) {
        return new Sds(bytes);
    }

    public static Sds empty() {
        return new Sds(new byte[0]);
    }

    public static Sds fromString(final String str) {
        if (str == null || str.isEmpty()) {
            return empty();
        }
        return create(str.getBytes(RedisBytes.CHARSET));
    }

    // --- 核心方法：原地修改和扩容 ---

    public int length() {
        return len;
    }

    public int alloc() {
        return alloc;
    }

    // 清空字符串，不清空底层数组，只重置长度
    public void clear() {
        this.len = 0;
        // alloc 保持不变，可以重用已分配的空间
    }

    /**
     * 追加字节数组，原地修改当前Sds实例
     *
     * @param extra 要追加的字节数组
     * @return 当前Sds实例 (this)
     */
    public Sds append(final byte[] extra) {
        if (extra == null || extra.length == 0) {
            return this;
        }

        final int newLen = this.len + extra.length;
        // 检查是否需要扩容
        if (newLen > this.alloc) {
            this.buf = realloc(newLen); // 扩容并更新buf和alloc
        }

        System.arraycopy(extra, 0, this.buf, this.len, extra.length);
        this.len = newLen; // 更新长度

        return this; // 返回当前实例，表示原地修改
    }

    /**
     * 追加字符串，原地修改当前Sds实例
     * @param str 要追加的字符串
     * @return 当前Sds实例 (this)
     */
    public Sds append(final String str) {
        return append(str.getBytes(RedisBytes.CHARSET));
    }

    /**
     * 内部扩容逻辑，返回新的底层byte[]
     */
    private byte[] realloc(final int minCapacity) {
        int newAlloc = calculateAllocGreedy(minCapacity); // 计算新的预分配大小
        // 确保新分配的空间至少能容纳minCapacity
        newAlloc = Math.max(newAlloc, minCapacity);

        // 创建新数组，复制旧数据
        byte[] newBuf = Arrays.copyOf(this.buf, newAlloc);
        this.alloc = newAlloc; // 更新分配空间
        return newBuf;
    }

    protected static int calculateAllocGreedy(final int length) {
        if (length < SDS_MAX_PREALLOC) {
            return Math.max(length * 2, SDS_INITIAL_CAPACITY); // 小于1MB时，翻倍扩容
        }
        return length + SDS_MAX_PREALLOC; // 大于等于1MB时，每次只增加1MB
    }


    public final int avail() {
        return alloc - len;
    }

    public final byte[] getBytes() {
        // 返回当前实际内容的副本，防止外部修改内部数组
        return Arrays.copyOf(buf, len);
    }

    @Override
    public String toString() {
        return new String(this.buf, 0, len, RedisBytes.CHARSET);
    }

    public final int compare(final Sds other) {
        // 比较逻辑不变
        final int minLen = Math.min(this.length(), other.length());
        for (int i = 0; i < minLen; i++) {
            final int diff = (this.buf[i] & 0xFF) - (other.buf[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return this.length() - other.length();
    }

    /**
     * 复制SDS字符串，返回新的Sds实例
     */
    public final Sds duplicate() {
        return Sds.create(this.getBytes()); // 调用create工厂方法，返回一个新Sds
    }

    /**
     * 字符串截取，原地修改当前Sds实例
     */
    public final Sds substring(final int start, final int len) {
        if (start < 0 || start >= this.len || len <= 0) {
            this.clear(); // 截取无效范围，清空
            return this;
        }

        final int actualLen = Math.min(len, this.len - start);
        if (start > 0) {
            System.arraycopy(this.buf, start, this.buf, 0, actualLen);
        }
        this.len = actualLen; // 更新实际长度
        return this;
    }

    public int charLength() {
        return new String(this.buf, 0, this.len, RedisBytes.CHARSET).length();
    }
}
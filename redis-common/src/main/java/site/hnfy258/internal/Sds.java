package site.hnfy258.internal;

import site.hnfy258.datastructure.RedisBytes;

/**
 * 改进版Simple Dynamic String (SDS) - Redis风格的动态字符串实现
 *
 * <p>相比原版实现的主要改进：</p>
 * <ul>
 *   <li><strong>多类型优化</strong>: 根据长度使用不同头部类型，节省内存</li>
 *   <li><strong>更丰富的API</strong>: 提供字符串比较、截取、去空格等功能</li>
 *   <li><strong>更精细的预分配</strong>: 支持贪婪和非贪婪两种分配策略</li>
 *   <li><strong>自动类型升级</strong>: 当容量不足时自动升级到更大类型</li>
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
public abstract class Sds {
    /** 字符串内容存储 */
    protected byte[] bytes;

    /** 最大预分配大小：1MB */
    private static final int SDS_MAX_PREALLOC = 1024 * 1024;    /**
     * 根据初始数据创建合适类型的SDS
     * 
     * @param bytes 初始字符串内容
     * @return 最优的SDS实例
     */
    public static Sds create(final byte[] bytes) {
        final int length = bytes.length;
        
        // 1. 基于数据长度选择类型，而不是预分配大小
        if (length <= 255) {
            return new Sds8(bytes);
        } else if (length <= 65535) {
            return new Sds16(bytes);
        } else {
            return new Sds32(bytes);
        }
    }
    

    @Deprecated
    public static Sds of(final byte[] bytes) {
        return create(bytes);
    }
    
    /**
     * 工厂方法：创建空的SDS
     * 
     * @return 空的SDS8实例
     */
    public static Sds empty() {
        return new Sds8(new byte[0]);
    }
    
    /**
     * 从字符串创建SDS
     * 
     * @param str 源字符串
     * @return SDS实例
     */
    public static Sds fromString(final String str) {
        if (str == null || str.isEmpty()) {
            return empty();
        }
        return create(str.getBytes(RedisBytes.CHARSET));
    }

    // 抽象方法
    public abstract int length();
    public abstract int alloc();
    public abstract void clear();
    public abstract Sds append(byte[] extra);
    protected abstract void setLength(int newLength);

    // 通用方法
    public final int avail() {
        return alloc() - length();
    }

    public final Sds append(final String str) {
        return append(str.getBytes(RedisBytes.CHARSET));
    }

    public final byte[] getBytes() {
        final byte[] result = new byte[length()];
        System.arraycopy(bytes, 0, result, 0, length());
        return result;
    }

    public String toString() {
        return new String(this.bytes, 0, length(), RedisBytes.CHARSET);
    }

    /**
     * 比较两个SDS字符串
     */
    public final int compare(final Sds other) {
        final int minLen = Math.min(this.length(), other.length());

        for (int i = 0; i < minLen; i++) {
            final int diff = (this.bytes[i] & 0xFF) - (other.bytes[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }

        return this.length() - other.length();
    }

    /**
     * 复制SDS字符串
     */
    public final Sds duplicate() {
        return Sds.create(this.getBytes());
    }

    /**
     * 字符串截取
     */
    public final Sds substring(final int start, final int len) {
        if (start < 0 || start >= length() || len <= 0) {
            clear();
            return this;
        }

        final int actualLen = Math.min(len, length() - start);
        if (start > 0) {
            System.arraycopy(bytes, start, bytes, 0, actualLen);
        }
        setLength(actualLen);
        return this;
    }

    /**
     * 计算预分配大小
     */
    protected static int calculateAlloc(final int length, final boolean greedy) {
        if (!greedy) {
            return Math.max(length, 8);
        }

        if (length < SDS_MAX_PREALLOC) {
            return Math.max(length * 2, 16);
        }
        return length + SDS_MAX_PREALLOC;
    }

    protected static int calculateAlloc(final int length) {
        return calculateAlloc(length, true);
    }

    /**
     * 保守的预分配策略，控制内存开销在合理范围内
     */
    protected static int calculateAllocConservative(final int length) {
        // 1. 极小字符串：最小预分配，控制内存开销在100%以内
        if (length <= 8) {
            return Math.max(length + 4, 8);  // 最多50%开销
        } else if (length <= 32) {
            // 2. 小字符串：适度预分配，确保内存开销不超过100%
            return Math.min(length + Math.max(4, length / 2), 255);
        } else if (length < 200) {
            // 3. 中小字符串：预留约25%空间，确保在SDS8范围内
            return Math.min(length + Math.max(8, length / 4), 255);
        } else if (length < 255) {
            // 4. 接近SDS8边界：只预留必要空间
            return Math.min(length + 4, 255);
        } else if (length < 32768) {
            // 5. 中等字符串：预留约25%空间，确保在SDS16范围内
            return Math.min(length + Math.max(32, length / 4), 65535);
        } else if (length < 65500) {
            // 6. 接近SDS16边界：只预留必要空间
            return Math.min(length + 16, 65535);
        } else {
            // 7. 大字符串：保守预分配
            return Math.min(length + 256, Integer.MAX_VALUE);
        }
    }

    /**
     * SDS8类型 - 适用于长度 < 256的字符串
     */
    private static final class Sds8 extends Sds {
        private byte len;
        private byte alloc;        public Sds8(final byte[] bytes) {
            this.len = (byte) bytes.length;
            final int allocSize = calculateAllocConservative(bytes.length);
            // 1. 确保分配大小不超过类型限制，如果超过则使用最大值
            this.alloc = (byte) Math.min(allocSize, 255);
            this.bytes = new byte[this.alloc & 0xFF];
            System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
        }

        @Override
        public int length() {
            return len & 0xFF;
        }

        @Override
        public int alloc() {
            return alloc & 0xFF;
        }

        @Override
        protected void setLength(final int newLength) {
            if (newLength > 255) {
                throw new IllegalStateException("Length too large for SDS8");
            }
            this.len = (byte) newLength;
        }

        @Override
        public void clear() {
            this.len = 0;
        }

        @Override
        public Sds append(final byte[] extra) {
            final int newLen = length() + extra.length;

            // 检查是否需要升级类型
            if (newLen >= 256) {
                final Sds16 newSds = new Sds16(getBytes());
                return newSds.append(extra);
            }            if (newLen > alloc()) {
                final int newAlloc = calculateAllocConservative(newLen);
                if (newAlloc > 255) {
                    final Sds16 newSds = new Sds16(getBytes());
                    return newSds.append(extra);
                }

                final byte[] newBytes = new byte[newAlloc];
                System.arraycopy(bytes, 0, newBytes, 0, length());
                bytes = newBytes;
                this.alloc = (byte) newAlloc;
            }

            System.arraycopy(extra, 0, bytes, length(), extra.length);
            setLength(newLen);
            return this;
        }
    }

    /**
     * SDS16类型 - 适用于长度 256-65535的字符串
     */
    private static final class Sds16 extends Sds {
        private short len;
        private short alloc;        public Sds16(final byte[] bytes) {
            this.len = (short) bytes.length;
            final int allocSize = calculateAllocConservative(bytes.length);
            // 1. 确保分配大小不超过类型限制，如果超过则使用最大值
            this.alloc = (short) Math.min(allocSize, 65535);
            this.bytes = new byte[this.alloc & 0xFFFF];
            System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
        }

        @Override
        public int length() {
            return len & 0xFFFF;
        }

        @Override
        public int alloc() {
            return alloc & 0xFFFF;
        }

        @Override
        protected void setLength(final int newLength) {
            if (newLength > 65535) {
                throw new IllegalStateException("Length too large for SDS16");
            }
            this.len = (short) newLength;
        }

        @Override
        public void clear() {
            this.len = 0;
        }

        @Override
        public Sds append(final byte[] extra) {
            final int newLen = length() + extra.length;

            if (newLen >= 65536) {
                final Sds32 newSds = new Sds32(getBytes());
                return newSds.append(extra);
            }            if (newLen > alloc()) {
                final int newAlloc = calculateAllocConservative(newLen);
                if (newAlloc > 65535) {
                    final Sds32 newSds = new Sds32(getBytes());
                    return newSds.append(extra);
                }

                final byte[] newBytes = new byte[newAlloc];
                System.arraycopy(bytes, 0, newBytes, 0, length());
                bytes = newBytes;
                this.alloc = (short) newAlloc;
            }

            System.arraycopy(extra, 0, bytes, length(), extra.length);
            setLength(newLen);
            return this;
        }
    }

    /**
     * SDS32类型 - 适用于长度 >= 65536的字符串
     */
    private static final class Sds32 extends Sds {
        private int len;
        private int alloc;        public Sds32(final byte[] bytes) {
            this.len = bytes.length;
            this.alloc = calculateAllocConservative(bytes.length);
            this.bytes = new byte[this.alloc];
            System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public int alloc() {
            return alloc;
        }

        @Override
        protected void setLength(final int newLength) {
            this.len = newLength;
        }

        @Override
        public void clear() {
            this.len = 0;
        }        @Override
        public Sds append(final byte[] extra) {
            final int newLen = len + extra.length;
            if (newLen > this.alloc) {
                final int newAlloc = calculateAllocConservative(newLen);
                final byte[] newBytes = new byte[newAlloc];
                System.arraycopy(bytes, 0, newBytes, 0, len);
                bytes = newBytes;
                this.alloc = newAlloc;
            }

            System.arraycopy(extra, 0, bytes, len, extra.length);
            this.len = newLen;
            return this;
        }
    }
}

package site.hnfy258.datastructure;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 高性能不可变字节数组封装类，专为Redis操作优化。
 *
 * <p>本类为字节数组提供不可变封装，包含多项性能优化机制：
 * <ul>
 *   <li>命令池缓存：预创建常用Redis命令实例，避免重复分配
 *   <li>字符串缓存：延迟初始化并缓存字符串表示，减少重复编码开销
 *   <li>哈希值缓存：预计算哈希值，提升HashMap等容器的查找性能
 *   <li>智能拷贝策略：优化数组拷贝，充分利用JVM内建优化
 *   <li>零拷贝接口：为受信任场景提供高性能访问路径
 * </ul>
 *
 * <p>线程安全性：本类是不可变的，线程安全。内部字符串缓存使用适当的同步机制。
 *
 * <p>性能特征：针对Redis命令处理场景优化，通过缓存和安全的零拷贝操作
 * 实现微秒级性能提升。
 *
 * @author hnfy258
 * @since 1.0
 */
public final class RedisBytes implements Comparable<RedisBytes> {

    /**
     * 字符串编码解码使用的字符集。
     */
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * 自动字符串缓存优化的最大长度。
     */
    private static final int MAX_CACHED_STRING_SIZE = 128;

    /**
     * 常用Redis命令缓存池，避免重复分配。
     */
    private static final ConcurrentHashMap<String, RedisBytes> COMMAND_CACHE =
            new ConcurrentHashMap<>(64);

    /**
     * 预分配的空字节数组实例。
     */
    public static final RedisBytes EMPTY = new RedisBytes(new byte[0]);

    /**
     * 预分配的"OK"响应实例。
     */
    public static final RedisBytes OK;
    /**
     * 预分配的"PONG"响应实例。
     */
    public static final RedisBytes PONG;

    static {
        // 1. 初始化常用命令缓存池
        initializeCommonCommands();

        // 2. 初始化常用常量实例
        OK = fromString("OK");
        PONG = fromString("PONG");
    }

    // ========== 实例字段 ==========

    /**
     * 存储的字节数组（不可变）。
     */
    private final byte[] bytes;

    /**
     * 预计算的哈希值，提升HashMap性能。
     */
    private final int hashCode;

    /**
     * 标记是否为受信任的数组，用于零拷贝优化。
     */
    private final boolean isTrusted;

    /**
     * 延迟初始化的字符串值，减少重复编码。
     */
    private volatile String stringValue;

    // ========== 构造函数 ==========

    /**
     * 创建不可变字节数组实例。
     *
     * <p>执行防御性拷贝以确保不可变性。
     *
     * @param bytes 源字节数组，不能为null
     * @throws IllegalArgumentException 如果bytes为null
     */
    public RedisBytes(final byte[] bytes) {
        this(bytes, false);
    }

    /**
     * 内部构造函数，支持零拷贝优化。
     *
     * @param bytes 源字节数组     * @param trusted 是否为受信任的数组，true时不执行拷贝
     * @throws IllegalArgumentException 如果bytes为null
     */
    private RedisBytes(final byte[] bytes, final boolean trusted) {
        if (bytes == null) {
            throw new IllegalArgumentException("字节数组不能为null");
        }

        if (trusted) {
            // 1. 零拷贝模式：直接使用原数组（仅用于内部受信任场景）
            this.bytes = bytes;
            this.isTrusted = true;
        } else {
            // 2. 安全模式：执行防御性拷贝（JVM对小数组的clone()高度优化）
            this.bytes = bytes.clone();
            this.isTrusted = false;
        }

        // 3. 预计算哈希值，提升HashMap查找性能
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    // ========== 工厂方法 ==========

    /**
     * 创建零拷贝RedisBytes实例。
     *
     * <p><b>警告</b>：调用者必须保证参数数组在实例生命周期内不被修改！
     * 仅用于内部受信任场景。
     *
     * @param trustedBytes 受信任的字节数组
     * @return RedisBytes实例，如果输入为null则返回null
     */
    public static RedisBytes wrapTrusted(final byte[] trustedBytes) {
        if (trustedBytes == null) {
            return null;
        }
        return new RedisBytes(trustedBytes, true);
    }

    /**
     * 创建RedisBytes实例，优先使用命令缓存池以提高性能。
     *
     * <p>该方法会首先检查命令缓存池中是否存在相同的字符串，如果存在则直接返回缓存的实例。
     * 对于常用的Redis命令（如GET、SET等），这可以显著减少内存分配和提高性能。</p>
     *
     * @param str 源字符串，不能为null
     * @return RedisBytes实例，如果输入为null则返回null
     */
    public static RedisBytes fromString(final String str) {
        if (str == null) {
            return null;
        }

        if (str.isEmpty()) {
            return EMPTY;
        }

        // 1. 优先从缓存池获取常用命令
        final RedisBytes cached = COMMAND_CACHE.get(str.toUpperCase());
        if (cached != null) {
            return cached;
        }

        // 2. 创建新实例
        final RedisBytes redisBytes = new RedisBytes(str.getBytes(CHARSET));

        // 3. 对于小字符串，预设字符串值以避免后续转换
        if (str.length() <= MAX_CACHED_STRING_SIZE) {
            redisBytes.stringValue = str;
        }

        return redisBytes;
    }

    /**
     * 初始化常用命令池
     */
    private static void initializeCommonCommands() {
        final String[] commands = {
                // 字符串命令
                "GET", "SET", "MSET", "APPEND", "INCR", "STRLEN", "GETRANGE",
                // 列表命令
                "LPUSH", "LPOP", "RPUSH", "RPOP", "LRANGE", "LLEN",
                // 集合命令
                "SADD", "SPOP", "SREM", "SCARD",
                // 哈希命令
                "HSET", "HGET", "HDEL",
                // 有序集合命令
                "ZADD", "ZRANGE", "ZCARD",
                // 服务器命令
                "PING", "SELECT", "SCAN", "KEYS", "INFO", "CONFIG", "DBSIZE",
                // 持久化命令
                "BGSAVE", "BGREWRITEAOF"
        };

        for (final String cmd : commands) {
            final RedisBytes redisBytes = new RedisBytes(cmd.getBytes(CHARSET));
            redisBytes.stringValue = cmd; // 预设字符串值            COMMAND_CACHE.put(cmd, redisBytes);
        }
    }

    // ========== 核心方法 ==========

    /**
     * 获取底层字节数组的副本
     *
     * @return 字节数组的副本（保证不可变性）
     */
    public byte[] getBytes() {
        if (bytes == null) {
            return null;
        }
        // 始终返回副本以保证不可变性，除非是受信任的内部构造
        return isTrusted ? bytes : bytes.clone();
    }

    /**
     * 获取底层字节数组的直接引用，用于高性能只读操作。
     *
     * <p><strong>警告：</strong>调用者不得修改返回的数组！仅用于只读操作。
     * 这个方法提供零拷贝访问，适用于序列化、网络传输等性能敏感场景。</p>
     *
     * @return 字节数组的直接引用，为 null 时返回 null
     */
    public byte[] getBytesUnsafe() {
        return bytes;
    }

    /**
     * 获取字符串表示
     *
     * @return 字符串值
     */
    public String getString() {
        // 1. 双检查锁定模式的延迟初始化
        String result = stringValue;
        if (result == null) {
            synchronized (this) {
                result = stringValue;
                if (result == null) {
                    result = bytes == null ? null : new String(bytes, CHARSET);
                    stringValue = result;
                }
            }
        }
        return result;
    }

    /**
     * 大小写不敏感的字节比较（优化版本）     *
     *
     * @param other 另一个RedisBytes对象
     * @return 是否相等（忽略大小写）
     */
    public boolean equalsIgnoreCase(final RedisBytes other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (this.bytes == null || other.bytes == null) {
            return this.bytes == other.bytes;
        }

        final byte[] thisBytes = this.bytes;
        final byte[] otherBytes = other.bytes;
        final int length = thisBytes.length;

        if (length != otherBytes.length) {
            return false;
        }

        // 优化的字节级别大小写不敏感比较
        for (int i = 0; i < length; i++) {
            byte thisByte = thisBytes[i];
            byte otherByte = otherBytes[i];

            // 如果字节相等，直接跳过
            if (thisByte == otherByte) {
                continue;
            }

            // 转换为小写进行比较（只处理ASCII字母）
            if (thisByte >= 'A' && thisByte <= 'Z') {
                thisByte += 32; // 转换为小写
            }
            if (otherByte >= 'A' && otherByte <= 'Z') {
                otherByte += 32; // 转换为小写
            }

            if (thisByte != otherByte) {
                return false;
            }
        }
        return true;
    }

    /**
     * 高性能字节数组比较方法。
     *
     * <p>直接与字节数组进行比较，避免创建临时对象，适用于性能敏感的比较操作。</p>
     *
     * @param otherBytes 要比较的字节数组
     * @return 如果字节数组内容相等则返回 true，否则返回 false
     */
    public boolean equalsByteArray(final byte[] otherBytes) {
        return Arrays.equals(this.bytes, otherBytes);
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final RedisBytes other = (RedisBytes) obj;
        return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode; // 返回预计算的哈希值
    }

    @Override
    public String toString() {
        if (bytes == null) {
            return "RedisBytes[null]";
        }

        // 1. 只显示基本信息，避免创建完整字符串
        final StringBuilder sb = new StringBuilder();
        sb.append("RedisBytes[length=").append(bytes.length)
                .append(", hashCode=").append(hashCode());

        // 2. 如果是小数据且可能是文本，显示前几个字符
        if (bytes.length <= 32) {
            sb.append(", preview='");
            for (int i = 0; i < Math.min(bytes.length, 16); i++) {
                final byte b = bytes[i];
                if (b >= 32 && b <= 126) {
                    sb.append((char) b);
                } else {
                    sb.append("\\x").append(String.format("%02x", b & 0xFF));
                }
            }
            if (bytes.length > 16) {
                sb.append("...");
            }
            sb.append("'");
        } else {
            // 3. 大数据只显示统计信息
            sb.append(", type=binary");
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * 获取字节数组的长度
     *
     * @return 字节数组的长度
     */
    public int length() {
        return bytes == null ? 0 : bytes.length;
    }

    /**
     * 检查是否为空
     *
     * @return 如果字节数组为null或长度为0返回true
     */
    public boolean isEmpty() {
        return bytes == null || bytes.length == 0;
    }

    /**
     * 获取命令缓存统计信息
     *
     * @return 缓存的命令数量
     */
    public static int getCachedCommandCount() {
        return COMMAND_CACHE.size();
    }

    /**
     * 检查指定命令是否在缓存中
     *
     * @param command 命令名称
     * @return 是否存在于缓存中
     */
    public static boolean isCommandCached(final String command) {
        return COMMAND_CACHE.containsKey(command.toUpperCase());
    }

    /**
     * 实现 Comparable 接口，提供字节级别的字典序比较。
     *
     * <p>使用字节数组的字典序比较，确保排序结果的一致性和可重现性。</p>
     * <p>比较逻辑：</p>
     * <ul>
     *   <li>逐字节比较，遇到不同字节时返回差值</li>
     *   <li>如果一个数组是另一个的前缀，则较短的数组排在前面</li>
     *   <li>空值处理：null < 非null，两个 null 相等</li>
     * </ul>
     *
     * @param other 要比较的 RedisBytes 对象
     * @return 负数（this < other），0（this == other），正数（this > other）
     */
    @Override
    public int compareTo(final RedisBytes other) {
        // 1. null 值处理
        if (other == null) {
            return 1; // 非null > null
        }

        // 2. 引用相等检查
        if (this == other) {
            return 0;
        }

        // 3. 获取字节数组（使用高性能路径）
        final byte[] thisBytes = this.getBytesUnsafe();
        final byte[] otherBytes = other.getBytesUnsafe();

        // 4. 处理空数组情况
        if (thisBytes == null && otherBytes == null) {
            return 0;
        }
        if (thisBytes == null) {
            return -1; // null < 非null
        }
        if (otherBytes == null) {
            return 1; // 非null > null
        }

        // 5. 字典序比较：逐字节比较
        final int minLength = Math.min(thisBytes.length, otherBytes.length);
        for (int i = 0; i < minLength; i++) {
            // 将字节转换为无符号整数进行比较
            final int thisByte = thisBytes[i] & 0xFF;
            final int otherByte = otherBytes[i] & 0xFF;

            if (thisByte != otherByte) {
                return thisByte - otherByte;
            }
        }

        // 6. 长度比较：如果前缀相同，较短的排在前面
        return Integer.compare(thisBytes.length, otherBytes.length);
    }
}

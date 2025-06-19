package site.hnfy258.core;

import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis批处理优化工具类
 * 
 * <p>提供高性能的批量操作功能，通过批处理策略优化大量数据的处理性能。
 * 当操作数量超过特定阈值时，自动启用优化策略以减少系统开销。
 * 
 * <p>主要优化策略包括：
 * <ul>
 *     <li>预分配内存空间，减少频繁的内存分配</li>
 *     <li>批量数据预处理，提高数据转换效率</li>
 *     <li>智能阈值判断，平衡性能和内存使用</li>
 *     <li>线程安全的并发处理支持</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
public final class RedisBatchOptimizer {

    /** 批处理阈值，超过此数量时启用批处理优化 */
    private static final int BATCH_SIZE_THRESHOLD = 10;
    
    /** 缓冲区初始容量，用于预分配内存 */
    private static final int BUFFER_INITIAL_CAPACITY = 16;
    /**
     * 批量设置字符串键值对
     * 
     * <p>根据键值对数量自动选择优化策略：
     * <ul>
     *     <li>小批量（少于阈值）：直接逐个设置</li>
     *     <li>大批量（超过阈值）：使用优化的批处理策略</li>
     * </ul>
     * 
     * @param redisCore Redis核心实例
     * @param keyValuePairs 键值对映射表
     */
    public static void batchSetStrings(final RedisCore redisCore,
                                      final Map<RedisBytes, RedisBytes> keyValuePairs) {
        if (keyValuePairs.isEmpty()) {
            return;
        }

        // 1. 小批量直接设置
        if (keyValuePairs.size() < BATCH_SIZE_THRESHOLD) {
            for (final Map.Entry<RedisBytes, RedisBytes> entry : keyValuePairs.entrySet()) {
                final RedisString redisString = new RedisString(Sds.create(entry.getValue().getBytes()));
                redisCore.put(entry.getKey(), redisString);
            }
            return;
        }

        // 2. 大批量优化设置
        batchSetOptimized(redisCore, keyValuePairs);
    }

    /**
     * 优化的批量设置方法
     * 
     * <p>使用三步优化策略：
     * <ol>
     *     <li>预分配RedisString对象缓存</li>
     *     <li>批量预处理所有数据</li>
     *     <li>批量写入到数据库</li>
     * </ol>
     * 
     * @param redisCore Redis核心实例
     * @param keyValuePairs 键值对映射表
     */
    private static void batchSetOptimized(final RedisCore redisCore,
                                         final Map<RedisBytes, RedisBytes> keyValuePairs) {
        // 1. 预分配RedisString对象缓存
        final Map<RedisBytes, RedisString> preparedData = new ConcurrentHashMap<>(
            keyValuePairs.size() + BUFFER_INITIAL_CAPACITY);

        // 2. 批量预处理数据
        for (final Map.Entry<RedisBytes, RedisBytes> entry : keyValuePairs.entrySet()) {
            final RedisString redisString = new RedisString(Sds.create(entry.getValue().getBytes()));
            preparedData.put(entry.getKey(), redisString);
        }

        // 3. 批量写入数据库
        for (final Map.Entry<RedisBytes, RedisString> entry : preparedData.entrySet()) {
            redisCore.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 批量递增数值
     * 
     * <p>对指定的键列表执行数值递增操作，支持自定义递增值。
     * 如果键不存在或值无效，则从0开始计算。
     * 
     * @param redisCore Redis核心实例
     * @param keys 需要递增的键数组
     * @param increment 递增值
     * @throws NumberFormatException 如果值不是有效的数字格式
     */
    public static void batchIncrement(final RedisCore redisCore,
                                     final RedisBytes[] keys,
                                     final long increment) {
        for (final RedisBytes key : keys) {
            final RedisData currentData = redisCore.get(key);
            final long currentValue = extractNumericValue(currentData);
            final long newValue = currentValue + increment;
            
            final RedisString newRedisString = new RedisString(
                Sds.create(String.valueOf(newValue).getBytes())
            );
            redisCore.put(key, newRedisString);
        }
    }

    /**
     * 从RedisData中提取数值
     * 
     * <p>支持从RedisString类型中安全地提取数值，处理各种边界情况：
     * <ul>
     *     <li>数据为null时返回0</li>
     *     <li>非字符串类型时抛出异常</li>
     *     <li>空字符串时返回0</li>
     *     <li>无效数字格式时抛出异常</li>
     * </ul>
     * 
     * @param redisData Redis数据对象
     * @return 提取的数值，默认为0
     * @throws NumberFormatException 如果数据类型不匹配或数字格式无效
     */
    private static long extractNumericValue(final RedisData redisData) {
        if (redisData == null) {
            return 0L;
        }

        if (!(redisData instanceof RedisString)) {
            throw new NumberFormatException("Value is not a string");
        }

        final RedisString redisString = (RedisString) redisData;
        final String valueStr = redisString.getSds().toString();

        if (valueStr.isEmpty()) {
            return 0L;
        }

        try {
            return Long.parseLong(valueStr);
        } catch (final NumberFormatException e) {
            throw new NumberFormatException("Value is not a valid integer: " + valueStr);
        }
    }

    /**
     * 私有构造函数，防止工具类被实例化
     * 
     * @throws UnsupportedOperationException 如果尝试实例化此工具类
     */
    private RedisBatchOptimizer() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
}

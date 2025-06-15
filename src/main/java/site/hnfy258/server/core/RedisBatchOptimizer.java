package site.hnfy258.server.core;

import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis批处理优化工具类
 * 用于提升批量操作的性能
 * 
 * @author hnfy258
 * @since 2025-06-15
 */
public final class RedisBatchOptimizer {
    
    private static final int BATCH_SIZE_THRESHOLD = 10;
    private static final int BUFFER_INITIAL_CAPACITY = 16;

    /**
     * 批量设置字符串键值对
     * 当键值对数量超过阈值时使用批量优化策略
     * 
     * @param redisCore Redis核心实例
     * @param keyValuePairs 键值对映射
     */
    public static void batchSetStrings(final RedisCore redisCore, 
                                      final Map<RedisBytes, RedisBytes> keyValuePairs) {
        if (keyValuePairs.isEmpty()) {
            return;
        }

        // 1. 小批量直接设置
        if (keyValuePairs.size() < BATCH_SIZE_THRESHOLD) {
            for (final Map.Entry<RedisBytes, RedisBytes> entry : keyValuePairs.entrySet()) {
                final RedisString redisString = new RedisString(new Sds(entry.getValue().getBytes()));
                redisCore.put(entry.getKey(), redisString);
            }
            return;
        }

        // 2. 大批量优化设置
        batchSetOptimized(redisCore, keyValuePairs);
    }

    /**
     * 优化的批量设置方法
     * 使用预分配和批量操作减少开销
     */
    private static void batchSetOptimized(final RedisCore redisCore, 
                                         final Map<RedisBytes, RedisBytes> keyValuePairs) {
        // 1. 预分配RedisString对象缓存
        final Map<RedisBytes, RedisString> preparedData = new ConcurrentHashMap<>(
            keyValuePairs.size() + BUFFER_INITIAL_CAPACITY);

        // 2. 批量预处理数据
        for (final Map.Entry<RedisBytes, RedisBytes> entry : keyValuePairs.entrySet()) {
            final RedisString redisString = new RedisString(new Sds(entry.getValue().getBytes()));
            preparedData.put(entry.getKey(), redisString);
        }        // 3. 批量写入数据库
        for (final Map.Entry<RedisBytes, RedisString> entry : preparedData.entrySet()) {
            redisCore.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 批量递增数值
     * 
     * @param redisCore Redis核心实例
     * @param keys 需要递增的键列表
     * @param increment 递增值
     */
    public static void batchIncrement(final RedisCore redisCore, 
                                     final RedisBytes[] keys, 
                                     final long increment) {
        for (final RedisBytes key : keys) {
            final RedisData currentData = redisCore.get(key);
            final long currentValue = extractNumericValue(currentData);
            final long newValue = currentValue + increment;
            
            final RedisString newRedisString = new RedisString(
                new Sds(String.valueOf(newValue).getBytes())
            );
            redisCore.put(key, newRedisString);
        }
    }

    /**
     * 从RedisData中提取数值
     * 
     * @param redisData Redis数据对象
     * @return 数值，如果数据为空或无效则返回0
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
     * 私有构造函数，防止实例化
     */
    private RedisBatchOptimizer() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
}

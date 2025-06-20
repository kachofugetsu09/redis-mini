package site.hnfy258.aof.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.datastructure.*;
import site.hnfy258.protocal.Resp;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * AOF（Append Only File）数据写入工具类
 * 
 * <p>提供 Redis 各种数据类型到 AOF 文件的序列化和写入功能。
 * 支持 String、List、Set、Hash、ZSet 等所有 Redis 数据类型的
 * 高效批量写入和命令重构。</p>
 * 
 * <p>主要功能：</p>
 * <ul>
 *   <li>将内存中的 Redis 数据结构转换为 RESP 协议命令</li>
 *   <li>批量写入 AOF 文件，保证数据持久化</li>
 *   <li>支持多种数据类型的智能分发处理</li>
 *   <li>提供健壮的异常处理和错误恢复机制</li>
 * </ul>
 * 
 * <p>线程安全性：此类中的方法不是线程安全的，需要外部调用者确保同步。</p>
 * 
 * @author hnfy258
 * @version 1.0.0
 * @since 1.0.0
 */
@Slf4j
public final class AofUtils {
    
    // ==================== 构造函数 ====================
    
    /**
     * 私有构造函数，防止实例化工具类
     * 
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    private AofUtils() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }    
    // ==================== 公共API方法 ====================

    /**
     * 将 Redis 数据写入 AOF 文件
     * 
     * <p>根据数据类型自动选择合适的序列化方式，将内存中的 Redis 数据结构
     * 转换为对应的 RESP 协议命令并写入 AOF 文件。支持所有 Redis 基本数据类型。</p>
     * 
     * <p>处理流程：</p>
     * <ol>
     *   <li>参数有效性验证</li>
     *   <li>跳过 null 值（避免无效写入）</li>
     *   <li>根据数据类型分发到专门的写入方法</li>
     *   <li>记录操作日志和异常处理</li>
     * </ol>
     * 
     * @param key Redis键，不能为null
     * @param value Redis值，可以为null（将被跳过）
     * @param channel 文件通道，不能为null且必须可写
     * @throws IllegalArgumentException 当key或channel为null时抛出
     * @throws RuntimeException 当写入操作失败时抛出，包装底层IO异常
     */
    public static void writeDataToAof(final RedisBytes key, final RedisData value, 
                                     final FileChannel channel) {
        // 1. 参数验证
        validateParameters(key, value, channel);
        
        if (value == null) {
            log.debug("Redis值为null，跳过写入操作: {}", key);
            return;
        }
        
        // 2. 根据数据类型分发处理
        final String dataType = value.getClass().getSimpleName();
        try {
            switch (dataType) {
                case "RedisString":
                    writeStringToAof(key, (RedisString) value, channel);
                    break;
                case "RedisList":
                    writeListToAof(key, (RedisList) value, channel);
                    break;
                case "RedisSet":
                    writeSetToAof(key, (RedisSet) value, channel);
                    break;
                case "RedisHash":
                    writeHashToAof(key, (RedisHash) value, channel);
                    break;
                case "RedisZset":
                    writeZSetToAof(key, (RedisZset) value, channel);
                    break;
                default:
                    log.warn("不支持的数据类型: {}, key: {}", dataType, key);
            }
        } catch (final Exception e) {
            log.error("写入{}类型数据到AOF失败, key: {}", dataType, key, e);
            throw new RuntimeException("Failed to write data to AOF", e);
        }
    }
    
    // ==================== 私有辅助方法 ====================

    /**
     * 验证方法参数的有效性
     * 
     * <p>检查核心参数是否满足业务要求，确保后续操作的安全性。</p>
     * 
     * @param key Redis键，不能为null
     * @param value Redis值，允许为null（外层方法会跳过处理）
     * @param channel 文件通道，不能为null
     * @throws IllegalArgumentException 当必需参数为null时抛出
     */
    private static void validateParameters(final RedisBytes key, final RedisData value, 
                                          final FileChannel channel) {
        if (key == null) {
            throw new IllegalArgumentException("Redis key cannot be null");
        }
        if (channel == null) {
            throw new IllegalArgumentException("FileChannel cannot be null");
        }
    }    // ==================== 数据类型特化写入方法 ====================

    /**
     * 写入 String 类型数据到 AOF 文件
     * 
     * <p>将 RedisString 对象转换为对应的 SET 命令并写入 AOF。</p>
     * 
     * @param key Redis键
     * @param data String数据，不能为null
     * @param channel 文件通道，不能为null
     * @throws RuntimeException 当转换或写入失败时抛出
     */
    private static void writeStringToAof(final RedisBytes key, final RedisString data, 
                                        final FileChannel channel) {
        data.setKey(key);
        final List<Resp> resps = data.convertToResp();
        if (!resps.isEmpty()) {
            writeCommandsToChannel(resps, channel);
            log.info("已重写字符串类型数据到AOF文件，key: {}", key);
        }
    }

    /**
     * 写入 List 类型数据到 AOF 文件
     * 
     * <p>将 RedisList 对象转换为对应的 LPUSH/RPUSH 系列命令并写入 AOF。</p>
     * 
     * @param key Redis键
     * @param data List数据，不能为null
     * @param channel 文件通道，不能为null
     * @throws RuntimeException 当转换或写入失败时抛出
     */
    private static void writeListToAof(final RedisBytes key, final RedisList data, 
                                      final FileChannel channel) {
        data.setKey(key);
        final List<Resp> resps = data.convertToResp();
        if (!resps.isEmpty()) {
            writeCommandsToChannel(resps, channel);
            log.info("已重写list类型数据到AOF文件，key: {}", key);
        }
    }

    /**
     * 写入 Set 类型数据到 AOF 文件
     * 
     * <p>将 RedisSet 对象转换为对应的 SADD 命令并写入 AOF。</p>
     * 
     * @param key Redis键
     * @param data Set数据，不能为null
     * @param channel 文件通道，不能为null
     * @throws RuntimeException 当转换或写入失败时抛出
     */
    private static void writeSetToAof(final RedisBytes key, final RedisSet data, 
                                     final FileChannel channel) {
        data.setKey(key);
        final List<Resp> resps = data.convertToResp();
        if (!resps.isEmpty()) {
            writeCommandsToChannel(resps, channel);
            log.info("已重写set类型到AOF文件，key: {}", key);
        }
    }

    /**
     * 写入 Hash 类型数据到 AOF 文件
     * 
     * <p>将 RedisHash 对象转换为对应的 HSET 命令并写入 AOF。</p>
     * 
     * @param key Redis键
     * @param data Hash数据，不能为null
     * @param channel 文件通道，不能为null
     * @throws RuntimeException 当转换或写入失败时抛出
     */
    private static void writeHashToAof(final RedisBytes key, final RedisHash data, 
                                      final FileChannel channel) {
        data.setKey(key);
        final List<Resp> resps = data.convertToResp();
        if (!resps.isEmpty()) {
            writeCommandsToChannel(resps, channel);
            log.info("已重写哈希类型到AOF文件，key: {}", key);
        }
    }

    /**
     * 写入 ZSet 类型数据到 AOF 文件
     * 
     * <p>将 RedisZset 对象转换为对应的 ZADD 命令并写入 AOF。</p>
     * 
     * @param key Redis键
     * @param data ZSet数据，不能为null
     * @param channel 文件通道，不能为null
     * @throws RuntimeException 当转换或写入失败时抛出
     */
    private static void writeZSetToAof(final RedisBytes key, final RedisZset data, 
                                      final FileChannel channel) {
        data.setKey(key);
        final List<Resp> resps = data.convertToResp();
        if (!resps.isEmpty()) {
            writeCommandsToChannel(resps, channel);
            log.info("已重写zset类型数据到AOF文件，key: {}", key);
        }
    }
    
    // ==================== 底层写入操作方法 ====================

    /**
     * 将 RESP 命令列表写入文件通道
     * 
     * <p>负责将转换后的 RESP 协议命令批量写入到文件通道中。
     * 使用 Netty ByteBuf 进行高效的内存管理和编码操作。</p>
     * 
     * <p>关键特性：</p>
     * <ul>
     *   <li>确保完整写入：循环写入直到所有数据都被写入</li>
     *   <li>自动资源管理：自动释放 ByteBuf 避免内存泄漏</li>
     *   <li>异常安全：保证在异常情况下也能正确释放资源</li>
     * </ul>
     * 
     * @param resps 命令列表，不能为null，可以为空（将被跳过）
     * @param channel 文件通道，不能为null且必须可写
     * @throws RuntimeException 当写入失败时抛出，包装底层IO异常
     */
    private static void writeCommandsToChannel(final List<Resp> resps, 
                                              final FileChannel channel) {
        if (resps.isEmpty()) {
            return;
        }
        
        for (final Resp command : resps) {
            ByteBuf buf = null;
            try {
                buf = Unpooled.buffer();
                command.encode(command, buf);
                final ByteBuffer byteBuffer = buf.nioBuffer();
                
                // 确保完整写入
                int written = 0;
                while (written < byteBuffer.remaining()) {
                    try {
                        written += channel.write(byteBuffer);
                    } catch (final Exception e) {
                        log.error("写入AOF文件时发生错误", e);
                        throw new RuntimeException("Failed to write to AOF file", e);
                    }
                }
            } finally {
                if (buf != null) {
                    buf.release();
                }
            }
        }
    }
}

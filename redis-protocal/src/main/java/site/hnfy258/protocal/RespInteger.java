package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * Redis整数类型
 * 
 * <p>负责Redis RESP协议中整数类型的实现，提供高效的整数处理和编码功能。
 * 基于享元模式设计，通过缓存常用整数值来优化性能和内存使用。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>整数编码 - 高效的RESP格式编码</li>
 *     <li>常量池优化 - 预分配常用整数实例</li>
 *     <li>范围缓存 - 支持-10到127的整数缓存</li>
 *     <li>工厂方法 - 智能的实例创建策略</li>
 * </ul>
 * 
 * <p>预定义常量：
 * <ul>
 *     <li>ZERO - 数值0</li>
 *     <li>ONE - 数值1</li>
 *     <li>MINUS_ONE - 数值-1</li>
 *     <li>TWO - 数值2</li>
 *     <li>THREE - 数值3</li>
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *     <li>整数缓存 - 预缓存常用整数范围</li>
 *     <li>实例复用 - 常用值使用共享实例</li>
 *     <li>编码优化 - 高效的整数序列化</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
public class RespInteger extends Resp {
    /** 缓存范围下限 */
    private static final int CACHE_LOW = -10;
    
    /** 缓存范围上限 */
    private static final int CACHE_HIGH = 127;
    
    /** 整数实例缓存数组 */
    private static final RespInteger[] CACHE = new RespInteger[CACHE_HIGH - CACHE_LOW + 1];
    
    static {
        // 初始化缓存数组
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new RespInteger(i + CACHE_LOW);
        }
    }
    
    /** 预定义的常用整数实例 */
    public static final RespInteger ZERO = CACHE[0 - CACHE_LOW];
    public static final RespInteger ONE = CACHE[1 - CACHE_LOW];
    public static final RespInteger MINUS_ONE = CACHE[-1 - CACHE_LOW];
    public static final RespInteger TWO = CACHE[2 - CACHE_LOW];
    public static final RespInteger THREE = CACHE[3 - CACHE_LOW];
    
    /** 整数值 */
    private final int content;
    
    /**
     * 工厂方法：获取 RespInteger 实例
     * 
     * <p>对于常用值范围内的整数，返回缓存的实例；
     * 对于其他值，创建新实例。</p>
     * 
     * @param value 整数值
     * @return RespInteger 实例
     */
    public static RespInteger valueOf(final int value) {
        if (value >= CACHE_LOW && value <= CACHE_HIGH) {
            return CACHE[value - CACHE_LOW];
        }
        return new RespInteger(value);
    }
    
    /**
     * 兼容性构造函数
     * 
     * @param content 整数值
     * @deprecated 推荐使用 {@link #valueOf(int)} 以获得更好的性能
     */
    @Deprecated
    public RespInteger(final int content) {
        this.content = content;
    }

    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte(':');
        writeIntegerAsBytes(byteBuf, ((RespInteger) resp).getContent());
        byteBuf.writeBytes(CRLF);
    }
}

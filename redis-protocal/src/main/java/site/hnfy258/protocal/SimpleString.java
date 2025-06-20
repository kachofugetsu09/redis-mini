package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import site.hnfy258.datastructure.RedisBytes;

/**
 * Redis简单字符串类型
 * 
 * <p>负责Redis RESP协议中简单字符串类型的实现，提供高效的字符串处理功能。
 * 基于享元模式设计，通过缓存常用响应来优化内存使用和性能。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>字符串编码 - 高效的RESP格式编码</li>
 *     <li>常量复用 - 预分配常用响应字符串</li>
 *     <li>内存优化 - 使用RedisBytes实现高效存储</li>
 *     <li>零拷贝 - 支持高效的内存操作</li>
 * </ul>
 * 
 * <p>预定义常量：
 * <ul>
 *     <li>OK - 成功响应</li>
 *     <li>PONG - 心跳响应</li>
 *     <li>QUEUED - 事务队列响应</li>
 * </ul>
 * 
 * <p>使用建议：
 * <ul>
 *     <li>优先使用valueOf工厂方法</li>
 *     <li>对于常用响应，使用预定义常量</li>
 *     <li>自定义响应使用构造函数</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
public class SimpleString extends Resp {
    /** 预定义的成功响应 */
    public static final SimpleString OK = new SimpleString("OK");
    
    /** 预定义的心跳响应 */
    public static final SimpleString PONG = new SimpleString("PONG");
    
    /** 预定义的事务队列响应 */
    public static final SimpleString QUEUED = new SimpleString("QUEUED");
    
    /** 字符串内容 */
    private final String content;
    
    /** 字符串的字节表示 */
    private final RedisBytes contentBytes;

    /**
     * 构造函数
     * 
     * @param content 字符串内容
     */
    public SimpleString(final String content) {
        this.content = content;
        this.contentBytes = RedisBytes.fromString(content);
    }
    
    /**
     * 工厂方法：获取 SimpleString 实例
     * 
     * <p>对于常用字符串，返回缓存的实例；对于其他字符串，创建新实例。</p>
     * 
     * @param content 字符串内容
     * @return SimpleString 实例
     */
    public static SimpleString valueOf(final String content) {
        if ("OK".equals(content)) {
            return OK;
        } else if ("PONG".equals(content)) {
            return PONG;
        } else if ("QUEUED".equals(content)) {
            return QUEUED;
        }
        return new SimpleString(content);
    }

    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte('+');
        final SimpleString simpleString = (SimpleString) resp;
        byteBuf.writeBytes(simpleString.contentBytes.getBytesUnsafe());
        byteBuf.writeBytes(CRLF);
    }
}

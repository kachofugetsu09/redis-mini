package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.nio.charset.StandardCharsets;

/**
 * Redis错误消息类型
 * 
 * <p>负责Redis RESP协议中错误消息类型的实现，提供标准化的错误响应格式。
 * 用于向客户端传递服务器端的错误信息，支持多种错误类型和格式化。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>错误编码 - 将错误消息编码为RESP格式</li>
 *     <li>标准化处理 - 统一的错误消息格式</li>
 *     <li>字符集支持 - 使用UTF-8编码确保兼容性</li>
 *     <li>内存优化 - 高效的错误消息序列化</li>
 * </ul>
 * 
 * <p>错误格式：
 * <ul>
 *     <li>语法："-Error message\r\n"</li>
 *     <li>示例："-ERR unknown command 'foobar'"</li>
 *     <li>示例："-WRONGTYPE Operation against a key holding the wrong kind of value"</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Getter
public class Errors extends Resp {
    /** 错误消息内容 */
    private final String content;

    /**
     * 创建错误消息实例
     * 
     * @param content 错误消息内容
     */
    public Errors(String content) {
        this.content = content;
    }

    /**
     * 将错误消息编码为RESP格式
     * 
     * @param resp 响应对象
     * @param byteBuf 目标缓冲区
     */
    @Override
    public void encode(Resp resp, ByteBuf byteBuf) {
        byteBuf.writeByte('-');
        byteBuf.writeBytes(((Errors) resp).getContent().getBytes(StandardCharsets.UTF_8));
        byteBuf.writeBytes(CRLF);
    }
}

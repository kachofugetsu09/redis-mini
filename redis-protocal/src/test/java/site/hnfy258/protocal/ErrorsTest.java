package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorsTest {
    
    @Test
    public void testConstructor() {
        // 测试构造函数
        String errorMessage = "Error message";
        Errors error = new Errors(errorMessage);
        assertEquals(errorMessage, error.getContent());
    }
    
    @Test
    public void testEncode() {
        // 测试编码普通错误消息
        String errorMessage = "Invalid command";
        Errors error = new Errors(errorMessage);
        ByteBuf buf = Unpooled.buffer();
        
        try {
            error.encode(error, buf);
            
            // 验证编码结果：-Invalid command\r\n
            assertEquals("-Invalid command\r\n", 
                buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeWithSpecialCharacters() {
        // 测试包含特殊字符的错误消息
        String errorMessage = "Error: Key 'test:123' not found!";
        Errors error = new Errors(errorMessage);
        ByteBuf buf = Unpooled.buffer();
        
        try {
            error.encode(error, buf);
            
            // 验证编码结果
            assertEquals("-Error: Key 'test:123' not found!\r\n", 
                buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeEmptyMessage() {
        // 测试空错误消息
        Errors error = new Errors("");
        ByteBuf buf = Unpooled.buffer();
        
        try {
            error.encode(error, buf);
            
            // 验证编码结果：-\r\n
            assertEquals("-\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
} 
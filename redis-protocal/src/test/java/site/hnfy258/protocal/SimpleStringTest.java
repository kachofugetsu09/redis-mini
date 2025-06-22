package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SimpleStringTest {
    
    @Test
    public void testConstructor() {
        // 测试构造函数
        String content = "test string";
        SimpleString simpleString = new SimpleString(content);
        assertEquals(content, simpleString.getContent());
    }
    
    @Test
    public void testValueOf() {
        // 测试常用值的缓存
        SimpleString ok = SimpleString.valueOf("OK");
        SimpleString pong = SimpleString.valueOf("PONG");
        SimpleString queued = SimpleString.valueOf("QUEUED");
        
        // 验证使用缓存实例
        assertSame(SimpleString.OK, ok);
        assertSame(SimpleString.PONG, pong);
        assertSame(SimpleString.QUEUED, queued);
        
        // 测试非缓存值
        SimpleString custom = SimpleString.valueOf("custom");
        assertEquals("custom", custom.getContent());
    }
    
    @Test
    public void testEncode() {
        // 测试编码普通字符串
        SimpleString simpleString = new SimpleString("hello");
        ByteBuf buf = Unpooled.buffer();
        
        try {
            simpleString.encode(simpleString, buf);
            
            // 验证编码结果：+hello\r\n
            assertEquals("+hello\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeEmpty() {
        // 测试编码空字符串
        SimpleString empty = new SimpleString("");
        ByteBuf buf = Unpooled.buffer();
        
        try {
            empty.encode(empty, buf);
            
            // 验证编码结果：+\r\n
            assertEquals("+\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeWithSpecialCharacters() {
        // 测试包含特殊字符的字符串
        SimpleString special = new SimpleString("hello:world@123");
        ByteBuf buf = Unpooled.buffer();
        
        try {
            special.encode(special, buf);
            
            // 验证编码结果：+hello:world@123\r\n
            assertEquals("+hello:world@123\r\n", 
                buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testPreallocatedConstants() {
        // 测试预分配的常量
        assertNotNull(SimpleString.OK);
        assertNotNull(SimpleString.PONG);
        assertNotNull(SimpleString.QUEUED);
        
        assertEquals("OK", SimpleString.OK.getContent());
        assertEquals("PONG", SimpleString.PONG.getContent());
        assertEquals("QUEUED", SimpleString.QUEUED.getContent());
    }
    
    @Test
    public void testEncodePreallocatedConstants() {
        // 测试编码预分配的常量
        ByteBuf buf = Unpooled.buffer();
        
        try {
            SimpleString.OK.encode(SimpleString.OK, buf);
            assertEquals("+OK\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
            
            buf.clear();
            SimpleString.PONG.encode(SimpleString.PONG, buf);
            assertEquals("+PONG\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
            
            buf.clear();
            SimpleString.QUEUED.encode(SimpleString.QUEUED, buf);
            assertEquals("+QUEUED\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
} 
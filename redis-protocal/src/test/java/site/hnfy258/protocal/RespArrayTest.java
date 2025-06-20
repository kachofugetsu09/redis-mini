package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class RespArrayTest {
    
    @Test
    public void testConstructor() {
        // 测试构造函数
        Resp[] content = new Resp[]{
            BulkString.fromString("hello"),
            BulkString.fromString("world")
        };
        RespArray array = new RespArray(content);
        
        assertArrayEquals(content, array.getContent());
    }
    
    @Test
    public void testValueOf() {
        // 测试null数组
        RespArray nullArray = RespArray.valueOf(null);
        assertNull(nullArray.getContent());
        assertSame(RespArray.NULL, nullArray);
        
        // 测试空数组
        RespArray emptyArray = RespArray.valueOf(new Resp[0]);
        assertEquals(0, emptyArray.getContent().length);
        assertSame(RespArray.EMPTY, emptyArray);
        
        // 测试普通数组
        Resp[] content = new Resp[]{
            BulkString.fromString("test")
        };
        RespArray array = RespArray.valueOf(content);
        assertArrayEquals(content, array.getContent());
    }
    
    @Test
    public void testEncode() {
        // 测试编码普通数组
        Resp[] content = new Resp[]{
            BulkString.fromString("hello"),
            BulkString.fromString("world")
        };
        RespArray array = new RespArray(content);
        ByteBuf buf = Unpooled.buffer();
        
        try {
            array.encode(array, buf);
            
            // 验证编码结果：*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
            assertEquals("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", 
                buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeNullArray() {
        // 测试编码null数组
        RespArray array = RespArray.NULL;
        ByteBuf buf = Unpooled.buffer();
        
        try {
            array.encode(array, buf);
            
            // 验证编码结果：*-1\r\n
            assertEquals("*-1\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeEmptyArray() {
        // 测试编码空数组
        RespArray array = RespArray.EMPTY;
        ByteBuf buf = Unpooled.buffer();
        
        try {
            array.encode(array, buf);
            
            // 验证编码结果：*0\r\n
            assertEquals("*0\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeNestedArray() {
        // 测试编码嵌套数组
        RespArray innerArray = new RespArray(new Resp[]{
            BulkString.fromString("inner")
        });
        RespArray outerArray = new RespArray(new Resp[]{
            BulkString.fromString("outer"),
            innerArray
        });
        
        ByteBuf buf = Unpooled.buffer();
        
        try {
            outerArray.encode(outerArray, buf);
            
            // 验证编码结果：*2\r\n$5\r\nouter\r\n*1\r\n$5\r\ninner\r\n
            assertEquals("*2\r\n$5\r\nouter\r\n*1\r\n$5\r\ninner\r\n", 
                buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testPreallocatedInstances() {
        // 测试预分配的实例
        assertNotNull(RespArray.EMPTY);
        assertNotNull(RespArray.NULL);
        
        // 验证EMPTY数组
        assertEquals(0, RespArray.EMPTY.getContent().length);
        
        // 验证NULL数组
        assertNull(RespArray.NULL.getContent());
        
        // 验证实例复用
        assertSame(RespArray.EMPTY, RespArray.valueOf(new Resp[0]));
        assertSame(RespArray.NULL, RespArray.valueOf(null));
    }
} 
package site.hnfy258.protocal.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import site.hnfy258.protocal.*;
import static org.junit.jupiter.api.Assertions.*;

public class RespEncoderTest {
    
    @Test
    public void testEncodeSimpleString() {
        // 创建测试通道
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        // 编码简单字符串
        SimpleString simpleString = new SimpleString("OK");
        assertTrue(channel.writeOutbound(simpleString));
        
        // 验证结果
        ByteBuf buf = channel.readOutbound();
        assertEquals("+OK\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        // 清理资源
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeError() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        Errors error = new Errors("Error message");
        assertTrue(channel.writeOutbound(error));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("-Error message\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeInteger() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        RespInteger integer = RespInteger.valueOf(1000);
        assertTrue(channel.writeOutbound(integer));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals(":1000\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeBulkString() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        BulkString bulkString = BulkString.fromString("hello");
        assertTrue(channel.writeOutbound(bulkString));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("$5\r\nhello\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeArray() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        Resp[] content = new Resp[]{
            BulkString.fromString("hello"),
            BulkString.fromString("world")
        };
        RespArray array = new RespArray(content);
        assertTrue(channel.writeOutbound(array));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", 
            buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeNullBulkString() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        BulkString nullString = BulkString.create((byte[]) null);
        assertTrue(channel.writeOutbound(nullString));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("$-1\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeEmptyArray() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        RespArray emptyArray = RespArray.EMPTY;
        assertTrue(channel.writeOutbound(emptyArray));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("*0\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeNullArray() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        RespArray nullArray = RespArray.NULL;
        assertTrue(channel.writeOutbound(nullArray));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("*-1\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeNestedArray() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        RespArray innerArray = new RespArray(new Resp[]{
            BulkString.fromString("inner")
        });
        RespArray outerArray = new RespArray(new Resp[]{
            BulkString.fromString("outer"),
            innerArray
        });
        
        assertTrue(channel.writeOutbound(outerArray));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("*2\r\n$5\r\nouter\r\n*1\r\n$5\r\ninner\r\n", 
            buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
    
    @Test
    public void testEncodeMixedTypes() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespEncoder());
        
        Resp[] content = new Resp[]{
            SimpleString.valueOf("OK"),
            RespInteger.valueOf(42),
            BulkString.fromString("hello"),
            new Errors("error")
        };
        RespArray array = new RespArray(content);
        
        assertTrue(channel.writeOutbound(array));
        
        ByteBuf buf = channel.readOutbound();
        assertEquals("*4\r\n+OK\r\n:42\r\n$5\r\nhello\r\n-error\r\n", 
            buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
        channel.finish();
    }
} 
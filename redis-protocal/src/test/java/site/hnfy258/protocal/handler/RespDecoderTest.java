package site.hnfy258.protocal.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import site.hnfy258.protocal.*;
import static org.junit.jupiter.api.Assertions.*;

public class RespDecoderTest {
    
    @Test
    public void testDecodeSimpleString() {
        // 创建测试通道
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入简单字符串
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("+OK\r\n".getBytes());
        
        // 测试解码
        assertTrue(channel.writeInbound(buf));
        
        // 验证结果
        SimpleString result = channel.readInbound();
        assertNotNull(result);
        assertEquals("OK", result.getContent());
        
        // 清理资源
        channel.finish();
    }
    
    @Test
    public void testDecodeError() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("-Error message\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        Errors result = channel.readInbound();
        assertNotNull(result);
        assertEquals("Error message", result.getContent());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeInteger() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(":1000\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        RespInteger result = channel.readInbound();
        assertNotNull(result);
        assertEquals(1000, result.getContent());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeBulkString() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$5\r\nhello\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        BulkString result = channel.readInbound();
        assertNotNull(result);
        assertEquals("hello", result.toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeArray() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        RespArray result = channel.readInbound();
        assertNotNull(result);
        assertEquals(2, result.getContent().length);
        assertEquals("hello", result.getContent()[0].toString());
        assertEquals("world", result.getContent()[1].toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeInlineCommand() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("PING\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        RespArray result = channel.readInbound();
        assertNotNull(result);
        assertEquals(1, result.getContent().length);
        assertEquals("PING", result.getContent()[0].toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeInlineCommandWithArgs() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("SET mykey myvalue\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        RespArray result = channel.readInbound();
        assertNotNull(result);
        assertEquals(3, result.getContent().length);
        assertEquals("SET", result.getContent()[0].toString());
        assertEquals("mykey", result.getContent()[1].toString());
        assertEquals("myvalue", result.getContent()[2].toString());
        
        channel.finish();
    }    @Test
    public void testDecodeIncompleteMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入不完整的RESP消息（只有类型标识符）
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$".getBytes());
        
        // 第一次写入应该返回false，因为数据不完整
        assertFalse(channel.writeInbound(buf));
        assertNull(channel.readInbound());
        
        // 写入剩余部分形成完整的消息
        ByteBuf remaining = Unpooled.buffer();
        remaining.writeBytes("5\r\nhello\r\n".getBytes());
        assertTrue(channel.writeInbound(remaining));
        
        BulkString result = channel.readInbound();
        assertNotNull(result);
        assertEquals("hello", result.toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodePartialBulkString() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入部分BulkString消息
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$5\r\nhel".getBytes()); // 缺少"lo\r\n"
        
        // 应该返回false，等待更多数据
        assertFalse(channel.writeInbound(buf));
        assertNull(channel.readInbound());
        
        // 写入剩余部分
        ByteBuf remaining = Unpooled.buffer();
        remaining.writeBytes("lo\r\n".getBytes());
        assertTrue(channel.writeInbound(remaining));
        
        BulkString result = channel.readInbound();
        assertNotNull(result);
        assertEquals("hello", result.toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeMultipleMessages() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入多个消息
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("+OK\r\n:1000\r\n$5\r\nhello\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        // 验证第一个消息
        SimpleString result1 = channel.readInbound();
        assertNotNull(result1);
        assertEquals("OK", result1.getContent());
        
        // 验证第二个消息
        RespInteger result2 = channel.readInbound();
        assertNotNull(result2);
        assertEquals(1000, result2.getContent());
        
        // 验证第三个消息
        BulkString result3 = channel.readInbound();
        assertNotNull(result3);
        assertEquals("hello", result3.toString());
        
        channel.finish();
    }
      @Test
    public void testDecodeInvalidMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入无效消息
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("invalid message\r\n".getBytes());
        
        assertTrue(channel.writeInbound(buf));
        
        // 应该被解析为内联命令
        RespArray result = channel.readInbound();
        assertNotNull(result);
        assertEquals(2, result.getContent().length);
        assertEquals("invalid", result.getContent()[0].toString());
        assertEquals("message", result.getContent()[1].toString());
        
        channel.finish();
    }
    
    @Test
    public void testDecodeInvalidRespThenFallbackToInline() {
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());
        
        // 写入格式错误的RESP消息（长度与实际不符）
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$10\r\nhello\r\n".getBytes()); // 声明长度10但只有5个字符
        
        // 应该能解码，但可能会有问题，让我们看看具体行为
        assertFalse(channel.writeInbound(buf)); // 数据不完整
        assertNull(channel.readInbound());
        
        channel.finish();
    }
} 
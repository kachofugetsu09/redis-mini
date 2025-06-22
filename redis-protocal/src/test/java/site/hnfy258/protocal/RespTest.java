package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class RespTest {
    
    @Test
    public void testDecodeSimpleString() {
        // 测试解码简单字符串
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("+OK\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof SimpleString);
            assertEquals("OK", ((SimpleString) resp).getContent());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeError() {
        // 测试解码错误消息
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("-Error message\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof Errors);
            assertEquals("Error message", ((Errors) resp).getContent());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeInteger() {
        // 测试解码整数
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(":1000\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof RespInteger);
            assertEquals(1000, ((RespInteger) resp).getContent());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeBulkString() {
        // 测试解码批量字符串
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$5\r\nhello\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof BulkString);
            assertEquals("hello", resp.toString());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeNullBulkString() {
        // 测试解码null批量字符串
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("$-1\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof BulkString);
            assertNull(((BulkString) resp).getContent());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeArray() {
        // 测试解码数组
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof RespArray);
            RespArray array = (RespArray) resp;
            assertEquals(2, array.getContent().length);
            assertEquals("hello", array.getContent()[0].toString());
            assertEquals("world", array.getContent()[1].toString());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeNullArray() {
        // 测试解码null数组
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*-1\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof RespArray);
            assertNull(((RespArray) resp).getContent());
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testDecodeEmptyArray() {
        // 测试解码空数组
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*0\r\n".getBytes());
        
        try {
            Resp resp = Resp.decode(buf);
            assertTrue(resp instanceof RespArray);
            assertEquals(0, ((RespArray) resp).getContent().length);
        } finally {
            buf.release();
        }
    }
      @Test
    public void testDecodeInvalidType() {
        // 测试解码无效类型
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("X1\r\n".getBytes());
        
        try {
            assertThrows(IllegalArgumentException.class, () -> {
                Resp.decode(buf);
            });
        } finally {
            buf.release();
        }
    }
      @Test
    public void testDecodeIncompleteMessage() {
        // 测试解码不完整消息 - 应该返回null而不是抛出异常
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("+OK".getBytes()); // 缺少\r\n
        
        try {
            Resp result = Resp.decode(buf);
            assertNull(result, "不完整的消息应该返回null");
            assertEquals(0, buf.readerIndex(), "读指针应该回滚到初始位置");
        } finally {
            buf.release();
        }
    }
} 
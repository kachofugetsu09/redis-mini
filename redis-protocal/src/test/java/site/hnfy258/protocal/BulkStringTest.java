package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BulkStringTest {
    
    @Test
    public void testCreateBulkString() {
        // 测试创建普通BulkString
        byte[] content = "hello".getBytes();
        BulkString bs = BulkString.create(content);
        assertNotNull(bs);
        assertArrayEquals(content, bs.getContent().getBytes());
        
        // 测试创建null BulkString
        BulkString nullBs = BulkString.create((byte[]) null);
        assertNull(nullBs.getContent());
    }
    
    @Test
    public void testWrapTrusted() {
        // 测试零拷贝创建
        byte[] content = "world".getBytes();
        BulkString bs = BulkString.wrapTrusted(content);
        assertNotNull(bs);
        assertArrayEquals(content, bs.getContent().getBytes());
        
        // 测试null值
        BulkString nullBs = BulkString.wrapTrusted(null);
        assertNull(nullBs.getContent());
    }
    
    @Test
    public void testFromString() {
        // 测试从字符串创建
        String str = "test string";
        BulkString bs = BulkString.fromString(str);
        assertNotNull(bs);
        assertEquals(str, bs.toString());
        
        // 测试null字符串
        BulkString nullBs = BulkString.fromString(null);
        assertNull(nullBs.getContent());
    }
    
    @Test
    public void testEncode() {
        // 测试编码普通字符串
        BulkString bs = BulkString.fromString("hello");
        ByteBuf buf = Unpooled.buffer();
        bs.encode(bs, buf);
        
        // 验证编码结果：$5\r\nhello\r\n
        assertEquals("$5\r\nhello\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        // 测试编码空字符串
        buf.clear();
        BulkString emptyBs = BulkString.fromString("");
        emptyBs.encode(emptyBs, buf);
        assertEquals("$0\r\n\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        // 测试编码null
        buf.clear();
        BulkString nullBs = BulkString.create((byte[]) null);
        nullBs.encode(nullBs, buf);
        assertEquals("$-1\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        
        buf.release();
    }
    
    @Test
    public void testPreallocatedConstants() {
        // 测试预分配的常量
        assertNotNull(BulkString.SET);
        assertNotNull(BulkString.GET);
        assertNotNull(BulkString.DEL);
        assertNotNull(BulkString.PING);
        assertNotNull(BulkString.PONG);
        assertNotNull(BulkString.OK);
        
        assertEquals("SET", BulkString.SET.toString());
        assertEquals("GET", BulkString.GET.toString());
        assertEquals("DEL", BulkString.DEL.toString());
        assertEquals("PING", BulkString.PING.toString());
        assertEquals("PONG", BulkString.PONG.toString());
        assertEquals("OK", BulkString.OK.toString());
    }
} 
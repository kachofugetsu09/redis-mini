package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class RespIntegerTest {
    
    @Test
    public void testConstructor() {
        // 测试构造函数
        RespInteger integer = new RespInteger(42);
        assertEquals(42, integer.getContent());
    }
    
    @Test
    public void testValueOf() {
        // 测试缓存范围内的值
        RespInteger zero = RespInteger.valueOf(0);
        RespInteger one = RespInteger.valueOf(1);
        RespInteger minusOne = RespInteger.valueOf(-1);
        
        // 验证使用缓存实例
        assertSame(RespInteger.ZERO, zero);
        assertSame(RespInteger.ONE, one);
        assertSame(RespInteger.MINUS_ONE, minusOne);
        
        // 测试缓存范围外的值
        RespInteger large = RespInteger.valueOf(1000);
        assertEquals(1000, large.getContent());
        
        RespInteger negative = RespInteger.valueOf(-1000);
        assertEquals(-1000, negative.getContent());
    }
    
    @Test
    public void testEncode() {
        // 测试编码正数
        RespInteger positive = RespInteger.valueOf(42);
        ByteBuf buf = Unpooled.buffer();
        
        try {
            positive.encode(positive, buf);
            
            // 验证编码结果：:42\r\n
            assertEquals(":42\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeNegative() {
        // 测试编码负数
        RespInteger negative = RespInteger.valueOf(-42);
        ByteBuf buf = Unpooled.buffer();
        
        try {
            negative.encode(negative, buf);
            
            // 验证编码结果：:-42\r\n
            assertEquals(":-42\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testEncodeZero() {
        // 测试编码零
        RespInteger zero = RespInteger.ZERO;
        ByteBuf buf = Unpooled.buffer();
        
        try {
            zero.encode(zero, buf);
            
            // 验证编码结果：:0\r\n
            assertEquals(":0\r\n", buf.toString(io.netty.util.CharsetUtil.UTF_8));
        } finally {
            buf.release();
        }
    }
    
    @Test
    public void testPreallocatedConstants() {
        // 测试预分配的常量
        assertNotNull(RespInteger.ZERO);
        assertNotNull(RespInteger.ONE);
        assertNotNull(RespInteger.MINUS_ONE);
        assertNotNull(RespInteger.TWO);
        assertNotNull(RespInteger.THREE);
        
        assertEquals(0, RespInteger.ZERO.getContent());
        assertEquals(1, RespInteger.ONE.getContent());
        assertEquals(-1, RespInteger.MINUS_ONE.getContent());
        assertEquals(2, RespInteger.TWO.getContent());
        assertEquals(3, RespInteger.THREE.getContent());
    }
    
    @Test
    public void testCacheRange() {
        // 测试缓存范围
        for (int i = -10; i <= 127; i++) {
            RespInteger first = RespInteger.valueOf(i);
            RespInteger second = RespInteger.valueOf(i);
            
            // 验证在缓存范围内的值使用相同实例
            assertSame(first, second, "Cache failed for value: " + i);
        }
        
        // 测试缓存范围外的值
        RespInteger outOfRange1 = RespInteger.valueOf(128);
        RespInteger outOfRange2 = RespInteger.valueOf(128);
        
        // 验证缓存范围外的值创建新实例
        assertNotSame(outOfRange1, outOfRange2);
    }
} 
package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisString类的单元测试
 * 
 * <p>全面测试RedisString的各种功能，包括：
 * <ul>
 *     <li>基本的字符串操作</li>
 *     <li>过期时间管理</li>
 *     <li>数值递增操作</li>
 *     <li>Redis协议转换</li>
 *     <li>边界条件和异常情况</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@DisplayName("RedisString单元测试")
class RedisStringTest {

    private RedisString redisString;
    private Sds testSds;
    private RedisBytes testKey;

    @BeforeEach
    void setUp() {
        // 1. 准备测试数据
        testSds = Sds.create("test-value".getBytes());
        redisString = new RedisString(testSds);
        testKey = RedisBytes.fromString("test-key");
        redisString.setKey(testKey);
    }

    @Test
    @DisplayName("测试基本构造和获取值")
    void testBasicConstructionAndGetValue() {
        // 1. 验证构造函数初始化
        assertNotNull(redisString.getSds());
        assertEquals("test-value", redisString.getSds().toString());
        assertEquals(-1, redisString.timeout());

        // 2. 测试getValue缓存机制
        RedisBytes value1 = redisString.getValue();
        RedisBytes value2 = redisString.getValue();
        
        assertNotNull(value1);
        assertSame(value1, value2); // 验证缓存机制
        assertArrayEquals("test-value".getBytes(), value1.getBytes());
    }

    @Test
    @DisplayName("测试过期时间管理")
    void testTimeoutManagement() {
        // 1. 测试默认过期时间
        assertEquals(-1, redisString.timeout());

        // 2. 测试设置过期时间
        long expireTime = System.currentTimeMillis() + 10000;
        redisString.setTimeout(expireTime);
        assertEquals(expireTime, redisString.timeout());

        // 3. 测试设置永不过期
        redisString.setTimeout(-1);
        assertEquals(-1, redisString.timeout());

        // 4. 测试设置0时间
        redisString.setTimeout(0);
        assertEquals(0, redisString.timeout());
    }

    @Test
    @DisplayName("测试SDS操作")
    void testSdsOperations() {
        // 1. 测试getSds
        Sds originalSds = redisString.getSds();
        assertSame(testSds, originalSds);

        // 2. 测试setSds
        Sds newSds = Sds.create("new-value".getBytes());
        redisString.setSds(newSds);
        
        assertSame(newSds, redisString.getSds());
        assertEquals("new-value", redisString.getSds().toString());

        // 3. 验证缓存被清除
        RedisBytes newValue = redisString.getValue();
        assertArrayEquals("new-value".getBytes(), newValue.getBytes());
    }

    @Test
    @DisplayName("测试数值递增操作")
    void testIncrementOperations() {
        // 1. 测试有效数字递增
        RedisString numString = new RedisString(Sds.create("42".getBytes()));
        
        long result = numString.incr();
        assertEquals(43, result);
        assertEquals("43", numString.getSds().toString());

        // 2. 测试连续递增
        result = numString.incr();
        assertEquals(44, result);
        assertEquals("44", numString.getSds().toString());

        // 3. 测试从0开始递增
        RedisString zeroString = new RedisString(Sds.create("0".getBytes()));
        result = zeroString.incr();
        assertEquals(1, result);

        // 4. 测试负数递增
        RedisString negativeString = new RedisString(Sds.create("-1".getBytes()));
        result = negativeString.incr();
        assertEquals(0, result);

        // 5. 测试最大值边界
        RedisString maxString = new RedisString(Sds.create(String.valueOf(Long.MAX_VALUE - 1).getBytes()));
        result = maxString.incr();
        assertEquals(Long.MAX_VALUE, result);
    }

    @Test
    @DisplayName("测试递增异常情况")
    void testIncrementExceptions() {
        // 1. 测试无效数字格式
        RedisString invalidString = new RedisString(Sds.create("not-a-number".getBytes()));
        
        IllegalStateException exception = assertThrows(IllegalStateException.class, 
            invalidString::incr);
        assertEquals("value is not a number", exception.getMessage());

        // 2. 测试空字符串
        RedisString emptyString = new RedisString(Sds.create("".getBytes()));
        
        exception = assertThrows(IllegalStateException.class, 
            emptyString::incr);
        assertEquals("value is not a number", exception.getMessage());        // 3. 测试浮点数格式
        RedisString floatString = new RedisString(Sds.create("3.14".getBytes()));
        
        exception = assertThrows(IllegalStateException.class, 
            floatString::incr);
        assertEquals("value is not a number", exception.getMessage());

        // 4. 测试长整型最大值的递增（会发生溢出但不会抛出异常）
        RedisString maxLongString = new RedisString(Sds.create(String.valueOf(Long.MAX_VALUE).getBytes()));
        
        // Long.MAX_VALUE + 1 会溢出变为 Long.MIN_VALUE，但不会抛出异常
        long result = maxLongString.incr();
        assertEquals(Long.MIN_VALUE, result);
    }@Test
    @DisplayName("测试Redis协议转换")
    void testConvertToResp() {
        // 1. 测试正常转换
        List<Resp> respList = redisString.convertToResp();
        
        assertNotNull(respList);
        assertEquals(1, respList.size());
        
        assertTrue(respList.get(0) instanceof RespArray);
        RespArray respArray = (RespArray) respList.get(0);
        Resp[] commands = respArray.getContent();
        
        assertEquals(3, commands.length);
        assertTrue(commands[0] instanceof BulkString);
        assertTrue(commands[1] instanceof BulkString);
        assertTrue(commands[2] instanceof BulkString);
        
        BulkString cmd = (BulkString) commands[0];
        BulkString key = (BulkString) commands[1];
        BulkString value = (BulkString) commands[2];
        
        assertEquals("SET", new String(cmd.getContent().getBytes()));
        assertEquals("test-key", new String(key.getContent().getBytes()));
        assertEquals("test-value", new String(value.getContent().getBytes()));

        // 2. 测试空值情况
        RedisString nullString = new RedisString(null);
        List<Resp> emptyResp = nullString.convertToResp();
        
        assertNotNull(emptyResp);
        assertTrue(emptyResp.isEmpty());
    }

    @Test
    @DisplayName("测试边界条件")
    void testBoundaryConditions() {
        // 1. 测试空字符串值
        RedisString emptyString = new RedisString(Sds.create("".getBytes()));
        RedisBytes emptyValue = emptyString.getValue();
        
        assertNotNull(emptyValue);
        assertEquals(0, emptyValue.getBytes().length);        // 2. 测试很长的字符串
        StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longStringBuilder.append("a");
        }
        String longString = longStringBuilder.toString();
        RedisString longRedisString = new RedisString(Sds.create(longString.getBytes()));
        RedisBytes longValue = longRedisString.getValue();
        
        assertEquals(1000, longValue.getBytes().length);
        assertEquals(longString, new String(longValue.getBytes()));

        // 3. 测试特殊字符
        String specialString = "!@#$%^&*()中文\n\t\r";
        RedisString specialRedisString = new RedisString(Sds.create(specialString.getBytes()));
        RedisBytes specialValue = specialRedisString.getValue();
        
        assertEquals(specialString, new String(specialValue.getBytes()));
    }

    @Test
    @DisplayName("测试缓存失效机制")
    void testCacheInvalidation() {
        // 1. 获取初始缓存值
        RedisBytes cachedValue1 = redisString.getValue();
        RedisBytes cachedValue2 = redisString.getValue();
        assertSame(cachedValue1, cachedValue2);

        // 2. 修改SDS后验证缓存失效
        Sds newSds = Sds.create("modified-value".getBytes());
        redisString.setSds(newSds);
        
        RedisBytes newCachedValue = redisString.getValue();
        assertNotSame(cachedValue1, newCachedValue);
        assertEquals("modified-value", new String(newCachedValue.getBytes()));

        // 3. 递增操作后验证缓存失效
        RedisString numString = new RedisString(Sds.create("100".getBytes()));
        RedisBytes numValue1 = numString.getValue();
        
        numString.incr();
        RedisBytes numValue2 = numString.getValue();
        
        assertNotSame(numValue1, numValue2);
        assertEquals("101", new String(numValue2.getBytes()));
    }

}

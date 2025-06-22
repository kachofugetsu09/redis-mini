package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis哈希表数据结构测试类
 * 
 * <p>测试RedisHash类的所有功能，包括：
 * <ul>
 *     <li>字段的设置和获取</li>
 *     <li>字段的删除操作</li>
 *     <li>过期时间管理</li>
 *     <li>Redis协议转换</li>
 *     <li>边界条件和异常情况</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
class RedisHashTest {

    /** 测试用的常量值 */
    private static final long DEFAULT_TIMEOUT = -1L;
    private static final long CUSTOM_TIMEOUT = 1000L;
    private static final String TEST_FIELD_NAME = "testField";
    private static final String TEST_VALUE = "testValue";
    private static final String TEST_KEY_NAME = "testKey";

    /** 被测试的Redis哈希表实例 */
    private RedisHash redisHash;
    
    /** 测试用的键、字段和值 */
    private RedisBytes testKey;
    private RedisBytes testField;
    private RedisBytes testValue;

    /**
     * 每个测试方法执行前的初始化
     */
    @BeforeEach
    void setUp() {
        // 1. 初始化RedisHash实例
        redisHash = new RedisHash();
        
        // 2. 创建测试用的键值对象
        testKey = RedisBytes.fromString(TEST_KEY_NAME);
        testField = RedisBytes.fromString(TEST_FIELD_NAME);
        testValue = RedisBytes.fromString(TEST_VALUE);
        
        // 3. 设置测试键
        redisHash.setKey(testKey);
    }

    /**
     * 测试RedisHash的构造函数
     */
    @Test
    void testConstructor() {
        // 1. 创建新的RedisHash实例
        RedisHash newHash = new RedisHash();
        
        // 2. 验证初始状态
        assertNotNull(newHash.getHash(), "哈希表不应为null");
        assertEquals(DEFAULT_TIMEOUT, newHash.timeout(), "默认过期时间应为-1");
        assertEquals(0, newHash.getHash().size(), "初始哈希表应为空");
    }

    /**
     * 测试过期时间的设置和获取
     */
    @Test
    void testTimeoutOperations() {
        // 1. 测试默认过期时间
        assertEquals(DEFAULT_TIMEOUT, redisHash.timeout(), "默认过期时间应为-1");
        
        // 2. 设置自定义过期时间
        redisHash.setTimeout(CUSTOM_TIMEOUT);
        assertEquals(CUSTOM_TIMEOUT, redisHash.timeout(), "过期时间设置失败");
        
        // 3. 设置为永不过期
        redisHash.setTimeout(DEFAULT_TIMEOUT);
        assertEquals(DEFAULT_TIMEOUT, redisHash.timeout(), "重置过期时间失败");
    }

    /**
     * 测试字段的设置操作
     */
    @Test
    void testPutOperations() {
        // 1. 测试新字段的设置
        int result = redisHash.put(testField, testValue);
        assertEquals(1, result, "新字段设置应返回1");
        assertTrue(redisHash.getHash().containsKey(testField), "字段应已存在");
        
        // 2. 测试已存在字段的更新
        RedisBytes newValue = RedisBytes.fromString("newValue");
        result = redisHash.put(testField, newValue);
        assertEquals(0, result, "更新已存在字段应返回0");
        assertEquals(newValue, redisHash.getHash().get(testField), "字段值应已更新");
        
        // 3. 测试多个字段的设置
        RedisBytes field2 = RedisBytes.fromString("field2");
        RedisBytes value2 = RedisBytes.fromString("value2");
        result = redisHash.put(field2, value2);
        assertEquals(1, result, "新字段设置应返回1");
        assertEquals(2, redisHash.getHash().size(), "哈希表应包含2个字段");
    }

    /**
     * 测试字段的删除操作
     */
    @Test
    void testDeleteOperations() {
        // 1. 准备测试数据
        redisHash.put(testField, testValue);
        RedisBytes field2 = RedisBytes.fromString("field2");
        RedisBytes value2 = RedisBytes.fromString("value2");
        redisHash.put(field2, value2);
        
        // 2. 测试删除存在的字段
        List<RedisBytes> fieldsToDelete = Arrays.asList(testField);
        int deletedCount = redisHash.del(fieldsToDelete);
        assertEquals(1, deletedCount, "应删除1个字段");
        assertFalse(redisHash.getHash().containsKey(testField), "字段应已被删除");
        
        // 3. 测试删除不存在的字段
        RedisBytes nonExistentField = RedisBytes.fromString("nonExistent");
        fieldsToDelete = Arrays.asList(nonExistentField);
        deletedCount = redisHash.del(fieldsToDelete);
        assertEquals(0, deletedCount, "删除不存在字段应返回0");
        
        // 4. 测试批量删除
        RedisBytes field3 = RedisBytes.fromString("field3");
        RedisBytes value3 = RedisBytes.fromString("value3");
        redisHash.put(field3, value3);
        
        fieldsToDelete = Arrays.asList(field2, field3, nonExistentField);
        deletedCount = redisHash.del(fieldsToDelete);
        assertEquals(2, deletedCount, "应删除2个字段");
        assertEquals(0, redisHash.getHash().size(), "哈希表应为空");
    }

    /**
     * 测试空哈希表的删除操作
     */
    @Test
    void testDeleteFromEmptyHash() {
        // 1. 对空哈希表执行删除操作
        List<RedisBytes> fieldsToDelete = Arrays.asList(testField);
        int deletedCount = redisHash.del(fieldsToDelete);
        
        // 2. 验证结果
        assertEquals(0, deletedCount, "空哈希表删除应返回0");
    }

    /**
     * 测试Redis协议转换功能
     */
    @Test
    void testConvertToResp() {
        // 1. 测试空哈希表的转换
        List<Resp> respList = redisHash.convertToResp();
        assertTrue(respList.isEmpty(), "空哈希表应返回空列表");
        
        // 2. 添加字段并测试转换
        redisHash.put(testField, testValue);
        respList = redisHash.convertToResp();
          // 3. 验证转换结果
        assertEquals(1, respList.size(), "应生成1个HSET命令");
        assertTrue(respList.get(0) instanceof RespArray, "应返回RespArray类型");
        
        RespArray respArray = (RespArray) respList.get(0);
        Resp[] commands = respArray.getContent();
        assertEquals(4, commands.length, "HSET命令应包含4个参数");
        
        // 4. 验证命令内容
        assertTrue(commands[0] instanceof BulkString, "第一个参数应为BulkString");
        BulkString command = (BulkString) commands[0];
        assertEquals("HSET", new String(command.getContent().getBytesUnsafe()), "命令应为HSET");
        
        // 5. 测试多字段转换
        RedisBytes field2 = RedisBytes.fromString("field2");
        RedisBytes value2 = RedisBytes.fromString("value2");
        redisHash.put(field2, value2);
        
        respList = redisHash.convertToResp();
        assertEquals(2, respList.size(), "应生成2个HSET命令");
    }

    /**
     * 测试哈希表的基本属性
     */
    @Test
    void testHashProperties() {
        // 1. 测试初始状态
        assertNotNull(redisHash.getHash(), "哈希表不应为null");
        assertEquals(0, redisHash.getHash().size(), "初始哈希表应为空");
        
        // 2. 添加字段后测试
        redisHash.put(testField, testValue);
        assertEquals(1, redisHash.getHash().size(), "哈希表大小应为1");
        
        // 3. 验证字段值
        assertEquals(testValue, redisHash.getHash().get(testField), "字段值应匹配");
    }

    /**
     * 测试边界条件
     */
    @Test
    void testBoundaryConditions() {
        // 1. 测试空字段名
        RedisBytes emptyField = RedisBytes.fromString("");
        RedisBytes emptyValue = RedisBytes.fromString("");
        
        int result = redisHash.put(emptyField, emptyValue);
        assertEquals(1, result, "空字段名应可以设置");
        assertTrue(redisHash.getHash().containsKey(emptyField), "空字段名应存在");
        
        // 2. 测试空值
        RedisBytes normalField = RedisBytes.fromString("normal");
        result = redisHash.put(normalField, emptyValue);
        assertEquals(1, result, "空值应可以设置");
        assertEquals(emptyValue, redisHash.getHash().get(normalField), "空值应正确存储");
        
        // 3. 测试删除空列表
        List<RedisBytes> emptyList = Arrays.asList();
        int deletedCount = redisHash.del(emptyList);
        assertEquals(0, deletedCount, "删除空列表应返回0");
    }

    /**
     * 测试键的设置和获取
     */
    @Test
    void testKeyOperations() {
        // 1. 测试键的设置
        RedisBytes newKey = RedisBytes.fromString("newKey");
        redisHash.setKey(newKey);
        assertEquals(newKey, redisHash.getKey(), "键设置失败");
        
        // 2. 测试null键
        redisHash.setKey(null);
        assertNull(redisHash.getKey(), "null键应可以设置");
    }

    /**
     * 测试大量数据操作的性能
     */
    @Test
    void testLargeDataOperations() {
        final int DATA_SIZE = 1000;
        
        // 1. 批量添加字段
        for (int i = 0; i < DATA_SIZE; i++) {
            RedisBytes field = RedisBytes.fromString("field" + i);
            RedisBytes value = RedisBytes.fromString("value" + i);
            redisHash.put(field, value);
        }
        
        // 2. 验证数据完整性
        assertEquals(DATA_SIZE, redisHash.getHash().size(), "数据大小应匹配");
        
        // 3. 验证随机字段
        RedisBytes randomField = RedisBytes.fromString("field500");
        RedisBytes expectedValue = RedisBytes.fromString("value500");
        assertEquals(expectedValue, redisHash.getHash().get(randomField), "随机字段值应匹配");
        
        // 4. 测试协议转换性能
        List<Resp> respList = redisHash.convertToResp();
        assertEquals(DATA_SIZE, respList.size(), "转换结果大小应匹配");
    }
    /**
     * 测试简单并发插入
     */
}

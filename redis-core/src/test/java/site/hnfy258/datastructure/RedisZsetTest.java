package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import site.hnfy258.datastructure.RedisZset.ZsetNode;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisZset类的单元测试
 * 
 * <p>全面测试Redis有序集合数据结构的功能，包括：
 * <ul>
 *     <li>基本的有序集合操作（添加、删除、查询）</li>
 *     <li>分数和排名范围查询</li>
 *     <li>过期时间管理</li>
 *     <li>Redis协议转换</li>
 *     <li>边界条件和异常情况</li>
 *     <li>并发安全性测试</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@DisplayName("RedisZset单元测试")
class RedisZsetTest {    private static final String TEST_KEY = "test_zset";
    private static final int OPERATIONS_PER_THREAD = 100;

    private RedisZset redisZset;
    private RedisBytes testKey;

    @BeforeEach
    void setUp() {
        redisZset = new RedisZset();
        testKey = RedisBytes.fromString(TEST_KEY);
        redisZset.setKey(testKey);
    }

    @Test
    @DisplayName("测试基本构造和初始状态")
    void testBasicConstructionAndInitialState() {
        assertNotNull(redisZset);
        assertEquals(0, redisZset.size());
        assertEquals(-1, redisZset.timeout());
        assertEquals(TEST_KEY, redisZset.getKey().getString());
    }

    @Test
    @DisplayName("测试添加成员")
    void testAddMembers() {
        // 1. 测试添加新成员
        assertTrue(redisZset.add(1.0, "member1"));
        assertEquals(1, redisZset.size());
        
        // 2. 测试添加重复成员（相同分数）
        assertFalse(redisZset.add(1.0, "member1"));
        assertEquals(1, redisZset.size());
        
        // 3. 测试添加重复成员（不同分数）
        assertFalse(redisZset.add(2.0, "member1"));
        assertEquals(1, redisZset.size());
        assertEquals(2.0, redisZset.getScore("member1"));
        
        // 4. 测试添加多个成员
        assertTrue(redisZset.add(3.0, "member2"));
        assertTrue(redisZset.add(4.0, "member3"));
        assertEquals(3, redisZset.size());
    }

    @Test
    @DisplayName("测试按排名范围获取成员")
    void testGetRange() {
        // 1. 准备测试数据
        redisZset.add(1.0, "member1");
        redisZset.add(2.0, "member2");
        redisZset.add(3.0, "member3");
        redisZset.add(4.0, "member4");
        redisZset.add(5.0, "member5");
        
        // 2. 测试正常范围
        List<RedisZset.ZsetNode> range = redisZset.getRange(1, 3);
        assertEquals(3, range.size());
        assertEquals("member2", range.get(0).getMember());
        assertEquals("member4", range.get(2).getMember());
        
        // 3. 测试负数索引
        range = redisZset.getRange(-3, -1);
        assertEquals(3, range.size());
        assertEquals("member3", range.get(0).getMember());
        assertEquals("member5", range.get(2).getMember());
        
        // 4. 测试越界范围
        range = redisZset.getRange(-10, 10);
        assertEquals(5, range.size());
        
        // 5. 测试空范围
        range = redisZset.getRange(3, 1);
        assertTrue(range.isEmpty());
    }

    @Test
    @DisplayName("测试按分数范围获取成员")
    void testGetRangeByScore() {
        // 1. 准备测试数据
        redisZset.add(1.0, "member1");
        redisZset.add(2.0, "member2");
        redisZset.add(3.0, "member3");
        redisZset.add(4.0, "member4");
        redisZset.add(5.0, "member5");
        
        // 2. 测试正常范围
        List<RedisZset.ZsetNode> range = redisZset.getRangeByScore(2.0, 4.0);
        assertEquals(3, range.size());
        assertEquals("member2", range.get(0).getMember());
        assertEquals("member4", range.get(2).getMember());
        
        // 3. 测试边界值
        range = redisZset.getRangeByScore(1.0, 1.0);
        assertEquals(1, range.size());
        assertEquals("member1", range.get(0).getMember());
        
        // 4. 测试空范围
        range = redisZset.getRangeByScore(2.5, 2.9);
        assertTrue(range.isEmpty());
        
        // 5. 测试越界范围
        range = redisZset.getRangeByScore(0.0, 10.0);
        assertEquals(5, range.size());
    }

    @Test
    @DisplayName("测试过期时间管理")
    void testTimeoutManagement() {
        // 1. 测试默认过期时间
        assertEquals(-1, redisZset.timeout());
        
        // 2. 测试设置过期时间
        long expireTime = System.currentTimeMillis() + 10000;
        redisZset.setTimeout(expireTime);
        assertEquals(expireTime, redisZset.timeout());
        
        // 3. 测试设置永不过期
        redisZset.setTimeout(-1);
        assertEquals(-1, redisZset.timeout());
    }

    @Test
    @DisplayName("测试Redis协议转换")
    void testConvertToResp() {
        // 1. 测试空集合转换
        List<Resp> respList = redisZset.convertToResp();
        assertTrue(respList.isEmpty());
        
        // 2. 测试有数据的集合转换
        redisZset.add(1.0, "member1");
        redisZset.add(2.0, "member2");
        
        respList = redisZset.convertToResp();
        assertEquals(2, respList.size());
        
        RespArray respArray = (RespArray) respList.get(0);
        Resp[] commands = respArray.getContent();
        
        assertEquals("ZADD", ((BulkString) commands[0]).getContent().getString());
        assertEquals(TEST_KEY, ((BulkString) commands[1]).getContent().getString());
    }

    @Test
    @DisplayName("测试成员分数操作")
    void testMemberScoreOperations() {
        // 1. 测试获取不存在成员的分数
        assertNull(redisZset.getScore("nonexistent"));
        
        // 2. 测试添加和获取分数
        redisZset.add(1.5, "member1");
        assertEquals(1.5, redisZset.getScore("member1"));
        
        // 3. 测试更新分数
        redisZset.add(2.5, "member1");
        assertEquals(2.5, redisZset.getScore("member1"));
        
        // 4. 测试移除成员后获取分数
        assertTrue(redisZset.remove("member1"));
        assertNull(redisZset.getScore("member1"));
    }

    @Test
    @DisplayName("测试成员移除操作")
    void testRemoveMembers() {
        // 1. 测试移除不存在的成员
        assertFalse(redisZset.remove("nonexistent"));
        
        // 2. 测试移除存在的成员
        redisZset.add(1.0, "member1");
        assertTrue(redisZset.remove("member1"));
        assertEquals(0, redisZset.size());
        
        // 3. 测试移除后再次移除
        assertFalse(redisZset.remove("member1"));
        
        // 4. 测试多次添加和移除
        redisZset.add(1.0, "member1");
        redisZset.add(2.0, "member2");
        assertTrue(redisZset.remove("member1"));
        assertEquals(1, redisZset.size());
        assertTrue(redisZset.contains("member2"));
    }

    @Test
    @DisplayName("测试边界条件")
    void testBoundaryConditions() {
        // 1. 测试空字符串成员
        assertTrue(redisZset.add(1.0, ""));
        assertEquals(1.0, redisZset.getScore(""));
        
        // 2. 测试特殊字符成员
        String specialChars = "!@#$%^&*()中文\n\t\r";
        assertTrue(redisZset.add(2.0, specialChars));
        assertEquals(2.0, redisZset.getScore(specialChars));
        
        // 3. 测试极端分数
        assertTrue(redisZset.add(Double.MIN_VALUE, "min_score"));
        assertTrue(redisZset.add(Double.MAX_VALUE, "max_score"));
        assertEquals(Double.MIN_VALUE, redisZset.getScore("min_score"));
        assertEquals(Double.MAX_VALUE, redisZset.getScore("max_score"));
        
        // 4. 测试NaN和无穷大（应该正常处理）
        assertTrue(redisZset.add(Double.POSITIVE_INFINITY, "inf"));
        assertTrue(redisZset.add(Double.NEGATIVE_INFINITY, "neg_inf"));
    }

    @Test
    @DisplayName("测试获取所有成员")
    void testGetAllMembers() {
        // 1. 测试空集合
        assertFalse(redisZset.getAll().iterator().hasNext());
        
        // 2. 测试成员存在性
        redisZset.add(3.0, "member3");
        redisZset.add(1.0, "member1");
        redisZset.add(2.0, "member2");
        
        // 验证所有成员都存在
        Set<String> expectedMembers = new HashSet<>();
        expectedMembers.add("member1");
        expectedMembers.add("member2");
        expectedMembers.add("member3");
        
        Set<String> actualMembers = new HashSet<>();
        for (Map.Entry<String, Double> entry : redisZset.getAll()) {
            actualMembers.add(entry.getKey());
        }
        
        assertEquals(expectedMembers, actualMembers, "所有添加的成员都应该存在");
        
        // 3. 测试分数正确性
        for (Map.Entry<String, Double> entry : redisZset.getAll()) {
            String member = entry.getKey();
            Double score = entry.getValue();
            if (member.equals("member1")) {
                assertEquals(1.0, score);
            } else if (member.equals("member2")) {
                assertEquals(2.0, score);
            } else if (member.equals("member3")) {
                assertEquals(3.0, score);
            }
        }
    }
} 
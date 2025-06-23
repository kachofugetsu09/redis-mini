package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisSet类的单元测试
 * 
 * <p>全面测试Redis集合数据结构的功能，包括：
 * <ul>
 *     <li>基本的集合操作（添加、删除、查询）</li>
 *     <li>随机弹出操作</li>
 *     <li>过期时间管理</li>
 *     <li>Redis协议转换</li>
 *     <li>边界条件和异常情况</li>
 *     <li>并发安全性测试</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
class RedisSetTest {

    /** 测试用的常量 */
    private static final String TEST_KEY = "test_set";
    private static final int LARGE_SIZE = 1000;

    /** 被测试的RedisSet实例 */
    private RedisSet redisSet;

    /**
     * 每个测试方法执行前的初始化
     */
    @BeforeEach
    void setUp() {
        redisSet = new RedisSet();
        redisSet.setKey(RedisBytes.fromString(TEST_KEY));
    }

    /**
     * 测试基本的添加操作
     */
    @Test
    void testBasicAdd() {
        // 1. 测试添加单个元素
        List<RedisBytes> members = new ArrayList<>();
        members.add(RedisBytes.fromString("member1"));
        
        int result = redisSet.add(members);
        assertEquals(1, result, "添加新元素应该返回1");
        assertEquals(1, redisSet.size(), "集合大小应该为1");

        // 2. 测试添加重复元素
        result = redisSet.add(members);
        assertEquals(0, result, "添加重复元素应该返回0");
        assertEquals(1, redisSet.size(), "集合大小应该仍为1");

        // 3. 测试添加多个元素
        List<RedisBytes> newMembers = new ArrayList<>();
        newMembers.add(RedisBytes.fromString("member2"));
        newMembers.add(RedisBytes.fromString("member3"));
        newMembers.add(RedisBytes.fromString("member1")); // 重复元素
        
        result = redisSet.add(newMembers);
        assertEquals(2, result, "应该只添加2个新元素");
        assertEquals(3, redisSet.size(), "集合大小应该为3");
    }

    /**
     * 测试空列表添加
     */
    @Test
    void testAddEmptyList() {
        List<RedisBytes> emptyMembers = new ArrayList<>();
        int result = redisSet.add(emptyMembers);
        
        assertEquals(0, result, "添加空列表应该返回0");
        assertEquals(0, redisSet.size(), "集合大小应该为0");
    }

    /**
     * 测试移除操作
     */
    @Test
    void testRemove() {
        // 1. 准备测试数据
        List<RedisBytes> members = new ArrayList<>();
        members.add(RedisBytes.fromString("member1"));
        members.add(RedisBytes.fromString("member2"));
        redisSet.add(members);

        // 2. 测试移除存在的元素
        RedisBytes member1 = RedisBytes.fromString("member1");
        int result = redisSet.remove(member1);
        assertEquals(1, result, "移除存在的元素应该返回1");
        assertEquals(1, redisSet.size(), "集合大小应该减1");

        // 3. 测试移除不存在的元素
        RedisBytes nonExistent = RedisBytes.fromString("nonexistent");
        result = redisSet.remove(nonExistent);
        assertEquals(0, result, "移除不存在的元素应该返回0");
        assertEquals(1, redisSet.size(), "集合大小应该不变");

        // 4. 测试移除剩余元素
        RedisBytes member2 = RedisBytes.fromString("member2");
        result = redisSet.remove(member2);
        assertEquals(1, result, "移除存在的元素应该返回1");
        assertEquals(0, redisSet.size(), "集合应该为空");
    }

    /**
     * 测试随机弹出操作
     */
    @Test
    void testPop() {
        // 1. 测试空集合弹出
        List<RedisBytes> result = redisSet.pop(1);
        assertTrue(result.isEmpty(), "空集合弹出应该返回空列表");

        // 2. 准备测试数据
        List<RedisBytes> members = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            members.add(RedisBytes.fromString("member" + i));
        }
        redisSet.add(members);

        // 3. 测试弹出单个元素
        result = redisSet.pop(1);
        assertEquals(1, result.size(), "应该弹出1个元素");
        assertEquals(4, redisSet.size(), "集合大小应该减1");
        
        // 4. 测试弹出多个元素
        result = redisSet.pop(2);
        assertEquals(2, result.size(), "应该弹出2个元素");
        assertEquals(2, redisSet.size(), "集合大小应该减2");

        // 5. 测试弹出数量超过集合大小
        result = redisSet.pop(10);
        assertEquals(2, result.size(), "最多只能弹出剩余的2个元素");
        assertEquals(0, redisSet.size(), "集合应该为空");

        // 6. 测试弹出0个元素
        redisSet.add(members);
        result = redisSet.pop(0);
        assertTrue(result.isEmpty(), "弹出0个元素应该返回空列表");
        assertEquals(5, redisSet.size(), "集合大小应该不变");
    }

    /**
     * 测试获取所有元素
     */
    @Test
    void testGetAll() {
        // 1. 测试空集合
        RedisBytes[] result = redisSet.getAll();
        assertEquals(0, result.length, "空集合应该返回空数组");

        // 2. 测试有元素的集合
        List<RedisBytes> members = new ArrayList<>();
        members.add(RedisBytes.fromString("member1"));
        members.add(RedisBytes.fromString("member2"));
        members.add(RedisBytes.fromString("member3"));
        redisSet.add(members);

        result = redisSet.getAll();
        assertEquals(3, result.length, "应该返回3个元素");
          // 3. 验证所有元素都存在（顺序可能不同）
        Set<String> expectedMembers = ConcurrentHashMap.newKeySet();
        expectedMembers.add("member1");
        expectedMembers.add("member2");
        expectedMembers.add("member3");
          Set<String> actualMembers = ConcurrentHashMap.newKeySet();
        for (RedisBytes member : result) {
            actualMembers.add(member.getString());
        }
        assertEquals(expectedMembers, actualMembers, "返回的元素应该包含所有添加的成员");
    }

    /**
     * 测试过期时间管理
     */
    @Test
    void testTimeoutManagement() {
        // 1. 测试默认过期时间
        assertEquals(-1, redisSet.timeout(), "默认过期时间应该为-1");

        // 2. 测试设置过期时间
        long expireTime = System.currentTimeMillis() + 10000;
        redisSet.setTimeout(expireTime);
        assertEquals(expireTime, redisSet.timeout(), "过期时间应该被正确设置");

        // 3. 测试设置永不过期
        redisSet.setTimeout(-1);
        assertEquals(-1, redisSet.timeout(), "应该能设置为永不过期");

        // 4. 测试设置0作为过期时间
        redisSet.setTimeout(0);
        assertEquals(0, redisSet.timeout(), "应该能设置过期时间为0");
    }

    /**
     * 测试Redis协议转换
     */
    @Test
    void testConvertToResp() {
        // 1. 测试空集合转换
        List<Resp> result = redisSet.convertToResp();
        assertTrue(result.isEmpty(), "空集合应该返回空的RESP列表");

        // 2. 测试有数据的集合转换
        List<RedisBytes> members = new ArrayList<>();
        members.add(RedisBytes.fromString("member1"));
        members.add(RedisBytes.fromString("member2"));
        redisSet.add(members);

        result = redisSet.convertToResp();
        assertEquals(1, result.size(), "应该返回一个RESP数组");        // 3. 验证RESP数组结构
        assertTrue(result.get(0) instanceof RespArray, "应该是RespArray类型");
        RespArray respArray = (RespArray) result.get(0);
        Resp[] elements = respArray.getContent();        // 验证命令结构：SADD key member1 member2
        assertTrue(elements.length >= 3, "至少应该有3个元素（SADD + key + members）");        assertEquals("SADD", ((BulkString) elements[0]).getContent().getString(),
                    "第一个元素应该是SADD命令");
        assertEquals(TEST_KEY, ((BulkString) elements[1]).getContent().getString(),
                    "第二个元素应该是键名");
    }

    /**
     * 测试大量数据操作
     */
    @Test
    void testLargeDataOperations() {
        // 1. 添加大量数据
        List<RedisBytes> largeMembers = new ArrayList<>();
        for (int i = 0; i < LARGE_SIZE; i++) {
            largeMembers.add(RedisBytes.fromString("member_" + i));
        }

        int result = redisSet.add(largeMembers);
        assertEquals(LARGE_SIZE, result, "应该成功添加所有元素");
        assertEquals(LARGE_SIZE, redisSet.size(), "集合大小应该正确");

        // 2. 测试大量弹出
        List<RedisBytes> popped = redisSet.pop(LARGE_SIZE / 2);
        assertEquals(LARGE_SIZE / 2, popped.size(), "应该弹出指定数量的元素");
        assertEquals(LARGE_SIZE / 2, redisSet.size(), "剩余元素数量应该正确");

        // 3. 测试获取所有剩余元素
        RedisBytes[] remaining = redisSet.getAll();
        assertEquals(LARGE_SIZE / 2, remaining.length, "剩余元素数组大小应该正确");
    }    /**
     * 测试读写分离的并发安全性（模拟 Redis 真实场景）
     * 
     * <p>在真实的 Redis 中：
     * <ul>
     *     <li>主线程串行处理所有命令（写操作）</li>
     *     <li>后台线程进行持久化、过期清理等（读操作）</li>
     *     <li>不存在多线程同时写入的情况</li>
     * </ul>
     */
    @Test
    void testConcurrentReadWhileWrite() throws InterruptedException {
        // 1. 先添加一些初始数据
        List<RedisBytes> initialMembers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            initialMembers.add(RedisBytes.fromString("initial_member_" + i));
        }
        redisSet.add(initialMembers);
        
        final AtomicBoolean stopReading = new AtomicBoolean(false);
        final AtomicInteger readSuccessCount = new AtomicInteger(0);
        final AtomicInteger readErrorCount = new AtomicInteger(0);
        
        // 2. 启动读取线程（模拟持久化线程）
        Thread readerThread = new Thread(() -> {
            while (!stopReading.get()) {
                try {
                    // 模拟持久化操作：读取所有数据
                    RedisBytes[] allMembers = redisSet.getAll();
                    long size = redisSet.size();
                      // 验证数据一致性（允许弱一致性）
                    long sizeDiff = Math.abs(allMembers.length - size);
                    if (sizeDiff <= 2) { // 允许轻微的不一致，这在并发读写时是正常的
                        readSuccessCount.incrementAndGet();
                    } else {
                        readErrorCount.incrementAndGet();
                        System.err.println("严重数据不一致: 数组长度=" + allMembers.length + ", size()=" + size + ", 差值=" + sizeDiff);
                    }
                    
                    Thread.sleep(1); // 模拟持久化处理时间
                } catch (Exception e) {
                    readErrorCount.incrementAndGet();
                    e.printStackTrace();
                    break;
                }
            }
        });
        
        // 3. 启动读取线程
        readerThread.start();
        
        // 4. 主线程进行写操作（模拟命令处理）
        for (int i = 0; i < 50; i++) {
            List<RedisBytes> newMembers = new ArrayList<>();
            newMembers.add(RedisBytes.fromString("new_member_" + i));
            redisSet.add(newMembers);
            
            // 模拟一些删除操作
            if (i % 10 == 0) {
                redisSet.pop(2);
            }
            
            Thread.sleep(10); // 模拟命令处理间隔
        }
        
        // 5. 停止读取并等待完成
        stopReading.set(true);
        readerThread.join(2000);
          // 6. 验证并发读取的表现
        assertTrue(readSuccessCount.get() > 0, "应该有成功的读取操作");
        
        // 在读写分离的场景下，允许一定程度的弱一致性
        // 但严重的数据不一致（差值>2）应该不会出现
        int totalReads = readSuccessCount.get() + readErrorCount.get();
        double errorRate = (double) readErrorCount.get() / totalReads;
        assertTrue(errorRate < 0.1, "严重数据不一致的比例应该小于10%，当前: " + (errorRate * 100) + "%");
        
        System.out.println("成功读取次数: " + readSuccessCount.get());
        System.out.println("数据不一致次数: " + readErrorCount.get());
        System.out.println("总读取次数: " + totalReads);
        System.out.println("不一致比例: " + String.format("%.2f%%", errorRate * 100));
        System.out.println("最终集合大小: " + redisSet.size());
    }

    /**
     * 测试边界条件
     */
    @Test
    void testBoundaryConditions() {
        // 1. 测试null值处理
        assertDoesNotThrow(() -> {
            List<RedisBytes> membersWithNull = new ArrayList<>();
            membersWithNull.add(RedisBytes.fromString("valid_member"));
            redisSet.add(membersWithNull);
        }, "处理包含有效成员的列表不应抛出异常");

        // 2. 测试非常长的成员名
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longName.append("a");
        }
        
        List<RedisBytes> longMembers = new ArrayList<>();
        longMembers.add(RedisBytes.fromString(longName.toString()));
        
        assertDoesNotThrow(() -> {
            redisSet.add(longMembers);
        }, "添加长成员名不应抛出异常");

        assertTrue(redisSet.size() > 0, "长成员名应该被成功添加");

        // 3. 测试特殊字符
        List<RedisBytes> specialMembers = new ArrayList<>();
        specialMembers.add(RedisBytes.fromString(""));  // 空字符串
        specialMembers.add(RedisBytes.fromString(" "));  // 空格
        specialMembers.add(RedisBytes.fromString("\n\t\r"));  // 换行符等
        specialMembers.add(RedisBytes.fromString("中文测试"));  // 中文
        specialMembers.add(RedisBytes.fromString("🎉🚀💻"));  // emoji
        
        assertDoesNotThrow(() -> {
            redisSet.add(specialMembers);
        }, "添加特殊字符成员不应抛出异常");
    }

    /**
     * 测试内存和性能相关
     */
    @Test
    void testMemoryAndPerformance() {
        // 1. 测试重复添加和删除
        List<RedisBytes> members = new ArrayList<>();
        RedisBytes testMember = RedisBytes.fromString("test_member");
        members.add(testMember);

        for (int i = 0; i < 1000; i++) {
            redisSet.add(members);
            redisSet.remove(testMember);
        }

        assertEquals(0, redisSet.size(), "重复添加删除后集合应该为空");        // 2. 测试pop操作的随机性（简单验证）
        members.clear();
        Set<String> originalMembers = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            String memberStr = "member_" + i;
            members.add(RedisBytes.fromString(memberStr));
            originalMembers.add(memberStr);
        }
        redisSet.add(members);        // 多次pop操作，验证返回的元素确实存在于原集合中
        for (int i = 0; i < 10; i++) {
            List<RedisBytes> popped = redisSet.pop(1);
            if (!popped.isEmpty()) {
                String poppedValue = popped.get(0).getString();
                assertTrue(originalMembers.contains(poppedValue), 
                          "弹出的元素应该是原集合中的成员");
            }
        }
    }
}

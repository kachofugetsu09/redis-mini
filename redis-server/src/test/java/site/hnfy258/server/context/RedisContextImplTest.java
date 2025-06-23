package site.hnfy258.server.context;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;
import site.hnfy258.server.config.RedisServerConfig;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisContextImplTest {

    private RedisContextImpl redisContext;
    private RedisCore redisCore;
    private RedisServerConfig config;

    @BeforeEach
    void setUp() {
        // 创建基本配置
        config = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .aofEnabled(false)  // 测试时禁用AOF
                .rdbEnabled(false)  // 测试时禁用RDB
                .build();

        // 创建真实的RedisCore实例
        redisCore = new RedisCoreImpl(16);  // 16个数据库
        
        // 创建RedisContext实例
        redisContext = new RedisContextImpl(redisCore, "localhost", 6379, config);
    }

    @Test
    void testBasicDataOperations() {
        // 准备测试数据
        RedisBytes key = RedisBytes.fromString("test-key");
        RedisString value = new RedisString(Sds.create("test-value".getBytes()));

        // 测试put和get
        redisContext.put(key, value);
        assertEquals(value, redisContext.get(key));

        // 测试keys
        Set<RedisBytes> keys = redisContext.keys();
        assertTrue(keys.contains(key));
        assertEquals(1, keys.size());

        // 测试数据库选择
        assertEquals(0, redisContext.getCurrentDBIndex());
        redisContext.selectDB(1);
        assertEquals(1, redisContext.getCurrentDBIndex());
        assertNull(redisContext.get(key));  // 新数据库中不应该有数据

        // 测试flushAll
        redisContext.flushAll();
        assertTrue(redisContext.keys().isEmpty());
    }

    @Test
    void testDatabaseOperations() {
        // 测试数据库数量
        assertEquals(16, redisContext.getDBNum());

        // 测试数据库切换边界条件
        assertThrows(IllegalArgumentException.class, () -> redisContext.selectDB(-1));
        assertThrows(IllegalArgumentException.class, () -> redisContext.selectDB(16));

        // 测试数据库隔离性
        RedisBytes key = RedisBytes.fromString("isolation-test");
        RedisString value = new RedisString(Sds.create("value".getBytes()));

        redisContext.selectDB(0);
        redisContext.put(key, value);
        
        redisContext.selectDB(1);
        assertNull(redisContext.get(key));
        
        redisContext.selectDB(0);
        assertEquals(value, redisContext.get(key));
    }

    @Test
    void testPersistenceStatus() {
        // 测试持久化状态
        assertFalse(redisContext.isAofEnabled());
        assertFalse(redisContext.isRdbEnabled());
        
        // 测试持久化管理器可用性
        assertFalse(redisContext.isAofManagerAvailable());
        assertFalse(redisContext.isRdbManagerAvailable());
    }

    @Test
    void testSystemLifecycle() {
        // 测试系统启动
        assertFalse(redisContext.isRunning());
        redisContext.startup();
        assertTrue(redisContext.isRunning());

        // 测试系统关闭
        redisContext.shutdown();
        assertFalse(redisContext.isRunning());
    }

    @Test
    void testServerInfo() {
        // 测试服务器信息
        assertEquals("localhost", redisContext.getServerHost());
        assertEquals(6379, redisContext.getServerPort());
    }
} 
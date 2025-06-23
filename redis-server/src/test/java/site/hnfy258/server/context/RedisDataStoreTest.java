package site.hnfy258.server.context;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisDataStoreTest {

    private RedisDataStore redisDataStore;
    private RedisCore redisCore;

    @BeforeEach
    void setUp() {
        // 创建真实的RedisCore实例
        redisCore = new RedisCoreImpl(16);  // 16个数据库
        
        // 创建RedisDataStore实例
        redisDataStore = new RedisDataStore(redisCore);
    }

    @Test
    void testBasicDataOperations() {
        // 准备测试数据
        RedisBytes key = RedisBytes.fromString("test-key");
        RedisString value = new RedisString(Sds.create("test-value".getBytes()));

        // 测试put和get
        redisDataStore.put(key, value);
        assertEquals(value, redisDataStore.get(key));

        // 测试keys
        Set<RedisBytes> keys = redisDataStore.keys();
        assertTrue(keys.contains(key));
        assertEquals(1, keys.size());
    }

    @Test
    void testDatabaseOperations() {
        // 测试数据库切换
        assertEquals(0, redisDataStore.getCurrentDBIndex());
        redisDataStore.selectDB(1);
        assertEquals(1, redisDataStore.getCurrentDBIndex());

        // 测试数据库数量
        assertEquals(16, redisDataStore.getDBNum());

        // 测试数据库切换边界条件
        assertThrows(IllegalArgumentException.class, () -> redisDataStore.selectDB(-1));
        assertThrows(IllegalArgumentException.class, () -> redisDataStore.selectDB(16));
    }

    @Test
    void testDatabaseIsolation() {
        // 准备测试数据
        RedisBytes key = RedisBytes.fromString("isolation-test");
        RedisString value = new RedisString(Sds.create("value".getBytes()));

        // 在数据库0中存储数据
        redisDataStore.selectDB(0);
        redisDataStore.put(key, value);
        
        // 切换到数据库1，验证数据隔离
        redisDataStore.selectDB(1);
        assertNull(redisDataStore.get(key));
        
        // 切回数据库0，验证数据仍然存在
        redisDataStore.selectDB(0);
        assertEquals(value, redisDataStore.get(key));
    }

    @Test
    void testFlushAll() {
        // 准备测试数据
        RedisBytes key1 = RedisBytes.fromString("key1");
        RedisString value1 = new RedisString(Sds.create("value1".getBytes()));
        RedisBytes key2 = RedisBytes.fromString("key2");
        RedisString value2 = new RedisString(Sds.create("value2".getBytes()));

        // 在不同数据库中存储数据
        redisDataStore.selectDB(0);
        redisDataStore.put(key1, value1);
        redisDataStore.selectDB(1);
        redisDataStore.put(key2, value2);

        // 执行flushAll
        redisDataStore.flushAll();

        // 验证所有数据库都被清空
        redisDataStore.selectDB(0);
        assertTrue(redisDataStore.keys().isEmpty());
        redisDataStore.selectDB(1);
        assertTrue(redisDataStore.keys().isEmpty());
    }
} 
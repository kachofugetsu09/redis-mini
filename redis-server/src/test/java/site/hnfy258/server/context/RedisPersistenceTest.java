package site.hnfy258.server.context;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.aof.AofManager;
import site.hnfy258.rdb.RdbManager;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisPersistenceTest {

    @Mock
    private AofManager aofManager;

    @Mock
    private RdbManager rdbManager;

    private RedisPersistence persistence;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testAofEnabledOperations() throws IOException {
        // 创建启用AOF的RedisPersistence实例
        persistence = new RedisPersistence(aofManager, null);
        assertTrue(persistence.isAofEnabled());
        assertFalse(persistence.isRdbEnabled());

        // 测试AOF写入
        byte[] testCommand = "SET key value".getBytes();
        persistence.writeAof(testCommand);
        verify(aofManager).appendBytes(testCommand);
    }

    @Test
    void testRdbEnabledOperations() throws IOException {
        // 创建启用RDB的RedisPersistence实例
        persistence = new RedisPersistence(null, rdbManager);
        assertFalse(persistence.isAofEnabled());
        assertTrue(persistence.isRdbEnabled());

        // 测试RDB保存
        when(rdbManager.saveRdb()).thenReturn(true);
        assertTrue(persistence.saveRdb());
        verify(rdbManager).saveRdb();

        // 测试RDB加载
        when(rdbManager.loadRdb()).thenReturn(true);
        assertTrue(persistence.loadRdb());
        verify(rdbManager).loadRdb();
    }

    @Test
    void testBothPersistenceEnabled() throws IOException {
        // 创建同时启用AOF和RDB的RedisPersistence实例
        persistence = new RedisPersistence(aofManager, rdbManager);
        assertTrue(persistence.isAofEnabled());
        assertTrue(persistence.isRdbEnabled());

        // 测试AOF和RDB操作
        byte[] testCommand = "SET key value".getBytes();
        persistence.writeAof(testCommand);
        verify(aofManager).appendBytes(testCommand);

        when(rdbManager.saveRdb()).thenReturn(true);
        assertTrue(persistence.saveRdb());
        verify(rdbManager).saveRdb();
    }

    @Test
    void testNoPersistenceEnabled() {
        // 创建未启用任何持久化的RedisPersistence实例
        persistence = new RedisPersistence(null, null);
        assertFalse(persistence.isAofEnabled());
        assertFalse(persistence.isRdbEnabled());

        // 测试AOF写入（应该被忽略）
        byte[] testCommand = "SET key value".getBytes();
        persistence.writeAof(testCommand);
        verifyNoInteractions(aofManager);

        // 测试RDB操作（应该返回false）
        assertFalse(persistence.saveRdb());
        assertFalse(persistence.loadRdb());
        verifyNoInteractions(rdbManager);
    }

    @Test
    void testBackgroundSave() {
        // 创建启用RDB的RedisPersistence实例
        persistence = new RedisPersistence(null, rdbManager);

        // 模拟后台保存操作
        when(rdbManager.bgSaveRdb()).thenReturn(CompletableFuture.completedFuture(true));
        
        CompletableFuture<Boolean> future = persistence.bgSaveRdb();
        assertTrue(future.join());
        verify(rdbManager).bgSaveRdb();
    }

    @Test
    void testReplicationOperations() throws Exception {
        // 创建启用RDB的RedisPersistence实例
        persistence = new RedisPersistence(null, rdbManager);

        // 测试创建临时RDB
        byte[] mockRdbContent = "mock-rdb-content".getBytes();
        when(rdbManager.createTempRdbForReplication()).thenReturn(mockRdbContent);
        
        byte[] result = persistence.createTempRdbForReplication();
        assertArrayEquals(mockRdbContent, result);
        verify(rdbManager).createTempRdbForReplication();

        // 测试从字节数组加载RDB
        when(rdbManager.loadRdbFromBytes(any())).thenReturn(true);
        
        assertTrue(persistence.loadRdbFromBytes(mockRdbContent));
        verify(rdbManager).loadRdbFromBytes(mockRdbContent);
    }

    @Test
    void testShutdown() throws Exception {
        // 创建同时启用AOF和RDB的RedisPersistence实例
        persistence = new RedisPersistence(aofManager, rdbManager);

        // 测试关闭操作
        persistence.shutdown();
        verify(aofManager).close();
        verify(rdbManager).close();
    }
} 
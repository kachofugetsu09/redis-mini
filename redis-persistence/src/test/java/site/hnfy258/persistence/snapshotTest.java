package site.hnfy258.persistence;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import site.hnfy258.rdb.RdbLoader;
import site.hnfy258.rdb.RdbWriter;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 快照一致性测试
 * 写入100000000个数据，进行bgsave异步快照，边重写边往里写入1000个数据
 */
@Slf4j
public class snapshotTest {
    
    private RedisCore redisCore;
    private RdbWriter rdbWriter;
    private File testDir;
    
    @BeforeEach
    public void setUp() throws Exception {
        // 创建测试目录
        testDir = new File("target/test-snapshots");
        testDir.mkdirs();
        
        // 初始化Redis核心
        redisCore = new RedisCoreImpl(16);
        rdbWriter = new RdbWriter(redisCore);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (rdbWriter != null) {
            rdbWriter.close();
        }
        
        // 清理测试文件
        deleteDirectory(testDir);
    }
    
    /**
     * 测试快照数据一致性
     * 写入100000000个数据，进行bgsave，边快照边写入1000个新数据
     */
    @Test
    public void testSnapshotDataConsistency() throws Exception {
        log.info("=== 开始快照数据一致性测试 ===");
        
        // 第1阶段：写入100000000个初始数据
        log.info("第1阶段：写入100000000个初始数据...");
        redisCore.flushAll();
        
        int initialDataCount = 50000;
        for (int i = 0; i < initialDataCount; i++) {
            RedisBytes key = RedisBytes.fromString("data-key-" + i);
            RedisString value = new RedisString(Sds.create(("initial-value-" + i).getBytes()));
            redisCore.put(key, value);
            
            // 每1000万个数据输出一次进度
            if ((i + 1) % 10000 == 0) {
                log.info("已写入 {} 个数据", i + 1);
            }
        }
        
        // 验证初始数据数量
        int keysBeforeSnapshot = redisCore.keys().size();
        log.info("初始数据写入完成，共 {} 个键", keysBeforeSnapshot);
        assertEquals(initialDataCount, keysBeforeSnapshot, "初始数据数量应该正确");
        
        // 第2阶段：触发BGSAVE异步快照
        log.info("第2阶段：触发BGSAVE异步快照...");
        File rdbFile = new File(testDir, "data-consistency-test.rdb");
        CompletableFuture<Boolean> bgsaveFuture = rdbWriter.bgSaveRdb(rdbFile.getAbsolutePath());
        
        // 验证bgSaveRdb立即返回（不阻塞主线程）
        assertFalse(bgsaveFuture.isDone(), "BGSAVE应该在后台执行，主线程立即返回");
        log.info("BGSAVE已启动，在后台线程中执行...");
        
        // 第3阶段：在BGSAVE执行期间，主线程写入1000个新数据
        log.info("第3阶段：主线程写入1000个新数据（应该不影响快照）...");
        int newDataCount = 1000;
        for (int i = 0; i < newDataCount; i++) {
            RedisBytes key = RedisBytes.fromString("new-data-key-" + i);
            RedisString value = new RedisString(Sds.create(("new-value-" + i).getBytes()));
            redisCore.put(key, value);
            
            // 每100个数据输出一次进度
            if ((i + 1) % 100 == 0) {
                log.info("已写入 {} 个新数据", i + 1);
            }
        }
        
        // 验证主线程当前的数据数量
        int keysAfterNewData = redisCore.keys().size();
        int expectedMainThreadKeys = initialDataCount + newDataCount;
        log.info("主线程当前数据数量：{}", keysAfterNewData);
        log.info("预期主线程数据数量：{} (初始{} + 新增{})", expectedMainThreadKeys, initialDataCount, newDataCount);
        
        assertEquals(expectedMainThreadKeys, keysAfterNewData, 
            "主线程应该包含所有初始数据和新增数据");
        
        // 第4阶段：等待BGSAVE完成
        log.info("第4阶段：等待BGSAVE完成...");
        Boolean bgsaveResult = bgsaveFuture.get(60, TimeUnit.SECONDS);
        assertTrue(bgsaveResult, "BGSAVE应该成功完成");
        assertTrue(rdbFile.exists(), "RDB文件应该被创建");
        log.info("BGSAVE完成，RDB文件大小：{} 字节", rdbFile.length());
        
        // 第5阶段：验证快照内容
        log.info("第5阶段：验证快照内容...");
        
        // 创建新的RedisCore实例来加载快照
        RedisCore snapshotRedisCore = new RedisCoreImpl(16);
        RdbLoader rdbLoader = new RdbLoader(snapshotRedisCore);
        
        // 加载RDB文件
        boolean loadResult = rdbLoader.loadRdb(rdbFile);
        assertTrue(loadResult, "RDB文件应该成功加载");
        
        // 验证快照数据数量
        int snapshotKeyCount = snapshotRedisCore.keys().size();
        log.info("快照验证结果：");
        log.info("  - 初始数据数量：{}", initialDataCount);
        log.info("  - 快照中数据数量：{}", snapshotKeyCount);
        log.info("  - 主线程当前数据数量：{}", keysAfterNewData);
        
        // 核心验证：快照应该只包含初始数据，不包含新增数据
        assertEquals(initialDataCount, snapshotKeyCount, 
            "快照应该只包含触发时刻的初始数据，不受后续新增数据影响");
        
        // 验证主线程包含更多数据
        assertTrue(keysAfterNewData > snapshotKeyCount, 
            "主线程应该包含比快照更多的数据");
        
        // 验证差异数量
        int dataDifference = keysAfterNewData - snapshotKeyCount;
        assertEquals(newDataCount, dataDifference, 
            "主线程与快照的数据差异应该等于新增数据数量");
        
        // 验证快照中包含初始数据（抽样验证前100个）
        log.info("抽样验证快照中的初始数据...");
        for (int i = 0; i < 100; i++) {
            RedisBytes key = RedisBytes.fromString("data-key-" + i);
            RedisString value = (RedisString) snapshotRedisCore.get(key);
            assertNotNull(value, "快照中应该包含初始数据键: " + key.getString());
            
            String expectedValue = "initial-value-" + i;
            assertEquals(expectedValue, value.getValue().getString(), 
                "快照中的值应该是初始值");
        }
        
        // 验证快照中不包含新增数据
        log.info("验证快照中不包含新增数据...");
        for (int i = 0; i < 100; i++) {
            RedisBytes key = RedisBytes.fromString("new-data-key-" + i);
            RedisString value = (RedisString) snapshotRedisCore.get(key);
            assertNull(value, "快照中不应该包含新增数据键: " + key.getString());
        }
        
        // 验证主线程包含新增数据
        log.info("验证主线程包含新增数据...");
        for (int i = 0; i < 100; i++) {
            RedisBytes key = RedisBytes.fromString("new-data-key-" + i);
            RedisString value = (RedisString) redisCore.get(key);
            assertNotNull(value, "主线程应该包含新增数据键: " + key.getString());
            
            String expectedValue = "new-value-" + i;
            assertEquals(expectedValue, value.getValue().getString(), 
                "主线程中的新增数据值应该正确");
        }
        
        log.info("=== 快照数据一致性测试通过 ===");
        log.info("验证结果：");
        log.info("  ✓ 快照包含 {} 个初始数据", snapshotKeyCount);
        log.info("  ✓ 主线程包含 {} 个数据（初始 + 新增）", keysAfterNewData);
        log.info("  ✓ 快照不包含BGSAVE期间新增的数据");
        log.info("  ✓ 主线程包含所有数据（初始 + 新增）");
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}

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
import java.util.Optional;
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
                log.info("已写入 {} 个数据", Optional.of(i + 1));
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



        // 检查快照启动后的主线程数据状态
        int keysAfterSnapshotStart = redisCore.keys().size();
        log.info("快照启动后主线程数据数量：{}", keysAfterSnapshotStart);
        assertEquals(initialDataCount, keysAfterSnapshotStart, "快照启动后主线程数据数量应该保持不变");

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

        // 检查快照完成后的主线程数据状态
        int keysAfterSnapshotComplete = redisCore.keys().size();
        log.info("快照完成后主线程数据数量：{}", keysAfterSnapshotComplete);
        assertEquals(expectedMainThreadKeys, keysAfterSnapshotComplete,
            "快照完成后主线程数据数量应该包含所有数据");

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
     * 测试快照过程中特定key的可见性
     * 在快照过程中写入 "huashen" -> "123"，验证主线程可见但快照中不可见
     */
    @Test
    public void testSnapshotSpecificKeyVisibility() throws Exception {
        log.info("=== 开始特定key可见性测试 ===");

        // 第1阶段：写入初始数据
        log.info("第1阶段：写入初始数据...");
        redisCore.flushAll();

        int initialDataCount = 1000;
        for (int i = 0; i < initialDataCount; i++) {
            RedisBytes key = RedisBytes.fromString("data-key-" + i);
            RedisString value = new RedisString(Sds.create(("initial-value-" + i).getBytes()));
            redisCore.put(key, value);
        }

        // 验证初始数据数量
        int keysBeforeSnapshot = redisCore.keys().size();
        log.info("初始数据写入完成，共 {} 个键", keysBeforeSnapshot);
        assertEquals(initialDataCount, keysBeforeSnapshot, "初始数据数量应该正确");

        // 第2阶段：触发BGSAVE异步快照
        log.info("第2阶段：触发BGSAVE异步快照...");
        File rdbFile = new File(testDir, "specific-key-test.rdb");
        CompletableFuture<Boolean> bgsaveFuture = rdbWriter.bgSaveRdb(rdbFile.getAbsolutePath());

        // 验证bgSaveRdb立即返回（不阻塞主线程）
        assertFalse(bgsaveFuture.isDone(), "BGSAVE应该在后台执行，主线程立即返回");
        log.info("BGSAVE已启动，在后台线程中执行...");

        // 第3阶段：在快照过程中写入特定的测试数据
        log.info("第3阶段：在快照过程中写入特定测试数据...");
        RedisBytes testKey = RedisBytes.fromString("huashen");
        RedisString testValue = new RedisString(Sds.create("123".getBytes()));
        redisCore.put(testKey, testValue);
        log.info("已写入测试数据: huashen -> 123");

        // 立即查询主线程中的数据，应该能看到新写入的数据
        RedisString mainThreadValue = (RedisString) redisCore.get(testKey);
        log.info("主线程查询结果: huashen -> {}", mainThreadValue != null ? mainThreadValue.getValue().getString() : "null");
        assertNotNull(mainThreadValue, "主线程应该能看到新写入的数据");
        assertEquals("123", mainThreadValue.getValue().getString(), "主线程应该能看到正确的值");

        // 再写入一些其他数据验证整体功能
        for (int i = 0; i < 50; i++) {
            RedisBytes key = RedisBytes.fromString("snapshot-test-" + i);
            RedisString value = new RedisString(Sds.create(("snapshot-value-" + i).getBytes()));
            redisCore.put(key, value);
        }

        // 验证主线程当前的数据数量
        int keysAfterNewData = redisCore.keys().size();
        int expectedMainThreadKeys = initialDataCount + 1 + 50; // 1000 + 1(huashen) + 50(snapshot-test)
        log.info("主线程当前数据数量：{}", keysAfterNewData);
        log.info("预期主线程数据数量：{} (初始{} + huashen + 其他{})", expectedMainThreadKeys, initialDataCount, 50);

        assertEquals(expectedMainThreadKeys, keysAfterNewData,
            "主线程应该包含所有初始数据和新增数据");

        // 第4阶段：等待BGSAVE完成
        log.info("第4阶段：等待BGSAVE完成...");
        Boolean bgsaveResult = bgsaveFuture.get(60, TimeUnit.SECONDS);
        assertTrue(bgsaveResult, "BGSAVE应该成功完成");
        assertTrue(rdbFile.exists(), "RDB文件应该被创建");

        // 第5阶段：验证快照文件中的数据
        log.info("第5阶段：验证快照文件中的数据...");

        // 创建新的RedisCore实例来读取快照文件
        RedisCore snapshotRedisCore = new RedisCoreImpl(16);
        RdbLoader rdbLoader = new RdbLoader(snapshotRedisCore);

        // 加载RDB文件
        boolean loadResult = rdbLoader.loadRdb(rdbFile);
        assertTrue(loadResult, "RDB文件应该成功加载");

        // 检查快照中的数据数量，应该只包含初始数据
        int snapshotDataCount = snapshotRedisCore.keys().size();
        log.info("快照中的数据数量：{}", snapshotDataCount);
        assertEquals(initialDataCount, snapshotDataCount, "快照应该只包含初始数据");

        // 检查快照中是否包含测试数据 huashen
        RedisString snapshotTestValue = (RedisString) snapshotRedisCore.get(testKey);
        log.info("快照中查询结果: huashen -> {}", snapshotTestValue != null ? snapshotTestValue.getValue().getString() : "null");
        assertNull(snapshotTestValue, "快照中不应该包含快照期间写入的数据");

        // 验证快照中包含初始数据
        RedisBytes initialKey = RedisBytes.fromString("data-key-0");
        RedisString initialValue = (RedisString) snapshotRedisCore.get(initialKey);
        assertNotNull(initialValue, "快照中应该包含初始数据");
        assertEquals("initial-value-0", initialValue.getValue().getString(), "快照中的初始数据应该正确");

        // 验证快照中不包含其他测试数据
        RedisBytes otherTestKey = RedisBytes.fromString("snapshot-test-0");
        RedisString otherTestValue = (RedisString) snapshotRedisCore.get(otherTestKey);
        assertNull(otherTestValue, "快照中不应该包含其他快照期间写入的数据");

        // 再次验证主线程中的数据依然正确
        log.info("第6阶段：再次验证主线程数据...");
        RedisString finalMainThreadValue = (RedisString) redisCore.get(testKey);
        assertNotNull(finalMainThreadValue, "主线程应该始终能看到写入的数据");
        assertEquals("123", finalMainThreadValue.getValue().getString(), "主线程中的数据应该始终正确");

        log.info("=== 特定key可见性测试完成 ===");
        log.info("测试结果：");
        log.info("1. ✓ 主线程能正确看到快照期间写入的数据 'huashen' -> '123'");
        log.info("2. ✓ 快照文件只包含快照启动时的 {} 个数据", snapshotDataCount);
        log.info("3. ✓ 快照文件不包含快照期间写入的数据 'huashen'");
        log.info("4. ✓ 主线程在快照完成后依然能正确访问新写入的数据");
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

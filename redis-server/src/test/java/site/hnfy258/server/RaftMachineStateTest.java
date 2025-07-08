package site.hnfy258.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import site.hnfy258.core.AppendResult;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.core.command.CommandExecutor;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.network.NettyRaftNetwork;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.raft.Raft;
import site.hnfy258.raft.RaftNode;
import site.hnfy258.server.command.executor.CommandExecutorImpl;
import site.hnfy258.server.config.RedisServerConfig;
import site.hnfy258.server.context.RedisContextImpl;

import static org.junit.jupiter.api.Assertions.*;

public class RaftMachineStateTest {
    
    private RedisCore redisCore;
    private Raft raft;
    private RaftNode raftNode;
    private NettyRaftNetwork network;
    
    @BeforeEach
    void setUp() {
        redisCore = new RedisCoreImpl(16);
        
        // 创建一个简单的CommandExecutor实现用于测试
        CommandExecutor commandExecutor =
                new CommandExecutorImpl(new RedisContextImpl(redisCore,"localhost",
                        9999,RedisServerConfig.defaultConfig()));
        ((RedisCoreImpl) redisCore).setCommandExecutor(commandExecutor);
        
        network = new NettyRaftNetwork("localhost", 8099);
        raft = new Raft(1, new int[]{1}, network, redisCore);
        raftNode = new RaftNode(1, new int[]{1}, network);
        
        // 手动设置为Leader，简化测试
        raft.becomeLeader();
    }
    
    @AfterEach
    void tearDown() {
        if (raftNode != null) {
            raftNode.stop();
        }
        if (network != null) {
            network.stop();
        }
    }
    
    @Test
    void testSetCommandAppliedToStateMachine() throws InterruptedException {
        // 创建一个SET命令: SET key1 value1
        RespArray setCommand = createSetCommand("key1", "value1");
        
        // 通过Raft提交命令
        AppendResult result = raft.start(setCommand);
        
        // 验证命令被接受
        assertTrue(result.isSuccess(), "SET命令应该被成功接受");
        assertTrue(result.getNewLogIndex() > 0, "应该生成新的日志索引");
        
        // 由于是单节点测试，直接模拟提交过程
        // 在实际环境中，这会通过心跳和复制机制自动完成
        simulateCommit();
        
        // 等待applier线程处理
        Thread.sleep(200);
        
        // 验证数据是否已经应用到状态机
        String retrievedValue = getValueFromRedisCore("key1");
        assertNotNull(retrievedValue, "应该能从RedisCore中获取到值");
        assertEquals("value1", retrievedValue, "获取的值应该与设置的值一致");
        
        System.out.println("✓ SET命令成功应用到状态机，key1 = " + retrievedValue);
    }
    
    @Test
    void testMultipleSetCommands() throws InterruptedException {
        // 测试多个SET命令
        String[] keys = {"key1", "key2", "key3"};
        String[] values = {"value1", "value2", "value3"};
        
        for (int i = 0; i < keys.length; i++) {
            RespArray setCommand = createSetCommand(keys[i], values[i]);
            AppendResult result = raft.start(setCommand);
            assertTrue(result.isSuccess(), "SET命令 " + keys[i] + " 应该被成功接受");
        }
        
        // 模拟提交
        simulateCommit();
        
        // 等待applier线程处理
        Thread.sleep(300);
        
        // 验证所有数据都已应用
        for (int i = 0; i < keys.length; i++) {
            String retrievedValue = getValueFromRedisCore(keys[i]);
            assertNotNull(retrievedValue, "应该能从RedisCore中获取到值: " + keys[i]);
            assertEquals(values[i], retrievedValue, "获取的值应该与设置的值一致: " + keys[i]);
            System.out.println("✓ " + keys[i] + " = " + retrievedValue);
        }
        
        System.out.println("✓ 多个SET命令都成功应用到状态机");
    }
    
    /**
     * 创建SET命令的RespArray
     */
    private RespArray createSetCommand(String key, String value) {
        return new RespArray(new BulkString[]{
            new BulkString(new RedisBytes("SET".getBytes())),
            new BulkString(new RedisBytes(key.getBytes())),
            new BulkString(new RedisBytes(value.getBytes()))
        });
    }
    
    /**
     * 模拟单节点环境下的提交过程
     * 在真实环境中，这会通过心跳和复制机制自动完成
     */
    private void simulateCommit() {
        // 获取当前最后的日志索引
        int lastLogIndex = raft.getLastLogIndex();
        
        // 在单节点环境下，直接更新commitIndex
        synchronized (raft) {
            raft.setCommitIndex(lastLogIndex);
        }
        
        // 通知applier线程
        raft.notifyApplierForTest();
    }
    
    /**
     * 从RedisCore中获取值
     */
    private String getValueFromRedisCore(String key) {
        try {
            // 直接使用RedisCore的get方法
            RedisBytes keyBytes =
                new RedisBytes(key.getBytes());
            RedisString value = (RedisString) redisCore.get(keyBytes);
            
            if (value != null) {
                // 假设返回的是字符串类型的数据
                return value.getValue().getString(); // 或者根据实际的RedisData实现来获取字符串值
            }
            return null;
        } catch (Exception e) {
            System.err.println("从RedisCore获取值时发生错误: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * 获取最后的执行结果
     * 这个方法需要根据你的RedisCore实现来调整
     */
    private String getLastExecutionResult() {
        // 由于我们已经修改了getValueFromRedisCore方法，这个方法现在不再需要
        // 保留这个方法以防万一，但实际上不会被使用
        return null;
    }
    
    @Test
    void testRaftApplierIntegration() throws InterruptedException {
        System.out.println("=== 测试Raft Applier集成 ===");
        
        // 验证初始状态
        assertEquals(0, raft.getLastApplied(), "初始lastApplied应该为0");
        assertEquals(0, raft.getCommitIndex(), "初始commitIndex应该为0");
        
        // 创建并提交SET命令
        RespArray setCommand = createSetCommand("testkey", "testvalue");
        AppendResult result = raft.start(setCommand);
        
        assertTrue(result.isSuccess(), "命令应该被成功接受");
        assertEquals(1, result.getNewLogIndex(), "应该生成日志索引1");
        
        // 验证日志已被添加但未提交
        assertEquals(1, raft.getLastLogIndex(), "日志索引应该为1");
        assertEquals(0, raft.getCommitIndex(), "commitIndex应该仍为0");
        
        // 手动提交（模拟集群中的多数同意）
        raft.setCommitIndex(1);
        raft.notifyApplierForTest();
        
        // 等待applier处理
        Thread.sleep(500);
        
        // 验证应用结果
        assertEquals(1, raft.getLastApplied(), "lastApplied应该被更新为1");
        
        // 验证数据已应用到状态机
        String retrievedValue = getValueFromRedisCore("testkey");
        assertNotNull(retrievedValue, "应该能从RedisCore中获取到值");
        
        System.out.println("✓ Raft Applier集成测试通过");
        System.out.println("  - 日志索引: " + raft.getLastLogIndex());
        System.out.println("  - 提交索引: " + raft.getCommitIndex());
        System.out.println("  - 应用索引: " + raft.getLastApplied());
        System.out.println("  - 状态机中的值: " + retrievedValue);
    }
}

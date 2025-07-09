package site.hnfy258.server;

import org.junit.jupiter.api.*;
import site.hnfy258.core.AppendResult;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.core.RoleState;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.network.NettyRaftNetwork;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.raft.RaftNode;
import site.hnfy258.server.command.executor.CommandExecutorImpl;
import site.hnfy258.server.config.RedisServerConfig;
import site.hnfy258.server.context.RedisContextImpl;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Raft状态机应用测试
 * 
 * 测试Raft日志提交后能否正确应用到RedisCore状态机
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Raft状态机应用测试")
public class RaftStateMachineTest {
    
    private static final int TEST_TIMEOUT = 10000; // 10秒超时
    private static final int COMMIT_WAIT_TIME = 2000; // 2秒等待提交
    
    private List<RaftNode> nodes;
    private List<NettyRaftNetwork> networks;
    private List<Integer> nodeIds;
    private List<Integer> ports;
    private List<RedisCore> redisCores;
    private List<RedisContextImpl> redisContexts;
    
    @BeforeEach
    void setUp() {
        // 清理持久化文件
        cleanupPersistenceFiles();
        
        nodes = new ArrayList<>();
        networks = new ArrayList<>();
        nodeIds = new ArrayList<>();
        ports = new ArrayList<>();
        redisCores = new ArrayList<>();
        redisContexts = new ArrayList<>();
    }
    
    @AfterEach
    void tearDown() {
        // 停止所有节点
        if (nodes != null) {
            for (RaftNode node : nodes) {
                if (node != null) {
                    try {
                        node.stop();
                    } catch (Exception e) {
                        System.err.println("Error stopping node: " + e.getMessage());
                    }
                }
            }
        }
        
        // 等待网络清理
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 清理持久化文件
        cleanupPersistenceFiles();
    }
    
    /**
     * 测试基本的日志应用到状态机
     */
    @Test
    @Order(1)
    @DisplayName("测试基本日志应用到状态机")
    void testBasicLogApplicationToStateMachine() throws InterruptedException {
        System.out.println("=== 测试基本日志应用到状态机 ===");
        
        // 1. 创建3节点集群
        setupClusterWithRedisCore(3);
        
        // 2. 等待选出Leader
        Thread.sleep(8000); // 等待选举完成
        int leader = findLeader();
        assertTrue(leader >= 0, "应该选出一个Leader");
        
        System.out.println("Leader选举完成，Leader节点: " + nodeIds.get(leader));
        
        // 3. 提交一些命令到Raft
        RespArray setCommand1 = createSetCommand("key1", "value1");
        RespArray setCommand2 = createSetCommand("key2", "value2");
        RespArray setCommand3 = createSetCommand("key3", "value3");
        
        // 提交命令并等待复制
        AppendResult result1 = nodes.get(leader).getRaft().start(setCommand1);
        assertTrue(result1.isSuccess(), "第一个命令应该提交成功");
        System.out.println("提交命令1: SET key1 value1, 日志索引: " + result1.getNewLogIndex());
        
        Thread.sleep(COMMIT_WAIT_TIME); // 等待命令被应用
        
        AppendResult result2 = nodes.get(leader).getRaft().start(setCommand2);
        assertTrue(result2.isSuccess(), "第二个命令应该提交成功");
        System.out.println("提交命令2: SET key2 value2, 日志索引: " + result2.getNewLogIndex());
        
        Thread.sleep(COMMIT_WAIT_TIME);
        
        AppendResult result3 = nodes.get(leader).getRaft().start(setCommand3);
        assertTrue(result3.isSuccess(), "第三个命令应该提交成功");
        System.out.println("提交命令3: SET key3 value3, 日志索引: " + result3.getNewLogIndex());
        
        Thread.sleep(COMMIT_WAIT_TIME * 2); // 给更多时间让日志被应用到状态机
        
        // 4. 验证命令是否被正确应用到状态机
        System.out.println("验证状态机中的数据...");
        
        // 检查Leader节点的状态机
        RedisCore leaderRedisCore = redisCores.get(leader);
        assertDataInStateMachine(leaderRedisCore, "key1", "value1");
        assertDataInStateMachine(leaderRedisCore, "key2", "value2");
        assertDataInStateMachine(leaderRedisCore, "key3", "value3");
        
        // 等待更长时间让所有节点都应用日志
        Thread.sleep(COMMIT_WAIT_TIME * 2);
        
        // 5. 验证所有节点的状态机都一致
        System.out.println("验证所有节点状态机一致性...");
        for (int i = 0; i < nodes.size(); i++) {
            RedisCore nodeRedisCore = redisCores.get(i);
            System.out.println("检查节点 " + nodeIds.get(i) + " 的状态机...");
            
            assertDataInStateMachine(nodeRedisCore, "key1", "value1");
            assertDataInStateMachine(nodeRedisCore, "key2", "value2");
            assertDataInStateMachine(nodeRedisCore, "key3", "value3");
        }
        
        System.out.println("✓ 所有节点状态机数据一致");
    }
    
    /**
     * 测试Leader更替后的日志应用
     */
    @Test
    @Order(2)
    @DisplayName("测试Leader更替后的日志应用")
    void testLogApplicationAfterLeaderChange() throws InterruptedException {
        System.out.println("=== 测试Leader更替后的日志应用 ===");
        
        // 1. 创建3节点集群
        setupClusterWithRedisCore(3);
        
        // 2. 等待选出Leader
        Thread.sleep(8000);
        int originalLeader = findLeader();
        assertTrue(originalLeader >= 0, "应该选出一个Leader");
        
        System.out.println("原始Leader: " + nodeIds.get(originalLeader));
        
        // 3. 在原始Leader上提交一些命令
        AppendResult result1 = nodes.get(originalLeader).getRaft().start(createSetCommand("before", "leader_change"));
        assertTrue(result1.isSuccess(), "Leader更替前的命令应该提交成功");
        
        Thread.sleep(COMMIT_WAIT_TIME);
        
        // 4. 停止原始Leader，触发重新选举
        System.out.println("停止原始Leader: " + nodeIds.get(originalLeader));
        nodes.get(originalLeader).stop();
        
        // 5. 等待新Leader选举
        System.out.println("等待新Leader选举...");
        Thread.sleep(12000); // 增加等待时间到12秒
        int newLeader = findLeader();
        assertTrue(newLeader >= 0 && newLeader != originalLeader, "应该选出新的Leader");
        
        System.out.println("新Leader: " + nodeIds.get(newLeader));
        
        // 6. 在新Leader上提交命令
        AppendResult result2 = nodes.get(newLeader).getRaft().start(createSetCommand("after", "leader_change"));
        assertTrue(result2.isSuccess(), "Leader更替后的命令应该提交成功");
        
        Thread.sleep(COMMIT_WAIT_TIME * 2);
        
        // 7. 验证两个命令都被正确应用
        for (int i = 0; i < nodes.size(); i++) {
            if (i == originalLeader) continue; // 跳过已停止的节点
            
            RedisCore nodeRedisCore = redisCores.get(i);
            System.out.println("检查节点 " + nodeIds.get(i) + " 的状态机...");
            
            assertDataInStateMachine(nodeRedisCore, "before", "leader_change");
            assertDataInStateMachine(nodeRedisCore, "after", "leader_change");
        }
        
        System.out.println("✓ Leader更替后日志应用正常");
    }
    
    /**
     * 创建包含RedisCore状态机的集群
     */
    private void setupClusterWithRedisCore(int size) throws InterruptedException {
        System.out.println("创建包含RedisCore状态机的" + size + "节点集群...");
        
        // 生成节点ID和端口
        for (int i = 0; i < size; i++) {
            nodeIds.add(i);
            ports.add(9100 + i); // 使用不同的端口范围
        }
        
        int[] peerIds = nodeIds.stream().mapToInt(Integer::intValue).toArray();
        
        // 为每个节点创建RedisCore和网络层
        for (int i = 0; i < size; i++) {
            // 1. 创建RedisCore
            RedisCore redisCore = new RedisCoreImpl(16); // 16个数据库
            redisCores.add(redisCore);
            
            // 2. 创建RedisContext
            RedisServerConfig config = RedisServerConfig.defaultConfig();
            RedisContextImpl redisContext = new RedisContextImpl(
                redisCore, 
                "127.0.0.1", 
                ports.get(i), 
                config
            );
            redisContexts.add(redisContext);
            
            // 3. 创建CommandExecutor并注入到RedisCore
            CommandExecutorImpl commandExecutor = new CommandExecutorImpl(redisContext);
            ((RedisCoreImpl) redisCore).setCommandExecutor(commandExecutor);
            
            // 4. 创建网络层
            NettyRaftNetwork network = new NettyRaftNetwork("127.0.0.1", ports.get(i));
            networks.add(network);
            
            // 配置其他节点的地址
            for (int j = 0; j < size; j++) {
                if (j != i) {
                    network.addPeer(nodeIds.get(j), "127.0.0.1", ports.get(j));
                }
            }
            
            // 5. 创建RaftNode (直接在构造函数中传入RedisCore)
            RaftNode node = new RaftNode(nodeIds.get(i), peerIds, network, redisCore);
            
            nodes.add(node);
        }
        
        // 启动所有节点
        for (int i = 0; i < size; i++) {
            nodes.get(i).start();
            System.out.println("启动节点 " + nodeIds.get(i) + " (端口: " + ports.get(i) + ")");
            Thread.sleep(500);
        }
        
        System.out.println("集群启动完成，等待选举...");
    }
    
    /**
     * 查找当前的Leader节点
     */
    private int findLeader() throws InterruptedException {
        for (int attempt = 0; attempt < 20; attempt++) {
            System.out.println("查找Leader尝试 " + (attempt + 1) + "/20");
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    RaftNode node = nodes.get(i);
                    if (!node.isStarted()) {
                        System.out.println("  节点 " + nodeIds.get(i) + " 已停止");
                        continue;
                    }
                    
                    RoleState state = node.getRaft().getState();
                    System.out.println("  节点 " + nodeIds.get(i) + " 状态: " + state);
                    
                    if (state == RoleState.LEADER) {
                        System.out.println("找到Leader: 节点 " + nodeIds.get(i));
                        return i;
                    }
                } catch (Exception e) {
                    System.out.println("  节点 " + nodeIds.get(i) + " 检查状态时异常: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }
        System.out.println("未找到Leader");
        return -1; // 没有找到Leader
    }
    
    /**
     * 创建SET命令
     */
    private RespArray createSetCommand(String key, String value) {
        Resp[] args = new Resp[3];
        args[0] = new BulkString("SET".getBytes(StandardCharsets.UTF_8));
        args[1] = new BulkString(key.getBytes(StandardCharsets.UTF_8));
        args[2] = new BulkString(value.getBytes(StandardCharsets.UTF_8));
        return new RespArray(args);
    }
    
    /**
     * 验证状态机中的数据
     */
    private void assertDataInStateMachine(RedisCore redisCore, String key, String expectedValue) {
        RedisBytes keyBytes = RedisBytes.fromString(key);
        RedisData data = redisCore.get(keyBytes);
        
        assertNotNull(data, "键 '" + key + "' 应该存在于状态机中");
        assertTrue(data instanceof RedisString, "键 '" + key + "' 应该是字符串类型");
        
        RedisString redisString = (RedisString) data;
        String actualValue = redisString.getValue().getString();
        assertEquals(expectedValue, actualValue, "键 '" + key + "' 的值应该是 '" + expectedValue + "'");
        
        System.out.println("✓ 验证成功: " + key + " = " + actualValue);
    }
    
    /**
     * 清理持久化文件
     */
    private void cleanupPersistenceFiles() {
        for (int i = 0; i < 10; i++) {
            File file = new File("node-" + i);
            if (file.exists()) {
                file.delete();
                System.out.println("Cleaned up persistence file: node-" + i);
            }
        }
    }
}

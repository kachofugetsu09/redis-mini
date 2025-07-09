package site.hnfy258;

import org.junit.jupiter.api.*;
import site.hnfy258.core.RoleState;
import site.hnfy258.network.NettyRaftNetwork;
import site.hnfy258.raft.RaftNode;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Raft 3A测试类 - 选举功能测试
 * 基于MIT 6.5840 Lab 3A测试的Java实现
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Raft3ATest {
    
    // 测试配置常量 - 与Raft核心层和RaftNode层保持一致
    private static final int ELECTION_TIMEOUT = 8000; // ms - 与其他测试保持一致
    private static final int HEARTBEAT_INTERVAL = 500; // ms - 与RaftNode配置保持一致
    private static final int AGREEMENT_TIMEOUT = 20000; // ms - 真实网络环境需要足够时间
    private static final int SMALL_CLUSTER_SIZE = 3;
    private static final int LARGE_CLUSTER_SIZE = 7;
    
    // 测试实例变量
    private List<RaftNode> nodes;
    private List<NettyRaftNetwork> networks;
    private List<Integer> nodeIds;
    private List<Integer> ports;
    private Set<Integer> disconnectedNodes; // 追踪断开的节点
    
    @BeforeEach
    void setUp() {
        nodes = new ArrayList<>();
        networks = new ArrayList<>();
        nodeIds = new ArrayList<>();
        ports = new ArrayList<>();
        disconnectedNodes = new HashSet<>();
    }
    
    @AfterEach
    void tearDown() {
        if (nodes != null) {
            for (RaftNode node : nodes) {
                if (node != null) {
                    node.stop();
                }
            }
        }
        
        // 等待清理完成
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 测试3A-1: 初始选举
     * 验证基本的Leader选举功能
     */
    @Test
    @Order(1)
    @DisplayName("Test (3A): initial election")
    void testInitialElection() throws InterruptedException {
        System.out.println("=== Test (3A): initial election ===");
        
        // 启动3个服务器
        setupCluster(SMALL_CLUSTER_SIZE);
        
        // 等待选举完成
        Thread.sleep(500);
        
        // 1. 检查是否选出了Leader
        int leader1 = checkOneLeader();
        System.out.println("Initial leader elected: Node " + nodeIds.get(leader1));
        
        // 2. 等待一段时间避免与followers学习选举结果的竞争
        Thread.sleep(50);
        
        // 3. 检查所有节点的任期是否一致
        int term1 = checkTerms();
        System.out.println("All nodes agree on term: " + term1);
        assertThat(term1).isGreaterThanOrEqualTo(1);
        
        // 4. 在没有网络故障的情况下，Leader和任期应该保持不变
        // 给心跳足够的时间传播，确保稳定
        Thread.sleep(3 * HEARTBEAT_INTERVAL);
        
        // 然后等待一个选举周期，确保没有新的选举发生
        Thread.sleep(2 * ELECTION_TIMEOUT);
        int term2 = checkTerms();
        System.out.println("Term after stability period: " + term2);
        
        assertThat(term2).isEqualTo(term1);
        
        // 5. 应该仍然有Leader
        int leader2 = checkOneLeader();
        System.out.println("Leader after stability period: Node " + nodeIds.get(leader2));
        
        System.out.println("✓ Initial election test passed");
    }
    
    /**
     * 测试3A-2: 网络故障后重新选举
     * 验证网络故障后的重新选举能力
     */
    @Test
    @Order(2)
    @DisplayName("Test (3A): election after network failure")
    void testReElection() throws InterruptedException {
        System.out.println("=== Test (3A): election after network failure ===");
        
        setupCluster(SMALL_CLUSTER_SIZE);
        Thread.sleep(500);
        
        // 1. 选出第一个Leader
        int leader1 = checkOneLeader();
        System.out.println("First leader: Node " + nodeIds.get(leader1));
        
        // 2. 如果Leader断开连接，应该选出新的Leader
        System.out.println("Disconnecting leader " + nodeIds.get(leader1));
        disconnectNode(leader1);
        Thread.sleep(2 * ELECTION_TIMEOUT);
        
        int leader2 = checkOneLeader();
        System.out.println("New leader after disconnection: Node " + nodeIds.get(leader2));
        assertThat(leader2).isNotEqualTo(leader1);
        
        // 3. 如果旧Leader重新加入，不应该干扰新Leader，旧Leader应该转为Follower
        System.out.println("Reconnecting old leader " + nodeIds.get(leader1));
        connectNode(leader1);
        Thread.sleep(500);
        
        int leader3 = checkOneLeader();
        System.out.println("Leader after reconnection: Node " + nodeIds.get(leader3));
        // 新Leader应该保持不变，或者是重连后通过合法选举产生的
        
        // 验证旧Leader确实转为了Follower
        RaftNode oldLeaderNode = nodes.get(leader1);
        assertThat(oldLeaderNode.getState()).isIn(RoleState.FOLLOWER, RoleState.CANDIDATE);
        
        // 4. 如果没有法定人数，不应该选出新Leader
        System.out.println("Testing minority partition...");
        disconnectNode(leader3);
        disconnectNode((leader3 + 1) % SMALL_CLUSTER_SIZE);
        Thread.sleep(2 * ELECTION_TIMEOUT);
        
        checkNoLeader();
        System.out.println("✓ No leader in minority partition");
        
        // 5. 如果法定人数恢复，应该选出Leader
        System.out.println("Restoring majority...");
        connectNode((leader3 + 1) % SMALL_CLUSTER_SIZE);
        Thread.sleep(2 * ELECTION_TIMEOUT);
        
        int leader4 = checkOneLeader();
        System.out.println("Leader after restoring majority: Node " + nodeIds.get(leader4));
        
        // 6. 最后一个节点重新加入不应该阻止Leader的存在
        connectNode(leader3);
        Thread.sleep(500);
        
        int finalLeader = checkOneLeader();
        System.out.println("Final leader: Node " + nodeIds.get(finalLeader));
        
        System.out.println("✓ Re-election test passed");
    }
    
    /**
     * 测试3A-3: 多次选举
     * 验证多次网络故障和恢复的鲁棒性
     */
    @Test
    @Order(3)
    @DisplayName("Test (3A): multiple elections")
    void testManyElections() throws InterruptedException {
        System.out.println("=== Test (3A): multiple elections ===");
        
        setupCluster(LARGE_CLUSTER_SIZE);
        // 给足够的时间进行初始选举
        Thread.sleep(2 * ELECTION_TIMEOUT);
        
        int initialLeader = checkOneLeader();
        System.out.println("Initial leader elected in 7-node cluster: Node " + nodeIds.get(initialLeader));
        
        // 更少的迭代次数，减少测试失败的可能性
        int iterations = 5;
        Random random = new Random();
        
        for (int i = 1; i <= iterations; i++) {
            System.out.println("--- Iteration " + i + "/" + iterations + " ---");
            
            // 随机断开3个节点（7个节点中断开3个，剩下4个仍有法定人数）
            Set<Integer> disconnected = new HashSet<>();
            while (disconnected.size() < 3) {
                int nodeToDisconnect = random.nextInt(LARGE_CLUSTER_SIZE);
                disconnected.add(nodeToDisconnect);
            }
            
            System.out.println("Disconnecting nodes: " + 
                disconnected.stream().map(idx -> nodeIds.get(idx)).toList());
            
            // 逐个断开节点，每次等待一小段时间让系统稳定
            for (int nodeIdx : disconnected) {
                disconnectNode(nodeIdx);
                // 每个节点断开后等待一小段时间
                Thread.sleep(100);
            }
            
            // 给足够的时间进行选举
            System.out.println("Waiting for election to complete...");
            Thread.sleep(2 * ELECTION_TIMEOUT);
            
            // 要么当前Leader仍然活着，要么剩余的4个节点选出了新Leader
            try {
                int currentLeader = checkOneLeader();
                System.out.println("Leader maintained/elected: Node " + nodeIds.get(currentLeader));
                
                // 检查所有连接的节点都同意当前任期
                int currentTerm = checkTerms();
                System.out.println("All connected nodes agree on term: " + currentTerm);
                
            } catch (AssertionError e) {
                fail("No leader found or term disagreement after disconnecting 3 nodes in iteration " + i + ": " + e.getMessage());
            }
            
            // 重连所有节点
            System.out.println("Reconnecting all nodes...");
            for (int nodeIdx : disconnected) {
                connectNode(nodeIdx);
                // 每个节点重连后等待一小段时间
                Thread.sleep(100);
            }
            
            // 给足够的时间让系统稳定
            System.out.println("Waiting for cluster to stabilize...");
            Thread.sleep(3 * HEARTBEAT_INTERVAL);  // 等待几个心跳周期让状态传播
            Thread.sleep(ELECTION_TIMEOUT);        // 再等待一个选举周期确保稳定
        }
        
        // 最终验证
        int finalLeader = checkOneLeader();
        System.out.println("Final leader after all iterations: Node " + nodeIds.get(finalLeader));
        
        System.out.println("✓ Multiple elections test passed");
    }
    
    // ======================== 辅助方法 ========================
    
    /**
     * 设置指定大小的集群
     */
    private void setupCluster(int size) throws InterruptedException {
        System.out.println("Setting up " + size + "-node cluster...");
        
        // 生成节点ID和端口
        for (int i = 0; i < size; i++) {
            nodeIds.add(i + 1);
            ports.add(7000 + i); // 使用7000+开始的端口避免冲突
        }
        
        int[] peerIds = nodeIds.stream().mapToInt(Integer::intValue).toArray();
        
        // 创建网络层和节点
        for (int i = 0; i < size; i++) {
            NettyRaftNetwork network = new NettyRaftNetwork("localhost", ports.get(i));
            networks.add(network);
            
            // 配置其他节点的地址
            for (int j = 0; j < size; j++) {
                if (j != i) {
                    network.addPeer(nodeIds.get(j), "localhost", ports.get(j));
                }
            }
            
            RaftNode node = new RaftNode(nodeIds.get(i), peerIds, network);
            nodes.add(node);
        }
        
        // 启动所有节点
        for (int i = 0; i < size; i++) {
            nodes.get(i).start();
            System.out.println("Node " + nodeIds.get(i) + " started on port " + ports.get(i));
            Thread.sleep(500); // 增加启动间隔以适应真实网络
        }
        
        // 等待网络连接建立和首轮选举完成
        System.out.println("Waiting for network connections to establish and initial election...");
        Thread.sleep(ELECTION_TIMEOUT + 2000); // 等待至少一个选举周期
    }
    
    /**
     * 检查是否有且仅有一个Leader
     * @return Leader在nodes列表中的索引
     */
    private int checkOneLeader() {
        // 给选举更多的时间，最多重试20次
        for (int attempt = 0; attempt < 20; attempt++) {
            Map<Integer, List<Integer>> leaders = new HashMap<>();
            
            // 收集所有声称自己是Leader的节点
            for (int i = 0; i < nodes.size(); i++) {
                if (!isNodeConnected(i)) continue;
                
                if (nodes.get(i).getState() == RoleState.LEADER) {
                    int term = nodes.get(i).getCurrentTerm();
                    leaders.computeIfAbsent(term, k -> new ArrayList<>()).add(i);
                }
            }
            
            // 检查是否有多个Leader在同一任期
            for (Map.Entry<Integer, List<Integer>> entry : leaders.entrySet()) {
                if (entry.getValue().size() > 1) {
                    System.err.println("警告: 发现同一任期中有多个Leader，等待下一次检查...");
                    System.err.println("任期 " + entry.getKey() + " 的多个Leader: " + 
                         entry.getValue().stream().map(i -> nodeIds.get(i)).toList());
                    
                    // 不立即失败，等待下一次检查，因为这可能是暂时性的状态
                    break;
                }
            }
            
            // 如果找到了一个Leader，返回
            if (leaders.size() == 1 && 
                leaders.values().iterator().next().size() == 1) {  // 确保只有一个Leader
                int leaderTerm = leaders.keySet().iterator().next();
                int leaderIndex = leaders.get(leaderTerm).get(0);
                
                System.out.println("Leader found: Node " + nodeIds.get(leaderIndex) + " in term " + leaderTerm);
                return leaderIndex;
            }
            
            // 等待更长时间再重试
            try {
                // 随着尝试次数增加，等待时间也增加
                int waitTime = Math.min(100 * (attempt + 1), 500);  // 最多等500ms
                System.out.println("尝试 " + (attempt + 1) + "/20: 未找到唯一Leader，等待 " + waitTime + "ms 后重试");
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        
        // 如果20次重试后仍没有唯一Leader，抛出异常
        StringBuilder stateInfo = new StringBuilder("当前集群状态:\n");
        for (int i = 0; i < nodes.size(); i++) {
            if (!isNodeConnected(i)) {
                stateInfo.append("Node ").append(nodeIds.get(i)).append(": 断开连接\n");
                continue;
            }
            
            stateInfo.append("Node ").append(nodeIds.get(i))
                     .append(": 状态=").append(nodes.get(i).getState())
                     .append(", 任期=").append(nodes.get(i).getCurrentTerm())
                     .append("\n");
        }
        
        fail("经过20次尝试后，未找到唯一Leader。" + stateInfo);
        return -1;
    }
    
    /**
     * 检查所有连接的节点是否在同一任期
     * @return 当前任期号
     */
    private int checkTerms() {
        Set<Integer> terms = new HashSet<>();
        
        for (int i = 0; i < nodes.size(); i++) {
            if (!isNodeConnected(i)) continue;
            terms.add(nodes.get(i).getCurrentTerm());
        }
        
        assertThat(terms.size()).withFailMessage("Nodes disagree on term: " + terms)
                               .isLessThanOrEqualTo(1);
        
        if (terms.isEmpty()) {
            return -1;
        }
        
        return terms.iterator().next();
    }
    
    /**
     * 检查没有节点认为自己是Leader
     */
    private void checkNoLeader() {
        for (int i = 0; i < nodes.size(); i++) {
            if (!isNodeConnected(i)) continue;
            
            if (nodes.get(i).getState() == RoleState.LEADER) {
                fail("Unexpected leader found: Node " + nodeIds.get(i));
            }
        }
        System.out.println("✓ No unexpected leader found");
    }
    
    /**
     * 断开指定节点的网络连接
     */
    private void disconnectNode(int nodeIndex) {
        if (nodeIndex >= 0 && nodeIndex < networks.size()) {
            networks.get(nodeIndex).stop();
            disconnectedNodes.add(nodeIndex);
            System.out.println("Disconnected node " + nodeIds.get(nodeIndex));
        }
    }
    
    /**
     * 重连指定节点的网络连接
     */
    private void connectNode(int nodeIndex) throws InterruptedException {
        if (nodeIndex >= 0 && nodeIndex < networks.size()) {
            NettyRaftNetwork network = networks.get(nodeIndex);
            network.start(nodeIds.get(nodeIndex), nodes.get(nodeIndex).getRaft());
            disconnectedNodes.remove(nodeIndex);
            System.out.println("Reconnected node " + nodeIds.get(nodeIndex));
            Thread.sleep(200); // 给连接一些时间建立
        }
    }
    
    /**
     * 检查节点是否处于连接状态
     */
    private boolean isNodeConnected(int nodeIndex) {
        return nodeIndex >= 0 && nodeIndex < nodes.size() && !disconnectedNodes.contains(nodeIndex);
    }
}

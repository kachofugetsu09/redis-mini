package site.hnfy258.server;

import site.hnfy258.network.NettyRaftNetwork;
import site.hnfy258.raft.RaftNode;
import site.hnfy258.server.config.RedisServerConfig;
import site.hnfy258.server.context.RedisContextImpl;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Raft集群启动器
 * 仿照Raft3BTest的方式建立真实的网络连接
 */
public class RaftClusterStarter {
    
    private static final int NUM_NODES = 3;
    private static final int BASE_REDIS_PORT = 6379;
    private static final int BASE_RAFT_PORT = 8379;

    private RedisMiniServer[] servers;
    private RaftNode[] raftNodes;
    private NettyRaftNetwork[] networks;
    private ExecutorService executorService;

    public static void main(String[] args) throws Exception {
        RaftClusterStarter starter = new RaftClusterStarter();
        starter.startCluster();

        // 监控集群状态
        starter.monitorCluster();

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("正在关闭Raft集群...");
            starter.shutdown();
        }));
    }

    public void startCluster() throws Exception {
        servers = new RedisMiniServer[NUM_NODES];
        raftNodes = new RaftNode[NUM_NODES];
        networks = new NettyRaftNetwork[NUM_NODES];
        executorService = Executors.newFixedThreadPool(NUM_NODES);

        // 第一步：创建所有网络层
        System.out.println("=== 创建网络层 ===");
        for (int i = 0; i < NUM_NODES; i++) {
            int raftPort = BASE_RAFT_PORT + i;
            networks[i] = new NettyRaftNetwork("127.0.0.1", raftPort);
            System.out.println("创建网络层 node-" + i + " 监听端口: " + raftPort);
        }

        // 第二步：配置节点间连接（仿照Raft3BTest）
        System.out.println("=== 配置节点间连接 ===");
        for (int i = 0; i < NUM_NODES; i++) {
            for (int j = 0; j < NUM_NODES; j++) {
                if (i != j) {
                    networks[i].addPeer(j, "127.0.0.1", BASE_RAFT_PORT + j);
                    System.out.println("node-" + i + " 添加 peer node-" + j + " -> 127.0.0.1:" + (BASE_RAFT_PORT + j));
                }
            }
        }

        // 第三步：创建所有RedisMiniServer（先创建但不启动）
        System.out.println("=== 创建RedisMiniServer ===");
        for (int i = 0; i < NUM_NODES; i++) {
            // 创建Redis服务器配置
            RedisServerConfig config = RedisServerConfig.raftClusterConfig(
                String.valueOf(i),
                BASE_REDIS_PORT + i,
                BASE_RAFT_PORT + i
            );

            // 创建Redis服务器
            servers[i] = new RedisMiniServer(config);
            System.out.println("创建 RedisMiniServer-" + i);
        }

        // 第四步：创建并启动所有RaftNode，传入RedisCore
        System.out.println("=== 创建并启动RaftNode ===");
        int[] peerIds = new int[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            peerIds[i] = i;
        }

        for (int i = 0; i < NUM_NODES; i++) {
            final int nodeId = i;

            // 获取RedisCore实例
            RedisContextImpl context = (RedisContextImpl) servers[nodeId].getRedisContext();

            // 创建RaftNode，传入网络层和RedisCore
            raftNodes[i] = new RaftNode(nodeId, peerIds, networks[i], context.getRedisCore());

            // 注入Raft实例到Redis上下文
            context.setRaft(raftNodes[i].getRaft());

            // 启动RaftNode（这会启动网络层和选举定时器）
            raftNodes[i].start();
            System.out.println("启动 RaftNode-" + i);
        }

        // 第五步：启动所有RedisMiniServer
        System.out.println("=== 启动RedisMiniServer ===");
        for (int i = 0; i < NUM_NODES; i++) {
            final int nodeId = i;

            executorService.submit(() -> {
                try {
                    // 启动服务器
                    servers[nodeId].start();
                    System.out.println("启动 RedisMiniServer-" + nodeId + " 端口: " + (BASE_REDIS_PORT + nodeId));

                } catch (Exception e) {
                    System.err.println("启动 RedisMiniServer-" + nodeId + " 失败: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }

        // 等待所有服务器启动完成
        Thread.sleep(2000);
        System.out.println("=== Raft集群启动完成 ===");
        printClusterStatus();
    }

    public void monitorCluster() {
        // 启动监控线程
        Thread monitorThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000); // 每5秒打印一次状态
                    printClusterStatus();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void printClusterStatus() {
        System.out.println("\n=== 集群状态 ===");
        for (int i = 0; i < NUM_NODES; i++) {
            if (raftNodes[i] != null) {
                String state = raftNodes[i].getState().toString();
                long term = raftNodes[i].getCurrentTerm();
                System.out.println("Node-" + i + ": " + state + " (Term: " + term + ")");
            }
        }
        System.out.println("================\n");
    }

    public void shutdown() {
        System.out.println("正在关闭Raft集群...");

        // 关闭所有服务器
        if (servers != null) {
            for (int i = 0; i < NUM_NODES; i++) {
                if (servers[i] != null) {
                    try {
                        servers[i].stop();
                        System.out.println("关闭 RedisMiniServer-" + i);
                    } catch (Exception e) {
                        System.err.println("关闭 RedisMiniServer-" + i + " 失败: " + e.getMessage());
                    }
                }
            }
        }

        // 关闭所有RaftNode
        if (raftNodes != null) {
            for (int i = 0; i < NUM_NODES; i++) {
                if (raftNodes[i] != null) {
                    try {
                        raftNodes[i].stop();
                        System.out.println("关闭 RaftNode-" + i);
                    } catch (Exception e) {
                        System.err.println("关闭 RaftNode-" + i + " 失败: " + e.getMessage());
                    }
                }
            }
        }

        // 关闭网络层
        if (networks != null) {
            for (int i = 0; i < NUM_NODES; i++) {
                if (networks[i] != null) {
                    try {
                        networks[i].stop();
                        System.out.println("关闭 NettyRaftNetwork-" + i);
                    } catch (Exception e) {
                        System.err.println("关闭 NettyRaftNetwork-" + i + " 失败: " + e.getMessage());
                    }
                }
            }
        }

        // 关闭线程池
        if (executorService != null) {
            executorService.shutdown();
        }

        System.out.println("Raft集群已关闭");
    }
}

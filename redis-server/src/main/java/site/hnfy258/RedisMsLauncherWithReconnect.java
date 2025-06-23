package site.hnfy258;  
  
import lombok.extern.slf4j.Slf4j;  
import site.hnfy258.cluster.node.RedisNode;  
import site.hnfy258.server.RedisMiniServer;  
import site.hnfy258.server.RedisServer;  
import site.hnfy258.server.config.RedisServerConfig;
  
import java.util.concurrent.Executors;  
import java.util.concurrent.ScheduledExecutorService;  
import java.util.concurrent.TimeUnit;  
  
/**  
 * Redis主从复制启动器，包含断线重连测试 * 用于测试心跳管理器和增量同步功能 */@Slf4j  
public class RedisMsLauncherWithReconnect {    private static final int DISCONNECT_DELAY_SECONDS = 15;  
    private static final int RECONNECT_DELAY_SECONDS = 8;  
    private static RedisMiniServer masterServer;  
    private static RedisNode masterNode;  
    private static RedisMiniServer slaveServer1;  
    private static RedisNode slaveNode1;  
    private static RedisMiniServer slaveServer2;  
    private static RedisNode slaveNode2;
    private static final ScheduledExecutorService scheduler =   
Executors.newScheduledThreadPool(2);  
    public static void main(String[] args) throws Exception {  
        // 1. 启动主节点  
        log.info("=== 启动Redis主从复制集群 ===");  
        startMasterNode();  
        Thread.sleep(1000);  
        // 2. 启动从节点  
        startSlaveNodes();  
        Thread.sleep(2000);  
        // 3. 建立主从关系  
        establishMasterSlaveRelationship();  
        // 4. 启动连接  
        initializeConnections();  
        // 5. 调度断线重连测试  
        scheduleDisconnectionTest();  
        // 6. 注册关闭钩子  
        registerShutdownHook();  
        log.info("=== Redis集群启动完成，等待断线重连测试 ===");  
        log.info("将在{}秒后模拟slave-node1断线", DISCONNECT_DELAY_SECONDS);  
    }  
    /**  
     * 启动主节点     */

    private static void startMasterNode() throws Exception {
        RedisServerConfig masterConfig = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .rdbEnabled(true)
                .rdbFileName("master.rdb")
                .replicationEnabled(true)
                .replicationBufferSize(1024 * 1024) // 1MB复制缓冲区
                .backlogSize(1024)
                .workerThreadCount(4)
                .commandExecutorThreadCount(1) // 保持单线程命令执行
                .build();
                
        masterServer = new RedisMiniServer(masterConfig);  
        masterNode = new RedisNode(masterServer, "localhost", 6379, true, "master-node");  
        masterServer.setRedisNode(masterNode);  
        masterServer.start();  
        log.info("主节点启动完成: {}:{}", masterNode.getHost(), masterNode.getPort());  
    }  
    /**  
     * 启动从节点     */    
    private static void startSlaveNodes() throws Exception {  
        // 启动从节点1  
        RedisServerConfig slave1Config = RedisServerConfig.builder()
                .host("localhost")
                .port(6380)
                .rdbEnabled(true)
                .rdbFileName("slave1.rdb")
                .replicationEnabled(true)
                .replicationBufferSize(1024 * 1024) // 1MB复制缓冲区
                .backlogSize(1024)
                .workerThreadCount(4)
                .commandExecutorThreadCount(1) // 保持单线程命令执行
                .build();
                
        slaveServer1 = new RedisMiniServer(slave1Config);  
        slaveNode1 = new RedisNode(slaveServer1, "localhost", 6380, false, "slave-node1");  
        slaveServer1.setRedisNode(slaveNode1);  
        slaveServer1.start();  
        // 启动从节点2  
        RedisServerConfig slave2Config = RedisServerConfig.builder()
                .host("localhost")
                .port(6381)
                .rdbEnabled(true)
                .rdbFileName("slave2.rdb")
                .replicationEnabled(true)
                .replicationBufferSize(1024 * 1024) // 1MB复制缓冲区
                .backlogSize(1024)
                .workerThreadCount(4)
                .commandExecutorThreadCount(1) // 保持单线程命令执行
                .build();
                
        slaveServer2 = new RedisMiniServer(slave2Config);  
        slaveNode2 = new RedisNode(slaveServer2, "localhost", 6381, false, "slave-node2");  
        slaveServer2.setRedisNode(slaveNode2);  
        slaveServer2.start();  
        log.info("从节点启动完成: slave-node1({}:{}), slave-node2({}:{})",  
                slaveNode1.getHost(), slaveNode1.getPort(),  
                slaveNode2.getHost(), slaveNode2.getPort());  
    }  
    /**  
     * 建立主从关系     */    private static void establishMasterSlaveRelationship() {  
        masterNode.addSlaveNode(slaveNode1);  
        masterNode.addSlaveNode(slaveNode2);  
        slaveNode1.setMasterNode(masterNode);  
        slaveNode2.setMasterNode(masterNode);  
        log.info("主从关系建立完成，主节点从节点数量: {}", masterNode.getSlaves().size());  
    }  
    /**  
     * 初始化连接     */    private static void initializeConnections() {  
        try {  
            log.info("开始初始化从节点连接...");  
            slaveNode1.connectInit().thenRun(() -> {  
                log.info("slave-node1 连接初始化完成");  
            }).exceptionally(throwable -> {  
                log.error("slave-node1 连接初始化失败", throwable);  
                return null;  
            });  
            slaveNode2.connectInit().thenRun(() -> {  
                log.info("slave-node2 连接初始化完成");  
            }).exceptionally(throwable -> {  
                log.error("slave-node2 连接初始化失败", throwable);  
                return null;  
            });  
            } catch (Exception e) {  
            log.error("连接初始化过程中发生错误", e);  
        }  
    }  
    /**  
     * 调度断线重连测试     */    private static void scheduleDisconnectionTest() {  
        // 15秒后断开slave-node1  
        scheduler.schedule(() -> {  
            try {  
                log.warn("=== 开始模拟 slave-node1 断线 ===");  
                simulateSlaveDisconnection();  
            } catch (Exception e) {  
                log.error("模拟断线失败", e);  
            }  
        }, DISCONNECT_DELAY_SECONDS, TimeUnit.SECONDS);  
        // 20秒后重连slave-node1  
        scheduler.schedule(() -> {  
            try {  
                log.info("=== 开始模拟 slave-node1 重连 ===");  
                simulateSlaveReconnection();  
            } catch (Exception e) {  
                log.error("模拟重连失败", e);  
            }  
        }, DISCONNECT_DELAY_SECONDS + RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);  
    }  
    /**  
     * 模拟从节点断线     */    private static void simulateSlaveDisconnection() {  
        try {  
            log.warn("正在断开 slave-node1 连接...");  
            // 1. 获取当前连接状态  
            boolean wasConnected = slaveNode1.isConnected();  
            long currentOffset = slaveNode1.getNodeState().getReplicationOffset();  
            log.info("断线前状态 - 连接: {}, 偏移量: {}", wasConnected, currentOffset);  
            // 2. 强制关闭客户端连接  
            if (slaveNode1.getClientChannel() != null && slaveNode1.getClientChannel().isActive()) {  
                slaveNode1.getClientChannel().close().sync();  
                log.info("slave-node1 客户端连接已关闭");  
            }  
            // 3. 更新连接状态  
            slaveNode1.setConnected(false);  
            log.warn("slave-node1 断线完成，{}秒后将尝试重连", RECONNECT_DELAY_SECONDS);  
            } catch (Exception e) {  
            log.error("模拟断线过程中发生错误", e);  
        }  
    }  


    /**
     * 模拟从节点重连     */    private static void simulateSlaveReconnection() {  
        try {  
            log.info("正在重连 slave-node1...");  
            // 1. 检查是否有保存的连接信息  
            boolean hasSavedInfo = slaveNode1.getNodeState().hasSavedConnectionInfo();  
            String savedMasterId = slaveNode1.getNodeState().getLastKnownMasterId();  
            long savedOffset = slaveNode1.getNodeState().getLastKnownOffset();  
            log.info("重连前检查 - 有保存信息: {}, masterId: {}, offset: {}",  
                    hasSavedInfo, savedMasterId, savedOffset);  
            // 2. 尝试重连  
            slaveNode1.connectInit().thenRun(() -> {  
                log.info("=== slave-node1 重连成功！===");  
                log.info("当前状态 - 连接: {}, 偏移量: {}",  
                        slaveNode1.isConnected(),  
                        slaveNode1.getNodeState().getReplicationOffset());  
                // 3. 验证心跳管理器是否重新启动  
                if (slaveNode1.getHeartbeatManager() != null) {  
                    boolean heartbeatRunning = slaveNode1.getHeartbeatManager().isHeartbeatRunning();  
                    log.info("心跳管理器状态: {}", heartbeatRunning ? "运行中" : "已停止");  
                }  
                }).exceptionally(throwable -> {  
                log.error("slave-node1 重连失败", throwable);  
                return null;  
            });  
            } catch (Exception e) {  
            log.error("模拟重连过程中发生错误", e);  
        }  
    }  
    /**  
     * 注册关闭钩子     */    private static void registerShutdownHook() {  
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {  
            try {  
                log.info("=== 开始关闭Redis集群 ===");  
                // 关闭调度器  
                if (scheduler != null && !scheduler.isShutdown()) {  
                    scheduler.shutdown();  
                    try {  
                        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {  
                            scheduler.shutdownNow();  
                        }  
                    } catch (InterruptedException e) {  
                        scheduler.shutdownNow();  
                        Thread.currentThread().interrupt();  
                    }  
                }  
                // 停止服务器  
                if (masterServer != null) {  
                    masterServer.stop();  
                }  
                if (slaveServer1 != null) {  
                    slaveServer1.stop();  
                }  
                if (slaveServer2 != null) {  
                    slaveServer2.stop();  
                }  
                // 清理节点  
                if (masterNode != null) {  
                    masterNode.cleanup();  
                }  
                if (slaveNode1 != null) {  
                    slaveNode1.cleanup();  
                }  
                if (slaveNode2 != null) {  
                    slaveNode2.cleanup();  
                }  
                log.info("Redis集群关闭完成");  
                } catch (Exception e) {  
                log.error("关闭集群时发生错误", e);  
            }  
        }));  
    }  
}
package site.hnfy258;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.host.ReplicationHost;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;
import site.hnfy258.server.config.RedisServerConfig;

@Slf4j
public class RedisMsLauncher {
    public static void main(String[] args) throws Exception {
        // 主节点配置
        RedisServerConfig masterConfig = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .build();
        
        RedisMiniServer masterServer = new RedisMiniServer(masterConfig);
        RedisNode masterNode = new RedisNode(masterServer, "localhost", 6379, true, "master-node");
        masterServer.setRedisNode(masterNode);
        masterServer.start();

        log.info("主节点在{}上启动，端口为{}", masterNode.getHost(), masterNode.getPort());

        Thread.sleep(1000);

        // 从节点1配置
        RedisServerConfig slave1Config = RedisServerConfig.builder()
                .host("localhost")
                .port(6380)
                .rdbFileName("slave1.rdb")
                .build();
        
        RedisMiniServer slaveServer1 = new RedisMiniServer(slave1Config);
        RedisNode slaveNode1 = new RedisNode(slaveServer1, "localhost", 6380, false, "slave-node1");
        slaveServer1.setRedisNode(slaveNode1);
        slaveServer1.start();

        // 从节点2配置
        RedisServerConfig slave2Config = RedisServerConfig.builder()
                .host("localhost")
                .port(6381)
                .rdbFileName("slave2.rdb")
                .build();
        
        RedisMiniServer slaveServer2 = new RedisMiniServer(slave2Config);
        RedisNode slaveNode2 = new RedisNode(slaveServer2, "localhost", 6381, false, "slave-node2");
        slaveServer2.setRedisNode(slaveNode2);
        slaveServer2.start();

        Thread.sleep(2000);

        masterNode.addSlaveNode(slaveNode1);
        masterNode.addSlaveNode(slaveNode2);

        log.info("从节点已添加到主节点，当前从节点数量: {}", masterNode.getSlaves().size());

        slaveNode1.setMasterNode(masterNode);
        slaveNode2.setMasterNode(masterNode);

        slaveNode1.connectInit();
        slaveNode2.connectInit();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                masterServer.stop();
                slaveServer1.stop();
                slaveServer2.stop();
                masterNode.cleanup();
                slaveNode1.cleanup();
                slaveNode2.cleanup();
                Thread.sleep(500);
            } catch (Exception e) {
                log.error("关闭服务器时发生错误", e);
            }
        }));
    }
}

package site.hnfy258.cluster.replication;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.RedisServer;

import java.net.InetSocketAddress;

@Slf4j
@Setter
@Getter
public class ReplicationManager {
    private RedisNode node;
    private RedisServer redisServer;
    private ReplicationTransfer transfer;

    public ReplicationManager(RedisNode node) {
        this.node = node;
        this.redisServer = node.getRedisServer();
        this.transfer = new ReplicationTransfer(node);
    }

    public Resp doFullSync(ChannelHandlerContext ctx){
        if(!node.isMaster()){
            return new Errors("不是主节点，无法执行全量同步");
        }

        String remoteAddress = ReplicationUtils.getRemoteAddress(ctx);
        log.info("开始全量同步，来自: {}", remoteAddress);

        try{
            //1.生成rdb数据
            byte[] rdbContent = transfer.generateRdbData(redisServer.getRdbManager());

            //2.更新主节点偏移量
            updateMasterOffset(rdbContent.length);

            //3.发送同步数据
            transfer.sendFullSyncData(ctx,rdbContent, node.getNodeId(),node.getNodeState().getReplicationOffset());

            //4.更新从节点状态
            updateSlaveStateAfterSync(ctx,rdbContent.length);

            return null;
        }catch(Exception e){
            log.error("全量同步失败", e);
            return new Errors("全量同步失败: " + e.getMessage());
        }
    }

    private void updateSlaveStateAfterSync(ChannelHandlerContext ctx, int length) {
        RedisNode slaveNode = findSlaveNodeForConnection(ctx);
        if(slaveNode != null){
            updateSlaveNodeAfterSync(slaveNode,ctx,length);
        }
    }

    private void updateSlaveNodeAfterSync(RedisNode slaveNode, ChannelHandlerContext ctx, int length) {
        slaveNode.getNodeState().setChannel(ctx.channel());
        slaveNode.getNodeState().setConnected(true);
        slaveNode.getNodeState().setReplicationOffset(length);
        slaveNode.getNodeState().setReadyForReplCommands(true);
    }

    private RedisNode findSlaveNodeForConnection(ChannelHandlerContext ctx) {
        try{
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            String remoteHost = socketAddress.getHostString();
            if(remoteHost.startsWith("/")){
                remoteHost = remoteHost.substring(1);
            }

            for(RedisNode slave : node.getSlaves()){
                if(slave.getChannel() == null || !slave.getChannel().isActive()) {
                    return slave;
                }
            }
        }catch(Exception e){
            log.error("查找从节点失败", e);
        }
        return null;
    }

    private void updateMasterOffset(int length) {
        long currentOffset = node.getNodeState().getMasterReplicationOffset();
        if (currentOffset == 0) {
            node.getNodeState().setMasterReplicationOffset(length);
            log.info("更新主节点偏移量为: {}", length);
        }
    }

    public boolean receiveAndLoadRdb(byte[] rdbData) {
        if(node.isMaster()){
            log.error("主节点不应该接收RDB数据");
            return false;
        }
        try{
            String tempRdbFile = "temp-slave-" + System.currentTimeMillis() + ".rdb";
            RdbManager rdbManager = new RdbManager(node.getRedisCore(), tempRdbFile);

            boolean success = transfer.receiveAndLoadRdb(rdbData,rdbManager);
            if(success){
                log.info("从节点 {} 加载RDB数据成功", node.getNodeId());
                node.getNodeState().setReadyForReplCommands(true);
                return true;
            } else {
                log.error("从节点 {} 加载RDB数据失败", node.getNodeId());
                return false;
            }
        }catch(Exception e){
            log.error("从节点 {} 加载RDB数据时发生错误", node.getNodeId(), e);
            return false;
        }
    }
}

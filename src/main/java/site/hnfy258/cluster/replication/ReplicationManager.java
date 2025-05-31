package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.cluster.replication.utils.ReplicationTransfer;
import site.hnfy258.cluster.replication.utils.ReplicationUtils;
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

            //3.发送同步数据 - 使用偏移量0，符合Redis规范
            transfer.sendFullSyncData(ctx,rdbContent, node.getNodeId(), 0);

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
    }    private void updateSlaveNodeAfterSync(RedisNode slaveNode, ChannelHandlerContext ctx, int length) {
        slaveNode.getNodeState().setChannel(ctx.channel());
        slaveNode.getNodeState().setConnected(true);
        // 1. 符合Redis规范：全量同步后从节点偏移量从0开始
        // 2. RDB文件大小不计入复制偏移量
        long currentOffset = slaveNode.getNodeState().getReplicationOffset();
        slaveNode.getNodeState().setReplicationOffset(currentOffset);
        slaveNode.getNodeState().setReadyForReplCommands(true);
        log.info("从节点 {} 全量同步后偏移量设为: 0 (RDB文件大小 {} 字节不计入偏移量)", 
                slaveNode.getNodeId(), length);
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
        log.info("主节点 {} 当前偏移量: {}", node.getNodeId(), currentOffset);
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

    public Resp doPartialSync(ChannelHandlerContext ctx, String masterId, long offset) {
        if (!node.isMaster()) {
            return new Errors("不是主节点，无法执行部分重同步");
        }

        String remoteAddress = ReplicationUtils.getRemoteAddress(ctx);
        log.info("从节点 {} 请求部分重同步，offset: {}, masterId: {}", remoteAddress, offset, masterId);

        try {
            // 1. 获取复制积压缓冲区并验证部分重同步条件
            ReplBackLog replBackLog = node.getNodeState().getReplBackLog();
            if (replBackLog == null) {
                log.warn("复制积压缓冲区未初始化，执行全量同步");
                return doFullSync(ctx);
            }

            // 2. 检查是否可以进行部分重同步
            if (!replBackLog.canPartialSync(masterId, offset)) {
                return doFullSync(ctx);
            }

            // 3. 获取增量命令
            long currentOffset = node.getNodeState().getMasterReplicationOffset();
            log.info("准备发送部分重同步数据，范围: [{},{}), 大小: {} 字节",
                    offset, currentOffset, currentOffset - offset);

            byte[] commands;
            try {
                // 获取增量命令
                commands = replBackLog.getCommandSince(offset);
                log.info("成功获取部分重同步数据，大小: {} 字节", commands.length);
            } catch (Exception e) {
                log.error("获取部分重同步数据失败: {}, 回退到全量同步", e.getMessage());
                return doFullSync(ctx);
            }

            // 4. 发送增量命令
            transfer.sendPartialSyncData(ctx, commands, node.getNodeId());

            // 5. 更新从节点状态
            RedisNode slaveNode = findSlaveNodeForConnection(ctx);
            if (slaveNode != null) {
                updateSlaveNodeAfterSync(slaveNode, ctx, (int)offset);
                log.info("从节点 {} 状态更新成功，偏移量: {}", slaveNode.getNodeId(), offset);
            } else {
                log.warn("未找到对应的从节点对象，无法更新节点状态");
            }

            return null;
        } catch (Exception e) {
            log.error("部分重同步过程中发生错误，回退到全量同步: {}", e.getMessage(), e);
            return doFullSync(ctx);
        }
    }

    public void propagateCommand(byte[] command) {
        if (!node.isMaster()) {
            return;
        }

        if (command == null || command.length == 0) {
            log.warn("传播命令为空，忽略");
            return;
        }

        long prevOffset;
        long newOffset;

        // 使用同步块确保主节点偏移量和复制积压缓冲区的更新是原子的
        synchronized (this) {
            // 1. 获取当前主节点偏移量
            prevOffset = node.getNodeState().getMasterReplicationOffset();

            // 2. 计算新偏移量
            newOffset = prevOffset + command.length;

            // 3. 获取复制积压缓冲区
            ReplBackLog replBackLog = node.getNodeState().getReplBackLog();
            if (replBackLog == null) {
                log.error("复制缓冲区为null，这不该发生");
                return;
            }
            node.getNodeState().setMasterReplicationOffset(newOffset);
            log.info("[主节点] 偏移量: {} -> {} (+{}字节)", prevOffset, newOffset, command.length);

            if (replBackLog.getBaseOffset() == 0 && prevOffset > 0) {
                replBackLog.setBaseOffset(prevOffset);
                log.info("[主节点] 首次设置复制积压缓冲区基准偏移量: {}", prevOffset);
            }

            try {
                long backlogOffset = replBackLog.addCommand(command);
                log.debug("[主节点] 复制积压缓冲区添加命令成功，新的偏移量: {}", backlogOffset);
            } catch (Exception e) {
                log.error("[主节点] 添加命令到复制积压缓冲区失败: {}", e.getMessage(), e);
                return;
            }

            // 3. 传播命令到所有就绪的从节点
            int readySlaveCount = 0;
            int propagatedCount = 0;

            for (RedisNode slave : node.getSlaves()) {
                // 3.1 检查从节点是否就绪
                if (isSlaveReady(slave)) {
                    readySlaveCount++;
                    // 3.2 检查通道是否可用
                    if (slave.getChannel() != null && slave.getChannel().isActive()) {
                        try {
                            // 3.3 直接发送已编码的RESP字节数组
                            ByteBuf byteBuf = Unpooled.wrappedBuffer(command);
                            slave.getChannel().writeAndFlush(byteBuf);
                            propagatedCount++;
                            log.info("[主节点] 向从节点 {} 传播命令，偏移量: {}",
                                    slave.getNodeId(),
                                    slave.getNodeState().getReplicationOffset());
                        } catch (Exception e) {
                            log.error("[主节点] 向从节点 {} 传播命令失败: {}", slave.getNodeId(), e.getMessage());
                        }
                    } else {
                        log.warn("[主节点] 从节点 {} 通道未就绪，无法传播命令", slave.getNodeId());
                    }
                }
            }
            // 4. 记录传播结果
            if (propagatedCount > 0) {
                log.info("[主节点] 命令传播完成: {}/{} 个从节点", propagatedCount, readySlaveCount);
            } else if (readySlaveCount > 0) {
                log.warn("[主节点] 命令传播失败，未能传播到任何从节点");
            }
        }
    }



    private boolean isSlaveReady(RedisNode slave) {
        return slave.getNodeState().isReadyForReplCommands();
    }

     /**
     * 处理RDB数据接收
     */
    public boolean receiveRdbFromMaster(byte[] rdbContent) {
        if (node.isMaster()) {
            log.error("主节点不能接收RDB数据");
            return false;
        }

        try {
            // 创建临时RDB管理器
            String tempRdbFile = "temp-slave-" + node.getNodeId() + "-" + System.currentTimeMillis() + ".rdb";
            RdbManager rdbManager = new RdbManager(node.getRedisCore(), tempRdbFile);            // 接收并加载RDB数据
            boolean success = transfer.receiveAndLoadRdb(rdbContent, rdbManager);
            if (success) {
                log.debug("从节点 {} 成功加载RDB数据，偏移量将由ReplicationHandler设置", 
                        node.getNodeId());
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("从节点 {} 处理RDB数据时发生错误: {}", node.getNodeId(), e.getMessage(), e);
            return false;
        }
    }
}

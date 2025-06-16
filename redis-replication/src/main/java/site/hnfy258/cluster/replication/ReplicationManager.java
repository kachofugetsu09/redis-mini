package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.host.ReplicationHost;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.cluster.replication.utils.ReplicationTransfer;
import site.hnfy258.cluster.replication.utils.ReplicationUtils;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.rdb.RdbManager;

@Slf4j
@Setter
@Getter
public class ReplicationManager {
    private final RedisNode node;
    private final ReplicationTransfer transfer;
    private final ReplicationHost replicationHost;

    /**
     * 构造函数：使用ReplicationHost接口的依赖倒置模式
     * 
     * @param replicationHost 复制主机接口，由上层模块实现
     * @param node Redis节点实例
     */
    public ReplicationManager(final ReplicationHost replicationHost, final RedisNode node) {
        this.replicationHost = replicationHost;
        this.node = node;
        this.transfer = new ReplicationTransfer(node);
        
        log.info("ReplicationManager初始化完成，节点: {}", 
                node != null ? node.getNodeId() : "null");
    }
    public Resp doFullSync(final ChannelHandlerContext ctx) {
        if (!node.isMaster()) {
            return new Errors("不是主节点，无法执行全量同步");
        }

        final String remoteAddress = ReplicationUtils.getRemoteAddress(ctx);
        log.info("开始全量同步，来自: {}", remoteAddress);        try {
            // 1. 生成RDB数据（支持新旧两种模式）
            final byte[] rdbContent = generateRdbData();
            if (rdbContent.length == 0) {
                log.error("RDB数据生成失败，全量同步终止");
                return new Errors("RDB数据生成失败");
            }

            // 2. 获取当前主节点偏移量
            long currentMasterOffset = node.getNodeState().getMasterReplicationOffset();
            
            // 3. 发送同步数据 - 使用主节点当前偏移量
            transfer.sendFullSyncData(ctx, rdbContent, node.getNodeId(), currentMasterOffset);

            // 4. 更新从节点状态
            updateSlaveStateAfterSync(ctx, rdbContent.length);

            return null;
        }catch(Exception e){
            log.error("全量同步失败", e);
            return new Errors("全量同步失败: " + e.getMessage());
        }
    }    private void updateSlaveStateAfterSync(ChannelHandlerContext ctx, int length) {
        RedisNode slaveNode = findSlaveNodeForConnection(ctx);
        if(slaveNode != null){            
            updateSlaveNodeAfterFullSync(slaveNode, ctx, length);
        }
    }    private RedisNode findSlaveNodeForConnection(ChannelHandlerContext ctx) {
        try {
            // 1. 首先尝试通过通道精确匹配
            for (RedisNode slave : node.getSlaves()) {
                if (slave.getChannel() != null && slave.getChannel() == ctx.channel()) {
                    return slave;
                }
            }
            
            // 2. 如果没有精确匹配，返回第一个未连接的从节点
            for (RedisNode slave : node.getSlaves()) {
                if (slave.getChannel() == null || !slave.getChannel().isActive()) {
                    // 绑定新通道到此从节点
                    slave.getNodeState().setChannel(ctx.channel());
                    log.info("将新连接绑定到从节点: {}", slave.getNodeId());
                    return slave;
                }
            }
        } catch (Exception e) {
            log.error("查找从节点失败", e);
        }
        return null;
    }    public boolean receiveAndLoadRdb(byte[] rdbData) {
        if(node.isMaster()){
            log.error("主节点不应该接收RDB数据");
            return false;
        }
        
        try{
            String tempRdbFile = "temp-slave-" + System.currentTimeMillis() + ".rdb";
            RdbManager rdbManager = new RdbManager(replicationHost.getRedisCore(), tempRdbFile);

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
            transfer.sendPartialSyncData(ctx, commands, node.getNodeId());            // 5. 更新从节点状态 - 部分重同步专用方法
            RedisNode slaveNode = findSlaveNodeForConnection(ctx);
            if (slaveNode != null) {
                updateSlaveNodeAfterPartialSync(slaveNode, ctx, node.getNodeState().getMasterReplicationOffset());
                log.info("从节点 {} 部分重同步状态更新成功，偏移量: {}", slaveNode.getNodeId(), node.getNodeState().getMasterReplicationOffset());
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
            RdbManager rdbManager = new RdbManager(replicationHost.getRedisCore(), tempRdbFile);
            // 接收并加载RDB数据
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
    }    /**
     * 获取有效的RdbManager实例
     * 
     * @return RdbManager实例
     */
    private RdbManager getEffectiveRdbManager() {
        // 通过ReplicationHost接口获取RDB数据
        try {
            byte[] rdbData = replicationHost.generateRdbSnapshot();
            if (rdbData != null && rdbData.length > 0) {
                // 创建临时RdbManager用于处理RDB数据
                String tempFile = "temp-replication-" + System.currentTimeMillis() + ".rdb";
                return new RdbManager(replicationHost.getRedisCore(), tempFile);
            }
        } catch (Exception e) {
            log.error("生成RDB快照失败", e);
        }
        
        return null;
    }
      /**
     * 生成复制用的RDB数据
     * 通过ReplicationHost接口实现
     * 
     * @return RDB字节数组
     */
    private byte[] generateRdbData() {
        // 通过ReplicationHost接口生成RDB数据
        try {
            return replicationHost.generateRdbSnapshot();
        } catch (Exception e) {
            log.error("生成RDB数据失败", e);
            return new byte[0];
        }    }

    /**
     * 更新从节点状态 - 部分重同步专用
     * 部分重同步后，从节点偏移量应该设置为主节点当前偏移量
     * 
     * @param slaveNode 从节点
     * @param ctx 网络通道上下文
     * @param masterCurrentOffset 主节点当前偏移量
     */
    private void updateSlaveNodeAfterPartialSync(RedisNode slaveNode, ChannelHandlerContext ctx, long masterCurrentOffset) {
        slaveNode.getNodeState().setChannel(ctx.channel());
        slaveNode.getNodeState().setConnected(true);
        
        // 1. 部分重同步后，从节点偏移量应该与主节点当前偏移量一致
        slaveNode.getNodeState().setReplicationOffset(masterCurrentOffset);
        slaveNode.getNodeState().setReadyForReplCommands(true);
        
        log.info("从节点 {} 部分重同步完成，偏移量设为: {} (与主节点当前偏移量一致)", 
                slaveNode.getNodeId(), masterCurrentOffset);
    }

    /**
     * 更新从节点状态 - 全量同步专用
     * 全量同步后，从节点偏移量应该与主节点当前偏移量保持一致
     * 
     * @param slaveNode 从节点
     * @param ctx 网络通道上下文  
     * @param rdbLength RDB文件长度（不影响偏移量计算）
     */
    private void updateSlaveNodeAfterFullSync(RedisNode slaveNode, ChannelHandlerContext ctx, int rdbLength) {
        slaveNode.getNodeState().setChannel(ctx.channel());
        slaveNode.getNodeState().setConnected(true);
        
        // 1. 获取主节点当前偏移量
        long masterOffset = node.getNodeState().getMasterReplicationOffset();
        
        // 2. 全量同步后，从节点偏移量应该与主节点当前偏移量一致
        slaveNode.getNodeState().setReplicationOffset(masterOffset);
        slaveNode.getNodeState().setReadyForReplCommands(true);
        
        log.info("从节点 {} 全量同步完成，偏移量设为: {} (与主节点当前偏移量一致)", 
                slaveNode.getNodeId(), masterOffset);
    }
}

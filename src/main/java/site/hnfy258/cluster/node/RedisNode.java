package site.hnfy258.cluster.node;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.heartbeat.HeartbeatManager;
import site.hnfy258.cluster.replication.ReplicationHandler;
import site.hnfy258.cluster.replication.ReplicationManager;
import site.hnfy258.cluster.replication.ReplicationStateMachine;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.RedisServer;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.handler.RespDecoder;
import site.hnfy258.server.handler.RespEncoder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Getter
@Setter
@Slf4j
public class RedisNode {
    //使用的服务端
    private RedisServer redisServer;
    private RedisCore redisCore;
    private EventLoopGroup group;
    private EventExecutorGroup commandExecutor;

    private final NodeState nodeState;
    private final ReplicationStateMachine replicationStateMachine;
    private ReplicationManager replicationManager;
    private HeartbeatManager heartbeatManager;

    private boolean connected = false;

    public RedisNode(RedisServer redisServer,
                     String host,
                     int port,
                     boolean isMaster,
                     String nodeId) {
        this.redisServer = redisServer;
        this.commandExecutor = new DefaultEventExecutorGroup(1, new DefaultThreadFactory("redis-ms"));
        this.nodeState = new NodeState(nodeId,host,port,isMaster);

        if(redisServer !=null){
            this.redisCore = redisServer.getRedisCore();
        }

        this.replicationStateMachine = new ReplicationStateMachine();
        this.replicationManager = new ReplicationManager(this);
        this.heartbeatManager = new HeartbeatManager(this);
    }

    //=================辅助方法===============

    public boolean isMaster() {
        return nodeState.isMaster();
    }

    public String getHost(){
        return nodeState.getHost();
    }
    public int getPort(){
        return nodeState.getPort();
    }

    public boolean isConnected() {
        return nodeState.getConnected().get();
    }

    public void setConnected(boolean connected) {
        nodeState.getConnected().set(connected);
    }

    public Channel getChannel() {
        return nodeState.getChannel();
    }

    public void setChannel(Channel channel) {
        nodeState.setChannel(channel);
    }

    public Channel getClientChannel() {
        return nodeState.getClientChannel();
    }

    public void setClientChannel(Channel clientChannel) {
        nodeState.setClientChannel(clientChannel);
    }

    public List<RedisNode> getSlaves() {
        return nodeState.getSlaves();
    }

    public RedisNode getMasterNode() {
        return nodeState.getMasterNode();
    }

    public void setMasterNode(RedisNode masterNode) {
        nodeState.setMasterNode(masterNode);
    }

    public void addSlaveNode(RedisNode slaveNode) {
        nodeState.addSlaveNode(slaveNode);
    }

    public String getMasterHost(){
        return nodeState.getMasterHost();
    }

    public int getMasterPort() {
        return nodeState.getMasterPort();
    }

    public String getNodeId() {
        return nodeState.getNodeId();
    }

    public void addSlave(RedisNode slaveNode) {
        nodeState.addSlaveNode(slaveNode);
    }



    //=================连接方法===============

    public CompletableFuture<Void> connectInit(){
        //1.如果已经有连接，关闭现有连接
        if(isConnected() && getClientChannel() != null){
            log.info("正在关闭现有连接 {}:{}", getHost(), getPort());
            if(getClientChannel().isOpen()){
                getClientChannel().close();
            }
        }
        //2.创建新的的连接

        if(isMaster()){
            return CompletableFuture.completedFuture(null);
        }

        if(group == null){
            group = new NioEventLoopGroup();
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY,true).
                handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new RespDecoder());
                        pipeline.addLast(commandExecutor, new ReplicationHandler(RedisNode.this));
                        pipeline.addLast(new RespEncoder());
                    }
                });

        bootstrap.connect(getMasterHost(),getMasterPort()).addListener((ChannelFutureListener) f1 -> {
            if(f1.isSuccess()) {
                setClientChannel(f1.channel());
                Resp[] psyncCommand = new Resp[3];
                RespArray command = null;
                if(nodeState.hasSavedConnectionInfo()){
                    psyncCommand[0] = new BulkString("PSYNC".getBytes());
                    psyncCommand[1] = new BulkString(nodeState.getLastKnownMasterId().getBytes());
                    psyncCommand[2] = new BulkString(String.valueOf(nodeState.getLastKnownOffset()).getBytes());
                    command = new RespArray(psyncCommand);
                }
                else if(!connected){
                    psyncCommand[0] = new BulkString("PSYNC".getBytes());
                    psyncCommand[1] = new BulkString("?".getBytes());
                    psyncCommand[2] = new BulkString("-1".getBytes());
                    command = new RespArray(psyncCommand);
                }

                else{
                    psyncCommand[0] = new BulkString("PSYNC".getBytes());
                    psyncCommand[1] = new BulkString(nodeState.getMasterNode().getNodeId().getBytes());
                    psyncCommand[2] = new BulkString(String.valueOf(nodeState.getMasterNode().getNodeState().getReplicationOffset()).getBytes());
                    command = new RespArray(psyncCommand);
                }
                connected = true;
                log.info("成功连接到主节点 {}:{}", getMasterHost(), getMasterPort());
                getClientChannel().writeAndFlush(command).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        log.info("向主节点 {}:{} 发送 PSYNC 命令成功", getMasterHost(), getMasterPort());

                        startHeartbeatManager();
                    } else {
                        log.error("向主节点 {}:{} 发送 PSYNC 命令失败", getMasterHost(), getMasterPort(), f.cause());
                        group.shutdownGracefully();
                        future.completeExceptionally(f.cause());
                    }
                });
                future.complete(null);
            } else {
                log.error("连接到主节点 {}:{} 失败", getMasterHost(), getMasterPort(), f1.cause());
                group.shutdownGracefully();
                future.completeExceptionally(f1.cause());
            }
        });

        return future.thenApply(v ->{
            setConnected(true);
            log.info("节点 {}:{}", getHost(), getPort());
            return null;
        });
    }

    public void startHeartbeatManager() {
        if(!isMaster() && heartbeatManager != null){
            HeartbeatManager.HeartbeatCallback callback = new HeartbeatManager.HeartbeatCallback() {
                @Override
                public void onHeartbeatFailed() {
                    log.debug("心跳失败，尝试重新连接主节点 {}:{}", getMasterHost(), getMasterPort());
                }

                @Override
                public void onConnectionLost() {
                    log.warn("与主节点 {}:{} 连接丢失，尝试重新连接", getMasterHost(), getMasterPort());
                    handleConnectionLost();
                }
            };
            heartbeatManager.startHeartbeat(callback);
            log.info("心跳管理器已启动，节点 {}:{}", getHost(), getPort());
        }
    }

    public void stopHeartbeatManager(){
        if(heartbeatManager != null){
            heartbeatManager.stopHeartbeat();
            log.info("心跳管理器已停止，节点 {}:{}", getHost(), getPort());
        }
    }

    private void handleConnectionLost(){
        try{
            if(nodeState.getMasterNode() != null){
                nodeState.saveConnectionInfo(nodeState.getMasterNode().getNodeId(),
                        nodeState.getReplicationOffset());
                log.info("已保存与主节点 {}:{} 的连接信息", getMasterHost(), getMasterPort());
            }
            stopHeartbeatManager();

            if(getClientChannel() != null && getClientChannel().isActive()){
                getClientChannel().close();
            }
            log.info("与主节点 {}:{} 的连接已关闭", getMasterHost(), getMasterPort());
        }catch(Exception e){
            log.error("处理连接丢失时发生异常: {}", e.getMessage(), e);
        }
    }

    public void resetHeartbeatFailedCount(){
        if(heartbeatManager != null) {
            heartbeatManager.resetFailedCount();
            log.debug("已重置心跳失败计数，节点 {}:{}", getHost(), getPort());
        } else {
            log.warn("无法重置心跳失败计数，心跳管理器未初始化");
        }
    }

    public void pauseHeartbeat(){
        if(heartbeatManager != null) {
            heartbeatManager.pasue();
            log.debug("心跳已暂停，节点 {}:{}", getHost(), getPort());
        } else {
            log.warn("无法暂停心跳，心跳管理器未初始化");
        }
    }

    public void resumeHeartbeat() {
        if(heartbeatManager != null) {
            heartbeatManager.resume();
            log.debug("心跳已恢复，节点 {}:{}", getHost(), getPort());
        } else {
            log.warn("无法恢复心跳，心跳管理器未初始化");
        }
    }

    public void cleanup(){
        if(heartbeatManager != null) {
            heartbeatManager.shutdown();
            heartbeatManager = null;
        }
        if(group !=null){
            group.shutdownGracefully();
            group = null;
        }
        if(commandExecutor != null) {
            commandExecutor.shutdownGracefully();
            commandExecutor = null;
        }
        nodeState.cleanup();
    }
    // =========================复制相关===================================

    public boolean receiveRdbFromMaster(byte[] rdbData) {
        return replicationManager.receiveAndLoadRdb(rdbData);
    }

    public Resp doFullSync(ChannelHandlerContext ctx) {
        return replicationManager.doFullSync(ctx);
    }

    public Resp doPartialSync(ChannelHandlerContext ctx, String masterId, long offset) {
        return replicationManager.doPartialSync(ctx, masterId, offset);
    }

    public void propagateCommand(byte[] commandBytes) {
        replicationManager.propagateCommand(commandBytes);

    }
}

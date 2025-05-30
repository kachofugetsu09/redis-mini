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
                psyncCommand[0] = new BulkString("PSYNC".getBytes());
                psyncCommand[1] = new BulkString("?".getBytes());
                psyncCommand[2] = new BulkString("-1".getBytes());
                RespArray command = new RespArray(psyncCommand);
                log.info("成功连接到主节点 {}:{}", getMasterHost(), getMasterPort());
                getClientChannel().writeAndFlush(command).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        log.info("向主节点 {}:{} 发送 PSYNC 命令成功", getMasterHost(), getMasterPort());
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

    public void cleanup(){
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

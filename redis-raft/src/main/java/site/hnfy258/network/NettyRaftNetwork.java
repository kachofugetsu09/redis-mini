package site.hnfy258.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import site.hnfy258.core.LogEntry;
import site.hnfy258.raft.Raft;
import site.hnfy258.rpc.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于Netty的Raft网络实现
 * 负责节点间的网络通信
 */
public class NettyRaftNetwork implements RaftNetwork {
    
    private final String host;
    private final int port;
    private final ConcurrentHashMap<Integer, String> peerAddresses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Channel> peerChannels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGenerator = new AtomicLong(0);
    private final AtomicLong rpcSentCount = new AtomicLong(0);
    private final AtomicLong rpcReceivedCount = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private final AtomicLong bytesReceived = new AtomicLong(0);
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventLoopGroup clientGroup;
    private Channel serverChannel;
    private Raft raftNode;
    private boolean started = false;
    
    public NettyRaftNetwork(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    /**
     * 添加对等节点地址
     */
    public void addPeer(int serverId, String host, int port) {
        peerAddresses.put(serverId, host + ":" + port);
    }
    
    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(int targetServerId, RequestVoteArg request) {
        if (!started) {
            CompletableFuture<RequestVoteReply> future = new CompletableFuture<>();
            future.complete(new RequestVoteReply()); // 返回默认失败回复而不是null
            return future;
        }
        
        return sendRequest(targetServerId, RaftMessage.Type.REQUEST_VOTE, request)
                .thenApply(response -> (RequestVoteReply) response)
                .exceptionally(throwable -> {
                    // 网络异常时返回默认回复
                    System.err.println("Failed to send RequestVote to " + targetServerId + ": " + throwable.getMessage());
                    RequestVoteReply reply = new RequestVoteReply();
                    reply.term = 0;
                    reply.voteGranted = false;
                    return reply;
                });
    }
    
    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(int targetServerId, AppendEntriesArgs request) {
        if (!started) {
            CompletableFuture<AppendEntriesReply> future = new CompletableFuture<>();
            future.complete(new AppendEntriesReply()); // 返回默认失败回复而不是null
            return future;
        }
        
        return sendRequest(targetServerId, RaftMessage.Type.APPEND_ENTRIES, request)
                .thenApply(response -> (AppendEntriesReply) response)
                .exceptionally(throwable -> {
                    // 网络异常时返回默认回复
                    System.err.println("Failed to send AppendEntries to " + targetServerId + ": " + throwable.getMessage());
                    AppendEntriesReply reply = new AppendEntriesReply();
                    reply.term = 0;
                    reply.success = false;
                    return reply;
                });
    }


    private CompletableFuture<Object> sendRequest(int targetServerId,
                                                  RaftMessage.Type type,
                                                  Object payload) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        
        // 异步获取连接并发送请求
        getOrCreateConnection(targetServerId).thenAccept(channel -> {
            if (channel != null && channel.isActive()) {
                long requestId = requestIdGenerator.incrementAndGet();
                RaftMessage message = new RaftMessage(type, payload, requestId);
                
                // 注册待处理的请求
                pendingRequests.put(requestId, future);
                
                // 发送请求
                channel.writeAndFlush(message).addListener(channelFuture -> {
                    if (!channelFuture.isSuccess()) {
                        pendingRequests.remove(requestId);
                        future.completeExceptionally(channelFuture.cause());
                    } else {
                        rpcSentCount.incrementAndGet();
                        // 估算发送的字节数（简化实现）
                        bytesSent.addAndGet(estimateMessageSize(message));
                    }
                });
                
                // 设置超时 - 真实网络环境需要更长的超时时间
                channel.eventLoop().schedule(() -> {
                    CompletableFuture<Object> timeoutFuture = pendingRequests.remove(requestId);
                    if (timeoutFuture != null) {
                        timeoutFuture.completeExceptionally(new RuntimeException("Request timeout"));
                    }
                }, 2000, java.util.concurrent.TimeUnit.MILLISECONDS); // 2秒超时，适应网络延迟
            } else {
                future.completeExceptionally(new RuntimeException("Connection not available"));
            }
        });
        
        return future;
    }
    
    private CompletableFuture<Channel> getOrCreateConnection(int targetServerId) {
        Channel existingChannel = peerChannels.get(targetServerId);
        if (existingChannel != null && existingChannel.isActive()) {
            return CompletableFuture.completedFuture(existingChannel);
        }
        
        String address = peerAddresses.get(targetServerId);
        if (address == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String[] parts = address.split(":");
        String targetHost = parts[0];
        int targetPort = Integer.parseInt(parts[1]);
        
        CompletableFuture<Channel> future = new CompletableFuture<>();
        
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                new RaftClientHandler(NettyRaftNetwork.this)
                        );
                    }
                });
        
        bootstrap.connect(targetHost, targetPort).addListener((ChannelFuture connectFuture) -> {
            if (connectFuture.isSuccess()) {
                Channel channel = connectFuture.channel();
                peerChannels.put(targetServerId, channel);
                future.complete(channel);
                
                // 处理连接关闭
                channel.closeFuture().addListener(closeFuture -> {
                    peerChannels.remove(targetServerId);
                });
            } else {
                future.completeExceptionally(connectFuture.cause());
            }
        });
        
        return future;
    }
    
    @Override
    public void start(int serverId, Raft raft) {
        this.raftNode = raft;
        this.started = true;
        
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        clientGroup = new NioEventLoopGroup();
        
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new RaftServerHandler(NettyRaftNetwork.this, raftNode)
                            );
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            
            ChannelFuture future = serverBootstrap.bind(host, port).sync();
            serverChannel = future.channel();
            
            System.out.println("Raft server started on " + host + ":" + port);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
        }
    }
    
    @Override
    public void stop() {
        started = false;
        
        // 关闭所有客户端连接
        peerChannels.values().forEach(channel -> {
            if (channel.isActive()) {
                channel.close();
            }
        });
        peerChannels.clear();
        
        // 关闭服务器
        if (serverChannel != null) {
            serverChannel.close();
        }
        
        // 关闭事件循环组
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (clientGroup != null) {
            clientGroup.shutdownGracefully();
        }
        
        // 完成所有待处理的请求
        pendingRequests.values().forEach(future -> {
            future.completeExceptionally(new RuntimeException("Network stopped"));
        });
        pendingRequests.clear();
    }
    
    // 处理接收到的响应
    void handleResponse(long requestId, Object response) {
        rpcReceivedCount.incrementAndGet();
        CompletableFuture<Object> future = pendingRequests.remove(requestId);
        if (future != null) {
            future.complete(response);
        }
    }
    
    // ======================== 统计方法 ========================
    
    /**
     * 获取发送的RPC数量
     */
    public long getRpcSentCount() {
        return rpcSentCount.get();
    }
    
    /**
     * 获取接收的RPC数量
     */
    public long getRpcReceivedCount() {
        return rpcReceivedCount.get();
    }
    
    /**
     * 获取发送的字节数
     */
    public long getBytesSent() {
        return bytesSent.get();
    }
    
    /**
     * 获取接收的字节数
     */
    public long getBytesReceived() {
        return bytesReceived.get();
    }
    
    /**
     * 重置统计计数器
     */
    public void resetStats() {
        rpcSentCount.set(0);
        rpcReceivedCount.set(0);
        bytesSent.set(0);
        bytesReceived.set(0);
    }
    
    /**
     * 估算消息大小
     */
    private long estimateMessageSize(RaftMessage message) {
        long size = 64; // 基础消息头大小
        
        if (message.getPayload() instanceof AppendEntriesArgs) {
            AppendEntriesArgs args = (AppendEntriesArgs) message.getPayload();
            size += 32; // AppendEntriesArgs基础大小
            if (args.entries != null) {
                for (LogEntry entry : args.entries) {
                    size += 32; // LogEntry基础大小
                    if (entry.getCommand() != null) {
                        size += (entry.getCommand()).getContent().length;
                    }
                }
            }
        } else if (message.getPayload() instanceof RequestVoteArg) {
            size += 32;
        }
        
        return size;
    }


}

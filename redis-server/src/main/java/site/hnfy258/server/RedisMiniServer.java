package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.host.ReplicationHost;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.handler.RespDecoder;
import site.hnfy258.protocal.handler.RespEncoder;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.handler.RespCommandHandler;
import io.netty.channel.ChannelOption;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.context.RedisContextImpl;
import site.hnfy258.server.config.RedisServerConfig;

import java.util.UUID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Getter
@Setter
public class RedisMiniServer implements RedisServer, ReplicationHost {
    
    /** Redis服务器配置 */
    private final RedisServerConfig config;

    private String host;
    private int port;    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup commandExecutor;    
    private Channel serverChannel;

    private AofManager aofManager;
    private RdbManager rdbManager;

    private RedisCore redisCore;
    private RedisNode redisNode;
    private RedisContext redisContext;
    
    // ReplicationHost 接口相关字段
    private String replicationId;
    private volatile long replicationOffset;
    public RedisMiniServer(String host, int port) throws Exception {
        this(RedisServerConfig.builder()
                .host(host)
                .port(port)
                .build());
    }

    public RedisMiniServer(String host, int port, String rdbFileName) throws Exception {
        this(RedisServerConfig.builder()
                .host(host)
                .port(port)
                .rdbFileName(rdbFileName)
                .build());
    }

    public RedisMiniServer(String host, int port, String rdbFileName, String aofFileName) throws Exception {
        this(RedisServerConfig.builder()
                .host(host)
                .port(port)
                .rdbFileName(rdbFileName)
                .aofFileName(aofFileName)
                .aofEnabled(true)  // 如果指定了AOF文件名，默认启用AOF
                .build());
    }

    /**
     * 主构造函数，使用配置对象
     * 
     * @param config Redis服务器配置
     * @throws Exception 初始化异常
     */
    public RedisMiniServer(RedisServerConfig config) throws Exception {
        // 1. 验证并保存配置
        config.validate();
        this.config = config;
        
        this.host = config.getHost();
        this.port = config.getPort();        
        // 2. 根据操作系统选择最优的EventLoopGroup实现
        initializeEventLoopGroups();
        
        // 3. 初始化严格单线程命令执行器
        initializeCommandExecutor();
        
        // 4. 初始化Redis核心组件
        this.redisCore = new RedisCoreImpl(config.getDatabaseCount());
        
        // 5. 初始化RedisContext
        this.redisContext = new RedisContextImpl(redisCore, null, null, host, port);
        
        // 6. 初始化持久化组件
        if (config.isAofEnabled()) {
            this.aofManager = new AofManager(config.getAofFileName(), redisContext.getRedisCore());
            aofManager.load();
            Thread.sleep(500);
        }
        
        if (config.isRdbEnabled()) {
            this.rdbManager = new RdbManager(redisCore, config.getRdbFileName());
            boolean success = rdbManager.loadRdb();
            if (!success) {
                log.warn("RDB文件加载失败，可能是文件不存在或格式错误");
            }
            Thread.sleep(500);
        }
        
        // 7. 更新RedisContext中的持久化组件
        this.redisContext = new RedisContextImpl(redisCore, aofManager, rdbManager, host, port);
        
        // 7. 初始化复制相关字段
        this.replicationId = generateReplicationId();
        this.replicationOffset = 0L;

    }

    @Override
    public void start() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, 32 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 32 * 1024)
                .childHandler(new ChannelInitializer<SocketChannel>() {                    
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new RespDecoder());
                        pipeline.addLast(new RespEncoder());
                        // 为每个连接创建独立的命令处理器
                        pipeline.addLast(commandExecutor, new RespCommandHandler(redisContext));
                    }
                });
        try {
            serverChannel = serverBootstrap.bind(host, port).sync().channel();
            log.info("Redis server started at {}:{}", host, port);
        } catch (InterruptedException e) {
            log.error("Redis server start error", e);
            stop();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        try {
            if(serverChannel != null){
                serverChannel.close().sync();
            }
            if(workerGroup != null){
                workerGroup.shutdownGracefully().sync();
            }
            if(bossGroup != null){
                bossGroup.shutdownGracefully().sync();
            }
            if(commandExecutor != null) {
                commandExecutor.shutdown();
            }
            if(aofManager != null){
                aofManager.close();
            }
            if(rdbManager != null){
                rdbManager.close();
            }
        }catch(InterruptedException e){
            log.error("Redis server stop error", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RdbManager getRdbManager() {
        if (rdbManager == null) {
            throw new IllegalStateException("RDB manager is not initialized");
        }
        return rdbManager;
    }

    @Override
    public AofManager getAofManager() {
        return aofManager;
    }    /**
     * 设置 Redis 节点，并将其注入到命令处理器中以支持命令传播
     * @param redisNode Redis 节点实例
     */
     @Override
    public void setRedisNode(RedisNode redisNode) {
        this.redisNode = redisNode;
        if (this.redisNode != null) {
            // 设置 ReplicationHost 而不是 RedisServer
            this.redisNode.setReplicationHost(this);
            if (this.redisContext != null) {
                this.redisContext.setRedisNode(redisNode);
            }
        }
    }

    /**
     * 根据操作系统选择最优的EventLoopGroup实现
     * Linux: 使用Epoll
     * macOS: 使用KQueue  
     * Windows: 使用NIO
     */
    private void initializeEventLoopGroups() {
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        final String osName = System.getProperty("os.name").toLowerCase();
        
        // 1. 优先使用Linux的Epoll，性能最佳
        if (Epoll.isAvailable()) {
            log.info("使用Epoll EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new EpollEventLoopGroup(1, 
                new DefaultThreadFactory("epoll-boss"));
            this.workerGroup = new EpollEventLoopGroup(cpuCount * 2, 
                new DefaultThreadFactory("epoll-worker"));
        }
        // 2. 其次使用macOS的KQueue
        else if (KQueue.isAvailable()) {
            log.info("使用KQueue EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new KQueueEventLoopGroup(1, 
                new DefaultThreadFactory("kqueue-boss"));
            this.workerGroup = new KQueueEventLoopGroup(cpuCount * 2, 
                new DefaultThreadFactory("kqueue-worker"));
        }
        // 3. 最后使用跨平台的NIO (Windows等)
        else {
            log.info("使用NIO EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new NioEventLoopGroup(1, 
                new DefaultThreadFactory("nio-boss"));
            this.workerGroup = new NioEventLoopGroup(cpuCount * 2, 
                new DefaultThreadFactory("nio-worker"));
        }
    }


    private void initializeCommandExecutor() {
        final String threadNamePrefix = "redis-cmd-single";
        log.info("使用单线程CommandExecutor，确保命令串行执行");
        
        this.commandExecutor = new DefaultEventExecutorGroup(1,
            new DefaultThreadFactory(threadNamePrefix));
    }    @Override
    public RedisContext getRedisContext() {
        return redisContext;
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }

    // ===== ReplicationHost 接口实现 =====

    @Override
    public String getReplicationId() {
        return replicationId;
    }

    @Override
    public long getReplicationOffset() {
        return replicationOffset;
    }

    @Override
    public void setReplicationOffset(final long offset) {
        this.replicationOffset = offset;
    }    @Override
    public byte[] generateRdbSnapshot() throws Exception {
        if (rdbManager != null) {
            return rdbManager.generateSnapshot();
        }
        throw new UnsupportedOperationException("RDB manager not initialized");
    }    @Override
    public Resp executeCommand(final Resp command) {
        if (command instanceof RespArray) {
            // 创建一个临时的命令处理器来执行复制命令
            RespCommandHandler tempHandler = new RespCommandHandler(redisContext);
            return tempHandler.executeCommand((RespArray) command);
        }
        return null;
    }

    /**
     * 生成复制ID
     * 
     * @return 新的复制ID
     */
    private String generateReplicationId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}

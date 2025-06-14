package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.core.RedisCoreImpl;
import site.hnfy258.server.handler.RespCommandHandler;
import site.hnfy258.server.handler.RespDecoder;
import site.hnfy258.server.handler.RespEncoder;
import io.netty.channel.ChannelOption;


@Slf4j
@Getter
@Setter
public class RedisMiniServer implements RedisServer{
    private static final int DEFAULT_DBCOUNT = 16;


    private String host;
    private int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup commandExecutor;
    private Channel serverChannel;

    public RespCommandHandler commandHandler;
    private static final boolean ENABLE_AOF = true;
    private AofManager aofManager;
    private static final boolean ENABLE_RDB = false;
    private RdbManager rdbManager;
    private String rdbFileName;

    private RedisCore redisCore;

    private RedisNode redisNode;

    public RedisMiniServer(String host, int port) throws Exception {
        this(host, port, "dump.rdb", "redis.aof");
    }

    public RedisMiniServer(String host, int port, String rdbFileName) throws Exception {
        this(host, port, rdbFileName, "redis.aof");
    }

    public RedisMiniServer(String host, int port, String rdbFileName, String aofFileName) throws Exception {
        this.host = host;
        this.port = port;
        // 1. 根据操作系统选择最优的EventLoopGroup实现
        initializeEventLoopGroups();
        // 2. 根据操作系统选择最优的CommandExecutor实现
        initializeCommandExecutor();
        this.redisCore = new RedisCoreImpl(DEFAULT_DBCOUNT,this);
        this.aofManager = null;
        this.rdbManager = null;
        if(ENABLE_AOF){
            this.aofManager = new AofManager(aofFileName, redisCore);
            aofManager.load();
            Thread.sleep(500);
        }
        if(ENABLE_RDB){
            this.rdbManager = new RdbManager(redisCore,rdbFileName);
            boolean success = rdbManager.loadRdb();
            if(!success){
                log.warn("RDB文件加载失败，可能是文件不存在或格式错误");
            }
            Thread.sleep(500);
        }
        this.commandHandler = new RespCommandHandler(redisCore, aofManager);
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
                        pipeline.addLast(commandExecutor, commandHandler);
                        pipeline.addLast(new RespEncoder());
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
    }

    /**
     * 设置 Redis 节点，并将其注入到命令处理器中以支持命令传播
     * @param redisNode Redis 节点实例
     */
     @Override
    public void setRedisNode(RedisNode redisNode) {
        this.redisNode = redisNode;
        if (this.redisNode != null) {
            this.redisNode.setRedisServer(this);
            if (this.commandHandler != null) {
                this.commandHandler.setRedisNode(this.redisNode);
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
    }
}

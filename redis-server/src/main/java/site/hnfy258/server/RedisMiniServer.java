package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
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
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.handler.RespDecoder;
import site.hnfy258.protocal.handler.RespEncoder;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.handler.RespCommandHandler;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.context.RedisContextImpl;
import site.hnfy258.server.config.RedisServerConfig;

import java.util.UUID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis服务器的轻量级实现。
 * 
 * <p>该类实现了{@link RedisServer}和{@link ReplicationHost}接口，提供了一个完整的Redis服务器实现，
 * 包括：
 * <ul>
 *   <li>基于Netty的高性能网络框架
 *   <li>支持多种操作系统的优化（Epoll/KQueue/NIO）
 *   <li>完整的命令处理系统
 *   <li>持久化支持（AOF/RDB）
 *   <li>主从复制功能
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用独立的命令执行线程池
 *   <li>支持操作系统特定的网络优化
 *   <li>可配置的网络参数（缓冲区大小等）
 * </ul>
 * 
 * <p>可靠性保证：
 * <ul>
 *   <li>优雅的启动和关闭流程
 *   <li>完整的异常处理
 *   <li>资源的合理释放
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
@Getter
@Setter
public class RedisMiniServer implements RedisServer, ReplicationHost {
    
    /** Redis服务器配置 */
    private final RedisServerConfig config;

    /** 服务器Channel类型，根据操作系统自动选择最优实现 */
    private Class<? extends ServerChannel> serverChannelClass;
    
    /** 接收连接的事件循环组 */
    private EventLoopGroup bossGroup;
    
    /** 处理I/O的事件循环组 */
    private EventLoopGroup workerGroup;
    
    /** 命令执行线程池 */
    private EventExecutorGroup commandExecutor;
    
    /** 服务器Channel */
    private Channel serverChannel;

    /** Redis核心功能实现 */
    private RedisCore redisCore;
    
    /** Redis服务器上下文 */
    private RedisContext redisContext;
    
    /** Redis节点信息（用于复制功能） */
    private RedisNode redisNode;
    
    /** 复制功能的唯一标识符 */
    private String replicationId;
    
    /** 复制偏移量（用于跟踪复制进度） */
    private volatile long replicationOffset;

    /**
     * 主构造函数，使用配置对象初始化服务器。
     * 
     * <p>初始化过程包括：
     * <ul>
     *   <li>创建并配置事件循环组
     *   <li>初始化命令执行器
     *   <li>创建Redis核心实例
     *   <li>设置服务器上下文
     *   <li>初始化复制相关字段
     * </ul>
     * 
     * @param config Redis服务器配置
     * @throws Exception 如果初始化过程中发生错误
     */
    public RedisMiniServer(RedisServerConfig config) throws Exception {
        this.config = config;
        
        // 1. 初始化事件循环组和命令执行器
        initializeEventLoopGroups();
        initializeCommandExecutor();
        
        // 2. 初始化Redis核心
        this.redisCore = new RedisCoreImpl(config.getDatabaseCount());
        
        // 3. 创建RedisContext（持久化组件的创建被移到RedisContext内部）
        this.redisContext = new RedisContextImpl(
            redisCore,
            config.getHost(),
            config.getPort(),
            config
        );
        
        // 4. 初始化复制相关字段
        this.replicationId = generateReplicationId();
        this.replicationOffset = 0L;
    }

    /**
     * 生成用于复制功能的唯一标识符。
     * 
     * @return UUID格式的复制ID
     */
    private String generateReplicationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 启动Redis服务器。
     * 
     * <p>该方法完成以下任务：
     * <ul>
     *   <li>配置并启动Netty服务器
     *   <li>设置网络参数（TCP参数、缓冲区等）
     *   <li>初始化编解码器和命令处理器
     *   <li>绑定服务器端口
     * </ul>
     */
    @Override
    public void start() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(serverChannelClass)
                .option(ChannelOption.SO_BACKLOG, config.getBacklogSize())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, config.getReceiveBufferSize())
                .childOption(ChannelOption.SO_SNDBUF, config.getSendBufferSize())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new RespDecoder());
                        pipeline.addLast(new RespEncoder());
                        pipeline.addLast(commandExecutor, new RespCommandHandler(redisContext));
                    }
                });
        try {
            serverChannel = serverBootstrap.bind(config.getHost(), config.getPort()).sync().channel();
            log.info("Redis server started at {}:{}", config.getHost(), config.getPort());
        } catch (InterruptedException e) {
            log.error("Redis server start error", e);
            stop();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 优雅停止Redis服务器。
     * 
     * <p>按以下顺序关闭各个组件：
     * <ul>
     *   <li>关闭服务器Channel
     *   <li>关闭worker线程组
     *   <li>关闭boss线程组
     *   <li>关闭命令执行器
     *   <li>关闭Redis上下文
     * </ul>
     * 
     * <p>确保所有资源都被正确释放，包括：
     * <ul>
     *   <li>网络连接
     *   <li>线程池资源
     *   <li>文件句柄
     *   <li>其他系统资源
     * </ul>
     */
    @Override
    public void stop() {
        try {
            if(serverChannel != null) {
                serverChannel.close().sync();
            }
            if(workerGroup != null) {
                workerGroup.shutdownGracefully().sync();
            }
            if(bossGroup != null) {
                bossGroup.shutdownGracefully().sync();
            }
            if(commandExecutor != null) {
                commandExecutor.shutdown();
            }
            if(redisContext != null) {
                redisContext.shutdown();
            }
        } catch(InterruptedException e) {
            log.error("Redis server stop error", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Redis server stop error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public RdbManager getRdbManager() {
        return redisContext.getRdbManager();
    }

    @Override
    public AofManager getAofManager() {
        return redisContext.getAofManager();
    }

    /**
     * 设置Redis节点信息，用于主从复制功能。
     * 
     * <p>当设置新的节点信息时：
     * <ul>
     *   <li>更新节点引用
     *   <li>设置复制主机
     *   <li>更新上下文中的节点信息
     * </ul>
     * 
     * @param redisNode 新的节点信息，如果为null表示取消主从关系
     */
    @Override
    public void setRedisNode(RedisNode redisNode) {
        this.redisNode = redisNode;
        if (this.redisNode != null) {
            this.redisNode.setReplicationHost(this);
            if (this.redisContext != null) {
                this.redisContext.setRedisNode(redisNode);
            }
        }
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }

    @Override
    public RedisNode getRedisNode() {
        return redisNode;
    }

    @Override
    public RedisContext getRedisContext() {
        return redisContext;
    }

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
    }

    /**
     * 生成RDB快照，用于主从复制。
     * 
     * <p>该方法会：
     * <ul>
     *   <li>检查RDB管理器是否可用
     *   <li>触发快照生成
     *   <li>返回二进制格式的快照数据
     * </ul>
     * 
     * @return RDB格式的快照数据
     * @throws Exception 如果RDB管理器未初始化或生成快照失败
     */
    @Override
    public byte[] generateRdbSnapshot() throws Exception {
        RdbManager rdbManager = redisContext.getRdbManager();
        if (rdbManager != null) {
            return rdbManager.generateSnapshot();
        }
        throw new UnsupportedOperationException("RDB manager not initialized");
    }

    /**
     * 执行Redis命令。
     * 
     * <p>命令执行流程：
     * <ul>
     *   <li>解析命令数组
     *   <li>验证命令格式
     *   <li>查找命令类型
     *   <li>创建命令实例
     *   <li>执行命令并返回结果
     * </ul>
     * 
     * @param command RESP格式的命令
     * @return 命令执行结果
     */
    @Override
    public Resp executeCommand(Resp command) {
        if (command instanceof RespArray) {
            try {
                RespArray cmdArray = (RespArray) command;
                Resp[] array = cmdArray.getContent();
                if (array == null || array.length == 0) {
                    return new Errors("ERR empty command");
                }
                
                // 获取命令类型
                RedisBytes cmdBytes = ((BulkString) array[0]).getContent();
                CommandType commandType = CommandType.findByBytes(cmdBytes);
                if (commandType == null) {
                    return new Errors("ERR unknown command");
                }
                
                // 创建并执行命令
                Command redisCommand = commandType.createCommand(redisContext);
                redisCommand.setContext(array);
                return redisCommand.handle();
            } catch (Exception e) {
                log.error("执行复制命令失败", e);
                return new Errors("ERR " + e.getMessage());
            }
        }
        return new Errors("ERR Invalid command format");
    }

    private void initializeEventLoopGroups() {
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        final String osName = System.getProperty("os.name").toLowerCase();

        if (Epoll.isAvailable()) {
            log.info("使用Epoll EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new EpollEventLoopGroup(config.getBossThreadCount(),
                    new DefaultThreadFactory("epoll-boss"));
            this.workerGroup = new EpollEventLoopGroup(config.getWorkerThreadCount(),
                    new DefaultThreadFactory("epoll-worker"));
            this.serverChannelClass = EpollServerSocketChannel.class;
        }
        else if (KQueue.isAvailable()) {
            log.info("使用KQueue EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new KQueueEventLoopGroup(config.getBossThreadCount(),
                    new DefaultThreadFactory("kqueue-boss"));
            this.workerGroup = new KQueueEventLoopGroup(config.getWorkerThreadCount(),
                    new DefaultThreadFactory("kqueue-worker"));
            this.serverChannelClass = KQueueServerSocketChannel.class;
        }
        else {
            log.info("使用NIO EventLoopGroup (操作系统: {})", osName);
            this.bossGroup = new NioEventLoopGroup(config.getBossThreadCount(),
                    new DefaultThreadFactory("nio-boss"));
            this.workerGroup = new NioEventLoopGroup(config.getWorkerThreadCount(),
                    new DefaultThreadFactory("nio-worker"));
            this.serverChannelClass = NioServerSocketChannel.class;
        }
    }

    private void initializeCommandExecutor() {
        final String threadNamePrefix = "redis-cmd-single";
        log.info("使用单线程CommandExecutor，确保命令串行执行");
        
        this.commandExecutor = new DefaultEventExecutorGroup(
            config.getCommandExecutorThreadCount(),
            new DefaultThreadFactory(threadNamePrefix)
        );
    }
}

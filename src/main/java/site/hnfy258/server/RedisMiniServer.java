package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.core.RedisCoreImpl;
import site.hnfy258.server.handler.RespCommandHandler;
import site.hnfy258.server.handler.RespDecoder;
import site.hnfy258.server.handler.RespEncoder;

import java.io.FileNotFoundException;


@Slf4j
public class RedisMiniServer implements RedisServer{
    private static final int DEFAULT_DBCOUNT = 16;


    private String host;
    private int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup commandExecutor;
    private Channel serverChannel;

    public RespCommandHandler commandHandler;
    private static final boolean ENABLE_AOF = false;
    private AofManager aofManager;
    private static final boolean ENABLE_RDB = true;
    private RdbManager rdbManager;

    private RedisCore redisCore;
    public RedisMiniServer(String host, int port) throws Exception {
        this.host = host;
        this.port = port;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
        this.commandExecutor = new DefaultEventExecutorGroup(1, new DefaultThreadFactory("redis-cmd"));
        this.redisCore = new RedisCoreImpl(DEFAULT_DBCOUNT,this);
        this.aofManager = null;
        this.rdbManager = null;
        if(ENABLE_AOF){
            this.aofManager = new AofManager("redis.aof",redisCore);
            aofManager.load();
            Thread.sleep(500);
        }
        if(ENABLE_RDB){
            this.rdbManager = new RdbManager(redisCore);
            boolean success = rdbManager.loadRdb();
            if(!success){
                log.warn("RDB文件加载失败，可能是文件不存在或格式错误");
            }
            Thread.sleep(500);
        }
        this.commandHandler = new RespCommandHandler(redisCore,  aofManager);
    }


    @Override
    public void start() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new RespDecoder());
                        pipeline.addLast(commandExecutor, commandHandler);
                        pipeline.addLast(new RespEncoder());
                    }
                });
        try{
            serverChannel = serverBootstrap.bind(host, port).sync().channel();
            log.info("Redis server started at {}:{}", host, port);
        }
        catch (InterruptedException e){
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
}

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
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.core.RedisCoreImpl;
import site.hnfy258.server.handler.RespCommandHandler;
import site.hnfy258.server.handler.RespDecoder;
import site.hnfy258.server.handler.RespEncoder;
import site.hnfy258.server.handler.StringHandler;
@Slf4j
public class RedisMiniServer implements RedisServer{
    private static final int DEFAULT_DBCOUNT = 16;


    private String host;
    private int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private RedisCore redisCore;


    public RedisMiniServer(String host, int port) {
        this.host = host;
        this.port = port;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(4);

        this.redisCore = new RedisCoreImpl(DEFAULT_DBCOUNT);
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
                        pipeline.addLast(new RespCommandHandler(redisCore));
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
        }catch(InterruptedException e){
            log.error("Redis server stop error", e);
            Thread.currentThread().interrupt();
        }


    }
}

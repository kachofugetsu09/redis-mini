package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.command.impl.cluster.Psync;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

@Slf4j
@Getter
public class RespCommandHandler extends SimpleChannelInboundHandler<Resp> {
    private AofManager aofManager;
    private final RedisCore redisCore;
    private RedisNode redisNode;
    private boolean isMaster = false;

    // 重用的ByteBuf，用于命令传播
    private static final ThreadLocal<ByteBuf> propagationBuf = ThreadLocal.withInitial(() ->
        Unpooled.buffer(4096)
    );

    public RespCommandHandler(RedisCore redisCore, AofManager aofManager, boolean isMaster) {
        this.redisCore = redisCore;
        this.aofManager = aofManager;
        this.isMaster = isMaster;
    }

    public RespCommandHandler(RedisCore redisCore, AofManager aofManager) {
        this.redisCore = redisCore;
        this.aofManager = aofManager;
    }

    public RespCommandHandler(RedisCore redisCore, AofManager aofManager, RedisNode redisNode) {
        this.redisCore = redisCore;
        this.aofManager = aofManager;
        this.redisNode = redisNode;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) throws Exception {
        if(msg instanceof RespArray){
            RespArray respArray = (RespArray) msg;
            Resp response = processCommand(respArray,ctx);

            if(response!=null){
                ctx.writeAndFlush(response);
            }
        }else{
            ctx.writeAndFlush(new Errors("不支持的命令"));
        }
    }

    /**
     * 确保数据被及时刷新到客户端
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
        super.channelReadComplete(ctx);
    }

    /**
     * 处理连接异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("连接异常: {}", cause.getMessage(), cause);
        ctx.close();
    }

    private Resp processCommand(RespArray respArray, ChannelHandlerContext ctx) {
        if(respArray.getContent().length==0){
            return new Errors("命令不能为空");
        }

        try {
            Resp[] array = respArray.getContent();
            RedisBytes cmd = ((BulkString)array[0]).getContent();
            
            // 直接使用byte[]比较，避免String转换
            CommandType commandType = null;
            for (CommandType type : CommandType.values()) {
                if (type.matchesRedisBytes(cmd)) {
                    commandType = type;
                    break;
                }
            }
            
            if (commandType == null) {
                return new Errors("命令不存在");
            }            // 使用命令对象池获取Command实例
            Command command = commandType.createCommand(redisCore);
            command.setContext(array);


            if(command instanceof Psync){
                ((Psync)command).setChannelHandlerContext(ctx);
                if(this.redisNode !=null && this.redisNode.isMaster()){
                    ((Psync)command).setMasterNode(this.redisNode);
                }
                log.info("执行PSYNC命令，来自：{}", ctx.channel().remoteAddress());
            }

            Resp result = command.handle();


            // 写命令处理：AOF持久化 + 主从复制传播
            if(command.isWriteCommand()){
                // AOF持久化
                if(aofManager != null){
                    aofManager.append(respArray);
                }
                
                // 主从复制：如果是主节点，则自动传播命令给从节点
                if(redisNode != null && redisNode.isMaster()){
                    ByteBuf buf = null;
                    try {
                        // 重用ByteBuf进行命令编码
                        buf = propagationBuf.get();
                        buf.clear();
                        respArray.encode(respArray, buf);
                        
                        // 获取可读字节，不创建新的数组
                        byte[] commandBytes = new byte[buf.readableBytes()];
                        buf.getBytes(buf.readerIndex(), commandBytes);
                        
                        redisNode.propagateCommand(commandBytes);
                        log.debug("[主节点] 写命令已传播: {} ({} bytes)", commandType, commandBytes.length);
                    } catch (Exception e) {
                        log.error("[主节点] 命令传播失败，命令: {}, 错误: {}", commandType, e.getMessage(), e);
                    }
                }
            }

            return result;
        } catch (Exception e) {
            log.error("命令执行失败", e);
            return new Errors("命令执行失败");
        }
    }

    public void setRedisNode(RedisNode redisNode) {
        this.redisNode = redisNode;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 清理ThreadLocal资源
        ByteBuf buf = propagationBuf.get();
        if (buf != null && buf.refCnt() > 0) {
            buf.release();
        }
        propagationBuf.remove();
        ctx.fireChannelInactive();
    }
}


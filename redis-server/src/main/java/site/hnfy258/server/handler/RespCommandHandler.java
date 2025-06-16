package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.command.impl.cluster.Psync;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.context.RedisContext;

@Slf4j
@Getter
public class RespCommandHandler extends SimpleChannelInboundHandler<Resp> {
    
    // ========== 核心组件：统一使用RedisContext ==========
    private final RedisContext redisContext;
    
    // ========== 系统状态 ==========
    private final boolean isMaster;

    // ========== 性能优化：重用的ByteBuf ==========
    private static final ThreadLocal<ByteBuf> propagationBuf = ThreadLocal.withInitial(() ->
        Unpooled.buffer(4096)
    );    /**
     * 构造函数：基于RedisContext的解耦架构
     * 
     * @param redisContext Redis统一上下文，提供所有核心功能的访问接口
     */
    public RespCommandHandler(final RedisContext redisContext) {
        this.redisContext = redisContext;
        this.isMaster = redisContext.isMaster();
        
        log.info("RespCommandHandler初始化完成 - 模式: {}", 
                isMaster ? "主节点" : "从节点");
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

    /**
     * 执行命令的公共接口
     * 
     * @param command 要执行的命令
     * @return 命令执行结果
     */
    public Resp executeCommand(final RespArray command) {
        return processCommand(command, null);
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
            }
            
            // 1. 使用RedisContext创建命令（解耦架构）
            final Command command = commandType.createCommand(redisContext);
            command.setContext(array);

            // 2. 特殊处理PSYNC命令
            if(command instanceof Psync){
                ((Psync)command).setChannelHandlerContext(ctx);
                final RedisNode masterNode = redisContext.getRedisNode();
                if(masterNode != null && masterNode.isMaster()){
                    ((Psync)command).setMasterNode(masterNode);
                }
                log.info("执行PSYNC命令，来自：{}", ctx.channel().remoteAddress());
            }            final Resp result = command.handle();

            // 3. 写命令处理：AOF持久化 + 主从复制传播
            if(command.isWriteCommand()){
                handleWriteCommand(respArray, commandType);
            }

            return result;
        } catch (Exception e) {
            log.error("命令执行失败", e);
            return new Errors("命令执行失败");
        }
    }
    
    /**
     * 处理写命令的持久化和复制
     * 
     * @param respArray 命令数组
     * @param commandType 命令类型
     */
    private void handleWriteCommand(final RespArray respArray, final CommandType commandType) {
        // 1. AOF持久化
        if(redisContext.isAofEnabled()){
            try {
                ByteBuf buf = propagationBuf.get();
                buf.clear();
                respArray.encode(respArray, buf);
                
                byte[] commandBytes = new byte[buf.readableBytes()];
                buf.getBytes(buf.readerIndex(), commandBytes);
                
                redisContext.writeAof(commandBytes);
                log.debug("[AOF] 写命令已持久化: {}", commandType);
            } catch (Exception e) {
                log.error("[AOF] 持久化失败，命令: {}, 错误: {}", commandType, e.getMessage(), e);
            }
        }
        
        // 2. 主从复制：如果是主节点，则自动传播命令给从节点
        if(redisContext.isMaster()){
            try {
                ByteBuf buf = propagationBuf.get();
                buf.clear();
                respArray.encode(respArray, buf);
                
                byte[] commandBytes = new byte[buf.readableBytes()];
                buf.getBytes(buf.readerIndex(), commandBytes);
                
                redisContext.propagateCommand(commandBytes);
                log.debug("[主节点] 写命令已传播: {} ({} bytes)", commandType, commandBytes.length);
            } catch (Exception e) {
                log.error("[主节点] 命令传播失败，命令: {}, 错误: {}", commandType, e.getMessage(), e);
            }        }
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


package site.hnfy258.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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
    
    // ========== 静态错误响应：避免重复创建 ==========
    private static final Errors UNSUPPORTED_COMMAND_ERROR = new Errors("不支持的命令");
    private static final Errors EMPTY_COMMAND_ERROR = new Errors("命令不能为空");
    private static final Errors COMMAND_NOT_FOUND_ERROR = new Errors("命令不存在");
    private static final Errors COMMAND_EXECUTION_ERROR = new Errors("命令执行失败");
    
    // ========== 核心组件：统一使用RedisContext ==========
    private final RedisContext redisContext;
      // ========== 系统状态 ==========
    private final boolean isMaster;

    /**
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
        if (msg instanceof RespArray) {
            RespArray respArray = (RespArray) msg;
            Resp response = processCommand(respArray, ctx);

            if (response != null) {
                //  零拷贝优化：直接写入到 Channel，避免中间缓冲
                writeResponseDirectly(ctx, response);
            }
        } else {
            //  复用静态错误响应实例
            writeResponseDirectly(ctx, UNSUPPORTED_COMMAND_ERROR);
        }
    }

    /**
     * 🚀 零拷贝响应写入：直接写入 Channel 避免额外的 ByteBuf 分配
     * 
     * @param ctx Channel 上下文
     * @param response 要发送的响应
     */
    private void writeResponseDirectly(final ChannelHandlerContext ctx, final Resp response) {
        try {
            // 1. 检查 Channel 是否活跃和可写
            if (!ctx.channel().isActive() || !ctx.channel().isWritable()) {
                log.debug("Channel 不可写，跳过响应发送");
                return;
            }

            // 2.  直接写入响应，让 Netty 的编码器处理零拷贝优化
            ctx.writeAndFlush(response);
            
        } catch (Exception e) {
            log.error("响应写入失败: {}", e.getMessage(), e);
            ctx.close();
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
    }    private Resp processCommand(RespArray respArray, ChannelHandlerContext ctx) {
        if (respArray.getContent().length == 0) {
            return EMPTY_COMMAND_ERROR;
        }

        try {
            Resp[] array = respArray.getContent();
            //  高性能命令查找：直接使用哈希表，O(1) 时间复杂度
            final RedisBytes cmd = ((BulkString) array[0]).getContent();
            final CommandType commandType = CommandType.findByBytes(cmd);
            
            if (commandType == null) {
                return COMMAND_NOT_FOUND_ERROR;
            }
            
            // 1. 使用RedisContext创建命令（解耦架构）
            final Command command = commandType.createCommand(redisContext);
            command.setContext(array);

            // 2. 特殊处理PSYNC命令
            if (command instanceof Psync) {
                ((Psync) command).setChannelHandlerContext(ctx);
                final RedisNode masterNode = redisContext.getRedisNode();
                if (masterNode != null && masterNode.isMaster()) {
                    ((Psync) command).setMasterNode(masterNode);
                }
                log.info("执行PSYNC命令，来自：{}", ctx.channel().remoteAddress());
            }

            // 3. 执行命令
            final Resp result = command.handle();

            // 4. 写命令处理：AOF持久化 + 主从复制传播
            if (command.isWriteCommand()) {
                handleWriteCommand(respArray, commandType);
            }

            return result;
        } catch (Exception e) {
            log.error("命令执行失败", e);
            return COMMAND_EXECUTION_ERROR;
        }
    }

    /**
     * 处理写命令的持久化和复制
     * 专注于网络层零拷贝优化，持久化层保持原有的简单设计
     * 
     * @param respArray 命令数组
     * @param commandType 命令类型
     */
    private void handleWriteCommand(final RespArray respArray, final CommandType commandType) {
        final boolean needAof = redisContext.isAofEnabled();
        final boolean needReplication = redisContext.isMaster();
        
        if (!needAof && !needReplication) {
            return; // 无需持久化和复制
        }

        if (needAof) {
            try {
                // 使用现有的字节数组接口进行 AOF 持久化
                final ByteBuf tempBuf = PooledByteBufAllocator.DEFAULT.buffer();
                try {
                    respArray.encode(respArray, tempBuf);
                    final byte[] commandBytes = new byte[tempBuf.readableBytes()];
                    tempBuf.readBytes(commandBytes);
                    redisContext.writeAof(commandBytes);
                    log.debug("[AOF] 写命令已持久化: {}", commandType);
                } finally {
                    tempBuf.release();
                }
            } catch (Exception e) {
                log.error("[AOF] 持久化失败，命令: {}, 错误: {}", commandType, e.getMessage(), e);
            }
        }
        
        if (needReplication) {
            try {
                final ByteBuf tempBuf = PooledByteBufAllocator.DEFAULT.buffer();
                try {
                    respArray.encode(respArray, tempBuf);
                    final byte[] commandBytes = new byte[tempBuf.readableBytes()];
                    tempBuf.readBytes(commandBytes);
                    redisContext.propagateCommand(commandBytes);
                    log.debug("[主节点] 写命令已传播: {}", commandType);
                } finally {
                    tempBuf.release();
                }
            } catch (Exception e) {
                log.error("[主节点] 命令传播失败，命令: {}, 错误: {}", commandType, e.getMessage(), e);
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.fireChannelInactive();
    }
}


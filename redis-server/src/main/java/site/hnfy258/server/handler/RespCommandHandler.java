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

/**
 * Redis命令处理器，负责解析和执行客户端请求。
 * 
 * <p>该类的主要职责：
 * <ul>
 *   <li>解析RESP协议命令
 *   <li>执行Redis命令
 *   <li>处理命令响应
 *   <li>管理持久化和复制
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用静态错误响应避免重复创建
 *   <li>实现零拷贝响应写入
 *   <li>优化ByteBuf的分配和释放
 *   <li>高效的命令查找机制
 * </ul>
 * 
 * <p>可靠性保证：
 * <ul>
 *   <li>完整的异常处理
 *   <li>资源的合理释放
 *   <li>连接状态的监控
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
@Getter
public class RespCommandHandler extends SimpleChannelInboundHandler<Resp> {
    
    /** 不支持的命令错误响应 */
    private static final Errors UNSUPPORTED_COMMAND_ERROR = new Errors("不支持的命令");
    
    /** 空命令错误响应 */
    private static final Errors EMPTY_COMMAND_ERROR = new Errors("命令不能为空");
    
    /** 命令不存在错误响应 */
    private static final Errors COMMAND_NOT_FOUND_ERROR = new Errors("命令不存在");
    
    /** 命令执行失败错误响应 */
    private static final Errors COMMAND_EXECUTION_ERROR = new Errors("命令执行失败");
    
    /** Redis服务器上下文 */
    private final RedisContext redisContext;
    
    /** 当前节点是否为主节点 */
    private final boolean isMaster;

    /**
     * 创建命令处理器实例。
     * 
     * <p>初始化过程：
     * <ul>
     *   <li>设置Redis上下文
     *   <li>确定节点角色
     *   <li>记录初始化信息
     * </ul>
     * 
     * @param redisContext Redis服务器上下文
     * @throws IllegalArgumentException 如果redisContext为null
     */
    public RespCommandHandler(final RedisContext redisContext) {
        if (redisContext == null) {
            throw new IllegalArgumentException("Redis上下文不能为null");
        }
        this.redisContext = redisContext;
        this.isMaster = redisContext.isMaster();
        
        log.info("RespCommandHandler初始化完成 - 模式: {}", 
                isMaster ? "主节点" : "从节点");
    }

    /**
     * 处理客户端请求。
     * 
     * <p>处理流程：
     * <ul>
     *   <li>验证命令格式
     *   <li>执行命令
     *   <li>返回响应
     * </ul>
     * 
     * @param ctx 通道上下文
     * @param msg RESP格式的命令
     * @throws Exception 如果处理过程中发生错误
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) throws Exception {
        if (msg instanceof RespArray) {
            RespArray respArray = (RespArray) msg;
            Resp response = processCommand(respArray, ctx);

            if (response != null) {
                writeResponseDirectly(ctx, response);
            }
        } else {
            writeResponseDirectly(ctx, UNSUPPORTED_COMMAND_ERROR);
        }
    }

    /**
     * 使用零拷贝技术写入响应。
     * 
     * <p>优化特点：
     * <ul>
     *   <li>直接写入Channel
     *   <li>避免中间缓冲
     *   <li>减少内存分配
     *   <li>提高响应速度
     * </ul>
     * 
     * @param ctx 通道上下文
     * @param response 要发送的响应
     */
    private void writeResponseDirectly(final ChannelHandlerContext ctx, final Resp response) {
        try {
            if (!ctx.channel().isActive() || !ctx.channel().isWritable()) {
                log.debug("Channel 不可写，跳过响应发送");
                return;
            }

            ctx.writeAndFlush(response);
            
        } catch (Exception e) {
            log.error("响应写入失败: {}", e.getMessage(), e);
            ctx.close();
        }
    }

    /**
     * 确保数据被及时刷新到客户端。
     * 
     * @param ctx 通道上下文
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
        super.channelReadComplete(ctx);
    }

    /**
     * 处理连接异常。
     * 
     * @param ctx 通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("连接异常: {}", cause.getMessage(), cause);
        ctx.close();
    }

    /**
     * 提供命令执行的公共接口。
     * 
     * <p>主要用于：
     * <ul>
     *   <li>内部命令执行
     *   <li>复制命令处理
     *   <li>脚本执行
     * </ul>
     * 
     * @param command 要执行的命令
     * @return 命令执行结果
     */
    public Resp executeCommand(final RespArray command) {
        return processCommand(command, null);
    }

    /**
     * 处理Redis命令。
     * 
     * <p>处理流程：
     * <ul>
     *   <li>解析命令类型
     *   <li>创建命令实例
     *   <li>执行命令
     *   <li>处理写命令的持久化和复制
     * </ul>
     * 
     * @param respArray 命令数组
     * @param ctx 通道上下文（可选）
     * @return 命令执行结果
     */
    private Resp processCommand(RespArray respArray, ChannelHandlerContext ctx) {
        if (respArray.getContent().length == 0) {
            return EMPTY_COMMAND_ERROR;
        }

        try {
            Resp[] array = respArray.getContent();
            final RedisBytes cmd = ((BulkString) array[0]).getContent();
            final CommandType commandType = CommandType.findByBytes(cmd);
            
            if (commandType == null) {
                return COMMAND_NOT_FOUND_ERROR;
            }
            
            final Command command = commandType.createCommand(redisContext);
            command.setContext(array);

            // 特殊处理PSYNC命令
            if (command instanceof Psync) {
                ((Psync) command).setChannelHandlerContext(ctx);
                final RedisNode masterNode = redisContext.getRedisNode();
                if (masterNode != null && masterNode.isMaster()) {
                    ((Psync) command).setMasterNode(masterNode);
                }
                log.info("执行PSYNC命令，来自：{}", ctx.channel().remoteAddress());
            }

            final Resp result = command.handle();

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
     * 处理写命令的持久化和复制。
     * 
     * <p>处理流程：
     * <ul>
     *   <li>检查是否需要AOF持久化
     *   <li>检查是否需要主从复制
     *   <li>执行持久化操作
     *   <li>传播命令到从节点
     * </ul>
     * 
     * <p>优化策略：
     * <ul>
     *   <li>使用池化的ByteBuf
     *   <li>确保资源正确释放
     *   <li>异常的优雅处理
     * </ul>
     * 
     * @param respArray 命令数组
     * @param commandType 命令类型
     */
    private void handleWriteCommand(final RespArray respArray, final CommandType commandType) {
        final boolean needAof = redisContext.isAofEnabled();
        final boolean needReplication = redisContext.isMaster();
        
        if (!needAof && !needReplication) {
            return;
        }

        if (needAof) {
            try {
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

    /**
     * 处理通道关闭事件。
     * 
     * @param ctx 通道上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.fireChannelInactive();
    }
}


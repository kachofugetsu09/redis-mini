package site.hnfy258.command.impl.cluster;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;

import site.hnfy258.server.context.RedisContext;
@Slf4j
public class Psync implements Command {
    private RedisContext redisContext;
    private RedisNode masterNodeInstance;
    private String masterId;
    private long offset;
    private ChannelHandlerContext ctx;

    
    /**
     * 新增构造函数：支持RedisContext的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param redisContext Redis统一上下文，提供所有核心功能的访问接口
     */
    public Psync(final RedisContext redisContext) {
        this.redisContext = redisContext;
        
        log.debug("Psync命令使用RedisContext模式初始化");
    }

    @Override
    public CommandType getType() {
        return CommandType.PSYNC;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length != 3){
            throw new IllegalArgumentException("Invalid PSYNC command format");
        }
        try{
            RedisBytes masterIdBytes = ((BulkString)array[1]).getContent();
            this.masterId = "?".equals(masterIdBytes.getString())?null:masterIdBytes.getString();

            RedisBytes offsetBytes = ((BulkString)array[2]).getContent();
            String offsetStr = offsetBytes.getString();
            this.offset = "-1".equals(offsetStr) ? -1 : Long.parseLong(offsetStr);
        }catch(Exception e){
            log.error("Error parsing PSYNC command: {}", e.getMessage());
            throw new IllegalArgumentException("Invalid PSYNC command format", e);
        }
    }

    public void setChannelHandlerContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }    @Override
    public Resp handle() {
        if (ctx == null) {
            log.error("ChannelHandlerContext is not set for PSYNC command");
            return new Errors("ChannelHandlerContext is not set for PSYNC command");
        }

        try {
            RedisNode nodeToUse = this.masterNodeInstance;

            // 如果没有显式设置masterNodeInstance，尝试获取
            if (nodeToUse == null) {
                nodeToUse = getEffectiveRedisNode();
            }
            
            if (nodeToUse == null || !nodeToUse.isMaster()) {
                log.error("No master node available for PSYNC command");
                return new Errors("No master node available for PSYNC command");
            }

            if (masterId == null || offset == -1) {
                log.info("执行全量同步,从节点：{}", ctx.channel().remoteAddress());
                return nodeToUse.doFullSync(ctx);
            } else {
                // 执行增量同步
                return nodeToUse.doPartialSync(ctx, masterId, offset);
            }
        } catch (Exception e) {
            log.error("Error handling PSYNC command: {}", e.getMessage());
            return new Errors("Error handling PSYNC command: " + e.getMessage());
        }
    }
      /**
     * 获取有效的RedisNode实例
     * 支持新旧两种模式的兼容性，避免循环依赖
     * 
     * @return RedisNode实例，如果无法获取返回null
     */    private RedisNode getEffectiveRedisNode() {
        // 1. 优先使用RedisContext的直接接口（推荐）
        if (redisContext != null) {
            return redisContext.getRedisNode();
        }
        
        // 2. RedisCore模式已移除循环依赖，无法再获取RedisNode
        // 必须使用RedisContext模式或外部设置masterNodeInstance
        log.warn("无法自动获取RedisNode，请确保使用RedisContext模式或手动设置masterNodeInstance");
        return null;
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }

    public void setMasterNode(RedisNode redisNode) {
        if(redisNode != null && redisNode.isMaster()) {
            this.masterNodeInstance = redisNode;
        } else {
            log.warn("Attempted to set a non-master node as master for PSYNC command");
        }
    }
}

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
import site.hnfy258.server.core.RedisCore;
@Slf4j
public class Psync implements Command {
    private RedisCore redisCore;
    private RedisNode masterNodeInstance;
    private String masterId;
    private long offset;
    private ChannelHandlerContext ctx;

    public Psync(RedisCore redisCore) {
        this.redisCore = redisCore;
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
    }

    @Override
    public Resp handle() {
        if(ctx == null){
            log.error("ChannelHandlerContext is not set for PSYNC command");
            return new Errors("ChannelHandlerContext is not set for PSYNC command");
        }

        try{
            RedisNode nodeToUse = this.masterNodeInstance;

            if(nodeToUse == null) {
                nodeToUse = redisCore.getServer().getRedisNode();
            }
            if(nodeToUse == null || !nodeToUse.isMaster()) {
                log.error("No master node available for PSYNC command");
                return new Errors("No master node available for PSYNC command");
            }

            if(masterId == null || offset == -1){
                log.info("执行全量同步,从节点：{}",ctx.channel().remoteAddress());
                return nodeToUse.doFullSync(ctx);
            }else{
                //执行增量同步
                return nodeToUse.doPartialSync(ctx, masterId, offset);
            }
        }catch(Exception e){
            log.error("Error handling PSYNC command: {}", e.getMessage());
            return new Errors("Error handling PSYNC command: " + e.getMessage());
        }
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

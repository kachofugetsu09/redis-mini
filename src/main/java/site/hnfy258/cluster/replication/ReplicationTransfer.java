package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.rdb.RdbManager;
@Slf4j
public class ReplicationTransfer {
    private final RedisNode node;

    public ReplicationTransfer(RedisNode node) {
        this.node = node;
    }
    public byte[] generateRdbData(RdbManager rdbManager) {
        if(rdbManager ==null){
            log.error("RDB管理器未初始化，无法生成RDB数据");
            return null;
        }

        byte[] rdbContent = rdbManager.createTempRdbForReplication();
        if(rdbContent == null) {
            log.error("RDB数据生成失败");
            return null;
        }

        log.info("RDB数据生成成功，长度: {} bytes", rdbContent.length);
        return rdbContent;
    }

    public void sendFullSyncData(ChannelHandlerContext ctx, byte[] rdbContent, String nodeId, long replicationOffset) {
        //1.发送FULLRESYNC响应
        String fullResyncResponseStr  = String.format("FULLRESYNC %s %d",nodeId,replicationOffset);

        Resp fullResyncResp = new SimpleString(fullResyncResponseStr);

        ByteBuf fullResyncBuf = Unpooled.buffer();
        try{
            fullResyncResp.encode(fullResyncResp,fullResyncBuf);
            ctx.writeAndFlush(fullResyncBuf).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("全量同步响应发送成功: {}", fullResyncResponseStr);

                    //2.发送给rdb数据
                    Resp rdbResp = new BulkString(rdbContent);
                    ByteBuf rdbBuf = Unpooled.buffer();
                    try{
                        rdbResp.encode(rdbResp, rdbBuf);
                        ctx.writeAndFlush(rdbBuf).addListener(rdbFuture -> {
                            if (rdbFuture.isSuccess()) {
                                log.info("RDB数据发送成功，长度: {} bytes", rdbContent.length);
                            } else {
                                log.error("RDB数据发送失败", rdbFuture.cause());
                            }
                        });
                    } catch (Exception e) {
                        log.error("RDB数据编码失败", e);
                        rdbBuf.release();
                    }
                } else {
                    log.error("全量同步响应发送失败", future.cause());
                }
            });
        }catch(Exception e) {
            log.error("全量同步响应编码失败", e);
            fullResyncBuf.release();
        }
    }

    public boolean receiveAndLoadRdb(byte[] rdbContent, RdbManager rdbManager) {
        if(rdbContent == null || rdbContent.length == 0) {
            log.error("接收到的RDB数据为空，无法加载");
            return false;
        }

        log.info("开始加载接收到的RDB数据，长度: {} bytes", rdbContent.length);

        try{
            if(node.getRedisCore() != null){
                log.info("开始加载RDB数据到Redis核心");
                node.getRedisCore().flushAll();
            }

            boolean success = rdbManager.loadRdbFromBytes(rdbContent);
            if(success){
                log.info("RDB数据加载成功节点是{}", node.getNodeId());
                node.getRedisCore().selectDB(0);
                return true;
            } else {
                log.error("RDB数据加载失败");
                return false;
            }
        }catch(Exception e) {
            log.error("加载RDB数据时发生错误", e);
            return false;
        }
    }
}

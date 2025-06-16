package site.hnfy258.cluster.heartbeat;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

@Slf4j
@Getter
@Setter
public class HeartbeatManager {
    private static final int HEARTBEAT_INTERVAL_MS = 1000; // 心跳间隔时间，单位毫秒
    private RedisNode redisNode;
    private long heartbeatInterval;

    private volatile boolean running;
    private volatile boolean paused;
    private Thread heartbeatThread;
    private HeartbeatStats stats;

    private HeartbeatCallback callback;

    public boolean isHeartbeatRunning() {
        return running;
    }

    public interface HeartbeatCallback {
        void onConnectionLost();
        void onHeartbeatFailed();
    }

    public HeartbeatManager(RedisNode redisNode){
        this(redisNode, HEARTBEAT_INTERVAL_MS);

    }
    public HeartbeatManager(RedisNode redisNode, long heartbeatInterval) {
        this.redisNode = redisNode;
        this.heartbeatInterval = heartbeatInterval;
        this.paused = false;
        this.stats = new HeartbeatStats();
    }

    public void startHeartbeat(HeartbeatCallback callback){
        if(!running){
            this.callback = callback;
            running = true;
            heartbeatThread = new Thread(this::run,"heartbeat-thread-"+redisNode.getNodeId());
            heartbeatThread.start();
            log.info("心跳线程已启动，节点: {}", redisNode.getNodeId());
        }
    }

    public void stopHeartbeat() {
        if (running) {
            running = false;
            if (heartbeatThread != null && heartbeatThread.isAlive()) {
                heartbeatThread.interrupt();
                heartbeatThread = null;
            }
            log.info("心跳线程已停止，节点: {}", redisNode.getNodeId());
        }
    }

    public void pasue(){
        this.paused = true;
        log.debug("心跳已暂停，节点: {}", redisNode.getNodeId());
    }

    public void resume() {
        this.paused = false;
        log.debug("心跳已恢复，节点: {}", redisNode.getNodeId());
    }

    private void run(){
        try{
            if(!paused){
                sendHeartbeat();
            }
            Thread.sleep(heartbeatInterval);
        }catch(InterruptedException e){
            log.warn("心跳线程被中断，节点: {}", redisNode.getNodeId());
        }catch (Exception e){
            log.error("心跳线程发生异常，节点: {}", redisNode.getNodeId(), e);

        }
        log.info("心跳线程已结束，节点: {}", redisNode.getNodeId());
    }

    private void sendHeartbeat() {
        try{            if(redisNode.getClientChannel() != null && redisNode.getClientChannel().isActive()) {
                // 🚀 优化：使用 RedisBytes 缓存 PING 命令
                Resp[] pingArray = new Resp[]{
                        new BulkString(RedisBytes.fromString("PING"))
                };
                RespArray pingCommand = new RespArray(pingArray);
                // 发送心跳命令
                redisNode.getClientChannel().writeAndFlush(pingCommand);
                stats.recordHeartbeatSent();
                log.debug("心跳已发送，节点: {}", redisNode.getNodeId());
            } else {
                stats.recordHeartbeatFailed();
                if(callback != null) {
                   if(stats.getFailedAttempts() >=3){
                       callback.onConnectionLost();
                   }else{
                       callback.onHeartbeatFailed();
                   }
                }
                log.warn("心跳发送失败，节点: {}", redisNode.getNodeId());
            }

        }catch(Exception e){
            stats.recordHeartbeatFailed();
            log.error("发送心跳时发生异常，节点: {}", redisNode.getNodeId(), e);
            if(callback != null) {
                callback.onHeartbeatFailed();
            }
        } finally {
            // 重置失败次数
            stats.resetFailedCount();
        }
    }

    public void shutdown(){
        stopHeartbeat();
    }

    public HeartbeatStats getStats(){
        return stats;
    }

    public void resetFailedCount() {
        stats.resetFailedCount();
        log.debug("已重置失败次数，节点: {}", redisNode.getNodeId());
    }
}

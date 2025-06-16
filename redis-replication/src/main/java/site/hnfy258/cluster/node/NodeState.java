package site.hnfy258.cluster.node;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.replication.ReplBackLog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
@Slf4j
public class NodeState {
    private String nodeId;
    private String host;
    private int port;
    private boolean isMaster;

    private Channel channel;
    private Channel clientChannel;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    private volatile RedisNode masterNode;
    private volatile String masterHost;
    private volatile int masterPort;
    private final List<RedisNode> slaves = new ArrayList<>();



    private final AtomicLong replicationOffset = new AtomicLong(0);
    private final AtomicLong masterReplicationOffset = new AtomicLong(0);
    private final AtomicBoolean readyForReplCommands = new AtomicBoolean(false);
    private volatile ReplBackLog replBackLog;

    private volatile String lastKnownMasterId;
    private volatile long lastKnownOffset = -1;


    public NodeState(String nodeId, String host, int port, boolean isMaster) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.isMaster = isMaster;

        if(isMaster){
            this.replBackLog = new ReplBackLog(nodeId);
        }else{
            this.replBackLog = new ReplBackLog();
        }
    }

    public long getReplicationOffset() {
        return replicationOffset.get();
    }

    public void setReplicationOffset(long offset) {
        replicationOffset.getAndSet(offset);
    }

    public long getMasterReplicationOffset() {
        return masterReplicationOffset.get();
    }

    public void setMasterReplicationOffset(long offset) {
        masterReplicationOffset.getAndSet(offset);
    }

    public boolean getReadyForReplCommands() {
        return readyForReplCommands.get();
    }

    public void setReadyForReplCommands(boolean ready) {
        readyForReplCommands.set(ready);
    }


    public synchronized void setMasterNode(RedisNode masterNode) {
       if(isMaster){
           throw new IllegalStateException("不可以设置主节点为主节点");
       }
        this.masterNode = masterNode;
        if(masterNode != null) {
            this.masterHost = masterNode.getHost();
            this.masterPort = masterNode.getPort();
        } else {
            this.masterHost = null;
            this.masterPort = 0;
            log.warn("设置主节点为null，可能会导致数据不一致");
        }
    }

    public synchronized void addSlaveNode(RedisNode slaveNode) {
        if(slaveNode.isMaster()) {
            throw new IllegalStateException("不可以将主节点添加为从节点");
        }
       if(!slaves.contains(slaveNode)){
           slaves.add(slaveNode);
           log.info("添加从节点 {}:{}", slaveNode.getHost(), slaveNode.getPort());
       }
    }

    public synchronized void removeSlaveNode(RedisNode slaveNode) {
        if(slaves.remove(slaveNode)) {
            log.info("移除从节点 {}:{}", slaveNode.getHost(), slaveNode.getPort());
        } else {
            log.warn("尝试移除不存在的从节点 {}:{}", slaveNode.getHost(), slaveNode.getPort());
        }
    }

    public synchronized List<RedisNode> getSlaves() {
        return new ArrayList<>(slaves);
    }

    public synchronized int getSlaveCount() {
        return slaves.size();
    }

    public synchronized boolean isConnected() {
        return connected.get();
    }

    public void setConnected(boolean connected){
        boolean oldState = this.connected.getAndSet(connected);
        if(oldState != connected) {
            log.info("节点状态变更: {} -> {}", oldState, connected);
        }
    }

    public void cleanup(){
        try{
            if(channel != null && channel.isOpen()) {
                channel.close();
            }
            if(clientChannel != null && clientChannel.isOpen()) {
                clientChannel.close();
            }
            setConnected(false);
            log.info("节点状态已清理: {}", nodeId);
        }catch(Exception e) {
            log.error("清理节点状态时发生错误: {}", nodeId, e);
        }
    }

    public boolean isReadyForReplCommands() {
        return readyForReplCommands.get();
    }


    public void saveConnectionInfo(String masterId, long offset){
        this.lastKnownMasterId = masterId;
        this.lastKnownOffset = offset;
        log.info("保存连接信息: 主节点ID={}, 偏移量={}", masterId, offset);
    }



    public void clearConnectionInfo(){
        this.lastKnownMasterId = null;
        this.lastKnownOffset = -1;
        log.info("清除连接信息");
    }

    public boolean hasSavedConnectionInfo() {
        return lastKnownMasterId != null && lastKnownOffset >= 0;
    }
}


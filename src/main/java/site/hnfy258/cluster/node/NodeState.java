package site.hnfy258.cluster.node;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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


    public NodeState(String nodeId, String host, int port, boolean isMaster) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.isMaster = isMaster;
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
}

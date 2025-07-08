package site.hnfy258.rpc;

import java.io.Serializable;

/**
 * Raft消息封装类
 * 用于在网络中传输Raft相关的消息
 */
public class RaftMessage implements Serializable {
    
    public enum Type {
        REQUEST_VOTE,
        REQUEST_VOTE_REPLY,
        APPEND_ENTRIES,
        APPEND_ENTRIES_REPLY
    }
    
    private Type type;
    private Object payload;
    private long requestId; // 用于匹配请求和响应
    
    public RaftMessage() {}
    
    public RaftMessage(Type type, Object payload) {
        this.type = type;
        this.payload = payload;
    }
    
    public RaftMessage(Type type, Object payload, long requestId) {
        this.type = type;
        this.payload = payload;
        this.requestId = requestId;
    }
    
    // Getters and Setters
    public Type getType() {
        return type;
    }
    
    public void setType(Type type) {
        this.type = type;
    }
    
    public Object getPayload() {
        return payload;
    }
    
    public void setPayload(Object payload) {
        this.payload = payload;
    }
    
    public long getRequestId() {
        return requestId;
    }
    
    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }
    
    @Override
    public String toString() {
        return "RaftMessage{" +
                "type=" + type +
                ", requestId=" + requestId +
                ", payload=" + payload +
                '}';
    }
}

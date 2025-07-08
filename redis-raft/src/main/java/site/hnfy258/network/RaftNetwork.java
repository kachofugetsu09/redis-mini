package site.hnfy258.network;

import site.hnfy258.raft.Raft;
import site.hnfy258.rpc.AppendEntriesArgs;
import site.hnfy258.rpc.AppendEntriesReply;
import site.hnfy258.rpc.RequestVoteArg;
import site.hnfy258.rpc.RequestVoteReply;

import java.util.concurrent.CompletableFuture;

/**
 * Raft网络层抽象接口
 * 定义了Raft节点之间的通信接口
 */
public interface RaftNetwork {
    
    /**
     * 发送投票请求
     * @param targetServerId 目标服务器ID
     * @param request 投票请求参数
     * @return 异步的投票回复
     */
    CompletableFuture<RequestVoteReply> sendRequestVote(int targetServerId, RequestVoteArg request);
    
    /**
     * 发送心跳/日志追加请求
     * @param targetServerId 目标服务器ID
     * @param request 心跳请求参数
     * @return 异步的心跳回复
     */
    CompletableFuture<AppendEntriesReply> sendAppendEntries(int targetServerId, AppendEntriesArgs request);
    
    /**
     * 启动网络服务
     * @param serverId 当前服务器ID
     * @param raft Raft实例，用于处理接收到的请求
     */
    void start(int serverId, Raft raft);
    
    /**
     * 停止网络服务
     */
    void stop();
}

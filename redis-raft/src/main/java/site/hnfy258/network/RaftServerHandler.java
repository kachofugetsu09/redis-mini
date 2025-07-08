package site.hnfy258.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import site.hnfy258.raft.Raft;
import site.hnfy258.rpc.RaftMessage;
import site.hnfy258.rpc.AppendEntriesArgs;
import site.hnfy258.rpc.AppendEntriesReply;
import site.hnfy258.rpc.RequestVoteArg;
import site.hnfy258.rpc.RequestVoteReply;

/**
 * Raft服务器端消息处理器
 * 处理接收到的Raft请求消息
 */
public class RaftServerHandler extends SimpleChannelInboundHandler<RaftMessage> {
    
    private final NettyRaftNetwork network;
    private final Raft raftNode;
    
    public RaftServerHandler(NettyRaftNetwork network, Raft raftNode) {
        this.network = network;
        this.raftNode = raftNode;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
        switch (msg.getType()) {
            case REQUEST_VOTE:
                handleRequestVote(ctx, msg);
                break;
            case APPEND_ENTRIES:
                handleAppendEntries(ctx, msg);
                break;
            default:
                System.err.println("Unknown message type: " + msg.getType());
        }
    }
    
    private void handleRequestVote(ChannelHandlerContext ctx, RaftMessage msg) {
        RequestVoteArg request = (RequestVoteArg) msg.getPayload();
        
        // 异步处理请求，避免阻塞Netty的事件循环
        ctx.executor().execute(() -> {
            try {
                RequestVoteReply reply = raftNode.handleRequestVoteRequest(request);
                
                RaftMessage response = new RaftMessage(
                    RaftMessage.Type.REQUEST_VOTE_REPLY, 
                    reply, 
                    msg.getRequestId()
                );
                
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                System.err.println("Error handling RequestVote: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }
    
    private void handleAppendEntries(ChannelHandlerContext ctx, RaftMessage msg) {
        AppendEntriesArgs request = (AppendEntriesArgs) msg.getPayload();
        
        // 异步处理心跳请求，避免阻塞Netty的事件循环
        ctx.executor().execute(() -> {
            try {
                AppendEntriesReply reply = raftNode.handleAppendEntriesRequest(request);
                
                RaftMessage response = new RaftMessage(
                    RaftMessage.Type.APPEND_ENTRIES_REPLY, 
                    reply, 
                    msg.getRequestId()
                );
                
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                System.err.println("Error handling AppendEntries: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Client disconnected: " + ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("Exception in server handler: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}

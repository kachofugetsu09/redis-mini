package site.hnfy258.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import site.hnfy258.rpc.RaftMessage;

/**
 * Raft客户端消息处理器
 * 处理接收到的Raft响应消息
 */
public class RaftClientHandler extends SimpleChannelInboundHandler<RaftMessage> {
    
    private final NettyRaftNetwork network;
    
    public RaftClientHandler(NettyRaftNetwork network) {
        this.network = network;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
        switch (msg.getType()) {
            case REQUEST_VOTE_REPLY:
            case APPEND_ENTRIES_REPLY:
                // 将响应传递给网络层处理
                network.handleResponse(msg.getRequestId(), msg.getPayload());
                break;
            default:
                System.err.println("Unknown response message type: " + msg.getType());
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Connection to server lost: " + ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("Exception in client handler: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}

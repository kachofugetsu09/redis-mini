package site.hnfy258.cluster.replication.utils;

import io.netty.channel.ChannelHandlerContext;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

public final class ReplicationUtils {
    private ReplicationUtils() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    public static String getRemoteAddress(ChannelHandlerContext ctx) {
        try{
            if(ctx!=null && ctx.channel() !=null && ctx.channel().remoteAddress() != null) {
                return ctx.channel().remoteAddress().toString();
            }
        }catch (Exception e){
        }
        return "unknown";
    }

    public static RespArray parseCommandToRespArray(String commandStr){
        //1.分割命令行
        String[] lines = commandStr.split("\r\n");
        if(lines.length <2 || !lines[0].startsWith("*")) {
            throw new IllegalArgumentException("Invalid command format");
        }
        //2.解析参数数量
        int argCount = Integer.parseInt(lines[0].substring(1));
        Resp[] args = new Resp[argCount];
        //3.解析每个参数

        int lineIndex = 1;
        for(int i=0;i<argCount;i++){
            if(lineIndex >=lines.length || !lines[lineIndex].startsWith("$")) {
                throw new IllegalArgumentException("Invalid command format");
            }

            lineIndex++;

            if(lineIndex >=lines.length) {
                throw new IllegalArgumentException("Invalid command format");
            }

            String data = lines[lineIndex];
            args[i] = new BulkString(new RedisBytes(data.getBytes()));
            lineIndex++;
        }

        return new RespArray(args);
    }
}

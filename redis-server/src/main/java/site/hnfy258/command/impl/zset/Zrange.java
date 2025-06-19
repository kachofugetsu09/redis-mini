package site.hnfy258.command.impl.zset;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisZset;
import site.hnfy258.datastructure.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.context.RedisContext;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Zrange implements Command {
    private RedisContext redisContext;
    private RedisBytes key;
    private int start;
    private int stop;
    private boolean withScores = false;

    public Zrange(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZRANGE;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length < 4){
            throw new IllegalArgumentException("ZRANGE command requires at least 4 arguments.");
        }
        key = ((BulkString)array[1]).getContent();
        RedisBytes startBytes = ((BulkString)array[2]).getContent();
        start = Integer.parseInt(startBytes.getString());
        RedisBytes stopBytes = ((BulkString)array[3]).getContent();
        stop = Integer.parseInt(stopBytes.getString());
        if(array.length == 5){
            RedisBytes option = ((BulkString)array[4]).getContent();
            if(option.getString().equalsIgnoreCase("WITHSCORES")){
                withScores = true;
            }
        }
    }

    @Override
    public Resp handle() {
        try{
            // 检查key是否存在
            RedisData data = redisContext.get(key);
            if(data == null) return new RespArray(new Resp[0]);

            // 检查类型
            if(!(data instanceof RedisZset)){
                return new Errors("ERR wrong type for 'zrange' command");
            }
            
            RedisZset redisZset = (RedisZset) data;
            int size = redisZset.size();
            if(size == 0) return new RespArray(new Resp[0]);

            // 处理索引
            int startIndex = start;
            int stopIndex = stop;

            if(startIndex < 0) startIndex = size + startIndex;
            if(stopIndex < 0) stopIndex = size + stopIndex;

            startIndex = Math.max(0, startIndex);
            stopIndex = Math.min(size-1, stopIndex);

            if(startIndex > stopIndex){
                return new RespArray(new Resp[0]);
            }
              // 获取范围数据
            List<RedisZset.ZsetNode> range;
            try {
                range = redisZset.getRange(startIndex, stopIndex);
                if(range == null) {
                    return new RespArray(new Resp[0]);
                }
                
                // 不需要再反转列表，因为已经在ConcurrentSkipListMap中正确排序
                // Collections.reverse(range);
            } catch (Exception e) {
                return new Errors("ERR Failed to get range: " + e.getMessage());
            }
            
            // 构建返回数据
            List<Resp> respList = new ArrayList<>();
            try {
                for(RedisZset.ZsetNode node : range){
                    if(node == null || node.getMember() == null) {
                        continue; // 跳过空节点
                    }
                    
                    // 优化：使用 RedisBytes.fromString 获得缓存和零拷贝优势
                    final RedisBytes memberBytes = RedisBytes.fromString(node.getMember());
                    respList.add(new BulkString(memberBytes));
                    
                    if(withScores){
                        final RedisBytes scoreBytes = RedisBytes.fromString(String.valueOf(node.getScore()));
                        respList.add(new BulkString(scoreBytes));
                    }
                }
            } catch (Exception e) {
                return new Errors("ERR Failed to process range results: " + e.getMessage());
            }
            
            return new RespArray(respList.toArray(new Resp[0]));
        } catch(Exception e) {
            // 打印异常堆栈供调试
            e.printStackTrace();
            if(e.getMessage() == null) {
                return new Errors("ERR Internal error in 'zrange' command");
            }
            return new Errors("ERR " + e.getMessage());
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

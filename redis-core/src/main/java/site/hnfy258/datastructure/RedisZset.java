package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.internal.Dict;
import site.hnfy258.internal.SkipList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.*;
@Setter
@Getter
public class RedisZset implements RedisData{
    private volatile long timeout = -1;
    private SkipList<String> skipList;
    private Dict<Double,Object> dict;
    private RedisBytes key;

    public RedisZset() {
        skipList = new SkipList<>();
        dict = new Dict<>();
    }
    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public List<Resp> convertToResp() {
        List<Resp> result = new ArrayList<>();
        if(dict.size() == 0){
            return Collections.emptyList();
        }
       for(Map.Entry<Object,Object> entry : dict.entrySet()) {
           if (!(entry.getKey() instanceof Double)) {
               continue;
           }
           Double score = (Double) entry.getKey();
           Object member = entry.getValue();
           List<Resp> zaddCommand = new ArrayList<>();
           zaddCommand.add(new BulkString("ZADD".getBytes()));
           zaddCommand.add(new BulkString(key.getBytesUnsafe()));
           zaddCommand.add(new BulkString(score.toString().getBytes()));
           if (member instanceof RedisBytes) {
               zaddCommand.add(new BulkString(((RedisBytes) member).getBytesUnsafe()));
           } else {
               zaddCommand.add(new BulkString(member.toString().getBytes()));
           }
           result.add(new RespArray(zaddCommand.toArray(new Resp[0])));
       }
       return result;
    }

    public boolean add(double score, Object member){
        if(dict.contains(score,member)){
            return false;
        }
        dict.put(score, member);
        
        // 将成员转换为String，确保可比较
        String memberStr;
        if (member instanceof RedisBytes) {
            memberStr = ((RedisBytes) member).getString();
        } else {
            memberStr = member.toString();
        }
        
        skipList.insert(score, memberStr);
        return true;
    }

    public List<SkipList.SkipListNode<String>> getRange(int start, int stop){
        return skipList.getElementByRankRange(start, stop);
    }

    public List<SkipList.SkipListNode<String>> getRangeByScore(double min, double max){
        return skipList.getElementByScoreRange(min, max);
    }

    public int size(){
        return dict.size();
    }
    @SuppressWarnings("unchecked")
    public Iterable<? extends Map.Entry<Double, Object>> getAll() {
        // 使用显式类型转换创建一个新的包含正确类型的集合
        Map<Double, Object> typedMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : dict.entrySet()) {
            if (entry.getKey() instanceof Double) {
                typedMap.put((Double) entry.getKey(), entry.getValue());
            }
        }
        return typedMap.entrySet();
    }
}

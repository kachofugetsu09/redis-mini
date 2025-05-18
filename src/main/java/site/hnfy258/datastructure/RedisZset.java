package site.hnfy258.datastructure;

import site.hnfy258.internal.Dict;
import site.hnfy258.internal.SkipList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisZset implements RedisData{
    private volatile long timeout = -1;
    private SkipList<String> skipList;
    private Dict<Double,Object> dict;

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
        for(Double score : dict.keySet()){
            Object member = dict.get(score);
            Resp[] resp = new Resp[2];
            resp[0] = new BulkString(score.toString().getBytes());
            if(member instanceof RedisBytes){
                resp[1] = new BulkString(((RedisBytes) member).getBytes());
            }else{
                resp[1] = new BulkString(member.toString().getBytes());
            }
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
        return (Iterable<? extends Map.Entry<Double, Object>>) dict.entrySet();
    }
}

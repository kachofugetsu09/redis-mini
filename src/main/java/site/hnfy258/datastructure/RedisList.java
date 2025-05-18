package site.hnfy258.datastructure;

import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class RedisList implements RedisData{
    private volatile long timeout = -1;
    private LinkedList<RedisBytes> list;

    public RedisList() {
        this.list = new LinkedList<>();
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
        for(RedisBytes value : list){
            result.add(new BulkString(value));
        }
        return result;
    }

    public int size(){
        return list.size();
    }

    public void lpush(RedisBytes... values){
        for(RedisBytes value : values){
            list.addFirst(value);
        }
    }

    public RedisBytes lpop(){
        return list.pollFirst();
    }

    public List<RedisBytes> lrange(int start, int stop){
        int size = list.size();
        start = Math.max(0, start);
        stop = Math.min(size-1, stop);

        if(start <= stop){
            return list.subList(start, stop+1);
        }
        return Collections.emptyList();
    }

    public int remove(RedisBytes key){
        int count=0;
        while(list.remove(key)){
            count++;
        }
        return count;
    }

    public RedisBytes[] getAll() {
        RedisBytes[] result = new RedisBytes[list.size()];
        for(int i=0; i<list.size(); i++){
            result[i] = list.get(i);
        }
        return result;
    }
}

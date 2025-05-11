package site.hnfy258.datastructure;

import site.hnfy258.internal.Dict;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class RedisSet implements RedisData{
    private volatile long timeout = -1;
    private Dict<RedisBytes, Object> setCore;

    public RedisSet() {
        this.setCore = new Dict<>();
    }

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int add(List<RedisBytes> members){
        int count = 0;
        for(RedisBytes member: members){
            if(!setCore.containsKey(member)){
                count++;
                setCore.put(member, null);
            }
        }
        return count;
    }

    public int remove(RedisBytes member){
        int count=0;
        if(setCore.containsKey(member)){
            setCore.remove(member);
            count++;
        }
        return count;
    }

    public List<RedisBytes> pop(int count){
        if(setCore.size() ==0) return Collections.emptyList();

        count = Math.min(count, setCore.size());
        List<RedisBytes> poppedElements = new ArrayList<>(count);

        Random random = new Random();
        for(int i=0;i<count;i++){
            RedisBytes member = (RedisBytes) setCore.keySet().toArray()[random.nextInt(setCore.size())];
            poppedElements.add(member);
            setCore.remove(member);
        }
        return poppedElements;
    }

    public int size(){
        return setCore.size();
    }
}

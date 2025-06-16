package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.internal.Dict;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
@Setter
@Getter
public class RedisSet implements RedisData{
    private volatile long timeout = -1;
    private Dict<RedisBytes, Object> setCore;
    private RedisBytes key;

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

    @Override
    public List<Resp> convertToResp() {
        List<Resp> result = new ArrayList<>();
        if(setCore.size() == 0){
            return Collections.emptyList();
        }        List<Resp> saddCommand = new ArrayList<>();
        // 🚀 优化：使用 RedisBytes 缓存 SADD 命令
        saddCommand.add(new BulkString(RedisBytes.fromString("SADD")));
        saddCommand.add(new BulkString(key.getBytesUnsafe()));
        for(RedisBytes member : setCore.keySet()){
            saddCommand.add(new BulkString(member.getBytesUnsafe()));
        }
        result.add(new RespArray(saddCommand.toArray(new Resp[0])));
        return result;
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

    public RedisBytes[] getAll() {
        RedisBytes[] result = new RedisBytes[setCore.size()];
        int i = 0;
        for(RedisBytes key : setCore.keySet()){
            result[i++] = key;
        }
        return result;
    }
}

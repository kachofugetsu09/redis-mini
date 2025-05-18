package site.hnfy258.datastructure;

import site.hnfy258.internal.Dict;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisHash implements RedisData{
    private volatile long timeout = -1;
    private Dict<RedisBytes,RedisBytes>hash;

    public RedisHash() {
        this.hash = new Dict<>();
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
        if(hash.size() ==0){
            return result;
        }
        for(Map.Entry<Object, Object> entry : hash.entrySet()){
           Resp[] resp = new Resp[2];
           RedisBytes key = (RedisBytes) entry.getKey();
           RedisBytes value = (RedisBytes) entry.getValue();
           resp[0] = new BulkString(key);
           resp[1] = new BulkString(value);
           result.add(new RespArray(resp));
        }
        return result;
    }

    public int put(RedisBytes field, RedisBytes value){
        return hash.put(field, value)==null?1:0;
    }

    public Dict<RedisBytes,RedisBytes> getHash() {
        return hash;
    }

    public int del(List<RedisBytes> fields){
        return (int)fields.stream().filter(field -> hash.remove(field) != null).count();
    }

}

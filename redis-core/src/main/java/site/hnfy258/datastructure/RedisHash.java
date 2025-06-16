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
import java.util.Map;
@Setter
@Getter
public class RedisHash implements RedisData{
    private volatile long timeout = -1;
    private Dict<RedisBytes,RedisBytes>hash;
    private RedisBytes key;

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
        if(hash == null || hash.size() == 0){
            return Collections.emptyList();
        }
        List<Resp> result = new ArrayList<>();
        for(Map.Entry<Object, Object> entry : hash.entrySet()){
            Object field = entry.getKey();
            Object value = entry.getValue();            List<Resp> hsetCommand = new ArrayList<>();
            // 🚀 优化：使用 RedisBytes 缓存 HSET 命令
            hsetCommand.add(new BulkString(RedisBytes.fromString("HSET")));
            hsetCommand.add(new BulkString(key.getBytesUnsafe()));
            if(field instanceof RedisBytes) {
                hsetCommand.add(new BulkString(((RedisBytes) field).getBytesUnsafe()));
            }
            else{
                hsetCommand.add(new BulkString(field.toString().getBytes()));
            }
            if(value instanceof RedisBytes) {
                hsetCommand.add(new BulkString(((RedisBytes) value).getBytesUnsafe()));
            }
            else{
                hsetCommand.add(new BulkString(value.toString().getBytes()));
            }
            result.add(new RespArray(hsetCommand.toArray(new Resp[0])));
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

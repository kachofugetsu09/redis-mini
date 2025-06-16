package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
@Setter
@Getter
public class RedisString implements RedisData{
    private volatile long timeout;
    private Sds value;
    private RedisBytes key;
    private RedisBytes cachedValue;

    public RedisString(Sds value) {
        this.value = value;
        this.timeout = -1;
        this.cachedValue = null;
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
        if(value == null){
            return Collections.emptyList();
        }
        List<Resp> setCommand = new ArrayList<>();
        setCommand.add(new BulkString("SET".getBytes()));
        setCommand.add(new BulkString(key.getBytes()));
        setCommand.add(new BulkString(value.getBytes()));
        return Collections.singletonList(new RespArray(setCommand.toArray(new Resp[0])));
    }    public RedisBytes getValue() {
        if (cachedValue == null) {
            cachedValue = new RedisBytes(value.getBytes());
        }
        return cachedValue;
    }

    /**
     * 获取内部SDS对象的直接引用
     * 用于高效的字符串操作如append, length等
     * 
     * @return SDS对象引用
     */
    public Sds getSds() {
        return value;
    }

    public void setSds(Sds sds) {
        this.value = sds;
        this.cachedValue = null;
    }

    public long incr(){
        try{
            long cur = Long.parseLong(value.toString());
            long newValue = cur + 1;
            value = new Sds(String.valueOf(newValue).getBytes());
            cachedValue = null;
            return newValue;
        }catch(NumberFormatException e){
            throw new IllegalStateException("value is not a number");
        }
    }

}

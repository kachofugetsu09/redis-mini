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

    public RedisString(Sds value) {
        this.value = value;
        this.timeout = -1;
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
    }

    public RedisBytes getValue() {
        return new RedisBytes(value.getBytes());
    }

    public void setSds(Sds sds) {
        this.value = sds;
    }

    public long incr(){
        try{
            long cur = Long.parseLong(value.toString());
            long newValue = cur + 1;
            value = new Sds(String.valueOf(newValue).getBytes());
            return newValue;
        }catch(NumberFormatException e){
            throw new IllegalStateException("value is not a number");
        }
    }

}

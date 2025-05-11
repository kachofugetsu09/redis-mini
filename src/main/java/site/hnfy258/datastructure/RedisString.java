package site.hnfy258.datastructure;

import site.hnfy258.internal.Sds;

public class RedisString implements RedisData{
    private volatile long timeout;
    private Sds value;

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

package site.hnfy258.datastructure;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RedisBytes {
    private byte[] bytes;
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public RedisBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o){
        if(o == this){
            return true;
        }
        if(o == null || o.getClass() != getClass()){
            return false;
        }
        RedisBytes other = (RedisBytes) o;
        return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(bytes);
    }

    public byte[] getBytes(){
        return bytes;
    }
    public String getString(){
        return new String(bytes, StandardCharsets.UTF_8);
    }
}

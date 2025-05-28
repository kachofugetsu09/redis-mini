package site.hnfy258.database;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.internal.Dict;

import java.util.Set;

@Getter
@Setter
public class RedisDB {
    private final Dict<RedisBytes, RedisData> data;

    private final int id;

    public RedisDB(int id) {
        this.id = id;
        this.data = new Dict<>();
    }

    public Set<RedisBytes> keys(){
        return data.keySet();
    }

    public boolean exist(RedisBytes key){
        return data.containsKey(key);
    }

    public void put(RedisBytes key, RedisData value){
        data.put(key, value);
    }

    public RedisData get(RedisBytes key){
        return data.get(key);
    }

    public int size(){
        return data.size();
    }


    public void clear() {
        data.clear();
    }
}

package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
@Setter
@Getter
public class RedisList implements RedisData{
    private volatile long timeout = -1;
    private LinkedList<RedisBytes> list;
    private RedisBytes key;

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
        if(list == null || list.size() == 0){
            return Collections.emptyList();
        }        List<Resp> lpushCommand = new ArrayList<>();
        // 🚀 优化：使用 RedisBytes 缓存 LPUSH 命令
        lpushCommand.add(new BulkString(RedisBytes.fromString("LPUSH")));
        lpushCommand.add(new BulkString(key.getBytesUnsafe()));
        for(RedisBytes value : list){
            lpushCommand.add(new BulkString(value.getBytesUnsafe()));
        }
        return Collections.singletonList(new RespArray(lpushCommand.toArray(new Resp[0])));
    }

    public int size(){
        return list.size();
    }    public void lpush(RedisBytes... values){
        for(RedisBytes value : values){
            list.addFirst(value);
        }
    }

    public RedisBytes lpop(){
        return list.pollFirst();
    }

    /**
     * 向列表右端(尾部)推入一个或多个元素
     * 
     * @param values 要推入的元素
     */
    public void rpush(RedisBytes... values){
        for(RedisBytes value : values){
            list.addLast(value);
        }
    }

    /**
     * 从列表右端(尾部)弹出一个元素
     * 
     * @return 弹出的元素，如果列表为空则返回null
     */
    public RedisBytes rpop(){
        return list.pollLast();
    }    public List<RedisBytes> lrange(int start, int stop){
        int size = list.size();
        
        // 1. 处理负数索引
        if (start < 0) {
            start = size + start;
        }
        if (stop < 0) {
            stop = size + stop;
        }
        
        // 2. 边界检查
        start = Math.max(0, start);
        stop = Math.min(size - 1, stop);

        // 3. 返回子列表
        if (start <= stop && start < size) {
            return new ArrayList<>(list.subList(start, stop + 1));
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

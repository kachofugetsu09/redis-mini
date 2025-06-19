package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
@Setter
@Getter
public class RedisList implements RedisData{    private volatile long timeout = -1;
    private final ConcurrentLinkedDeque<RedisBytes> list;
    private RedisBytes key;

    public RedisList() {
        this.list = new ConcurrentLinkedDeque<>();
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

        lpushCommand.add(new BulkString(RedisBytes.fromString("LPUSH")));
        lpushCommand.add(new BulkString(key.getBytesUnsafe()));
        for(RedisBytes value : list){
            lpushCommand.add(new BulkString(value.getBytesUnsafe()));
        }
        return Collections.singletonList(new RespArray(lpushCommand.toArray(new Resp[0])));
    }

    public int size(){
        return list.size();
    }    /**
     * 向列表左端(头部)推入一个或多个元素
     * 
     * @param values 要推入的元素
     */
    public void lpush(final RedisBytes... values) {
        for (final RedisBytes value : values) {
            list.addFirst(value);
        }
    }

    /**
     * 从列表左端(头部)弹出一个元素
     * 
     * @return 弹出的元素，如果列表为空则返回null
     */
    public RedisBytes lpop() {
        return list.pollFirst();
    }

    /**
     * 向列表右端(尾部)推入一个或多个元素
     * 
     * @param values 要推入的元素
     */
    public void rpush(final RedisBytes... values) {
        for (final RedisBytes value : values) {
            list.addLast(value);
        }
    }

    /**
     * 从列表右端(尾部)弹出一个元素
     * 
     * @return 弹出的元素，如果列表为空则返回null
     */
    public RedisBytes rpop() {
        return list.pollLast();
    }    /**
     * 获取列表指定范围内的元素
     * 
     * @param start 开始索引
     * @param stop 结束索引
     * @return 指定范围的元素列表
     */
    public List<RedisBytes> lrange(final int start, final int stop) {
        final int size = list.size();
        
        // 1. 处理负数索引
        int actualStart = start < 0 ? size + start : start;
        int actualStop = stop < 0 ? size + stop : stop;
        
        // 2. 边界检查
        actualStart = Math.max(0, actualStart);
        actualStop = Math.min(size - 1, actualStop);

        // 3. 返回子列表
        if (actualStart <= actualStop && actualStart < size) {
            final List<RedisBytes> result = new ArrayList<>();
            final RedisBytes[] array = list.toArray(new RedisBytes[0]);
            
            for (int i = actualStart; i <= actualStop && i < array.length; i++) {
                result.add(array[i]);
            }
            return result;
        }
        return Collections.emptyList();
    }

    /**
     * 移除列表中所有等于指定值的元素
     * 
     * @param key 要移除的元素
     * @return 移除的元素数量
     */
    public int remove(final RedisBytes key) {
        int count = 0;
        while (list.remove(key)) {
            count++;
        }
        return count;
    }

    /**
     * 获取列表所有元素
     * 
     * @return 所有元素的数组
     */
    public RedisBytes[] getAll() {
        return list.toArray(new RedisBytes[0]);
    }
}

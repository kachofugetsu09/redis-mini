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

/**
 * Redis列表数据结构实现
 * 
 * <p>实现了Redis的List数据类型，提供双端队列操作功能。
 * 使用LinkedList作为底层存储结构，针对单线程写入场景优化。
 * 支持从列表两端进行高效的插入和删除操作。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>左端(头部)和右端(尾部)的元素推入和弹出</li>
 *     <li>指定范围的元素获取</li>
 *     <li>元素移除和列表大小查询</li>
 *     <li>支持Redis协议的序列化转换</li>
 *     <li>支持创建安全快照用于持久化</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Setter
@Getter
public class RedisList implements RedisData {

    /** 数据过期时间，-1表示永不过期 */
    private volatile long timeout = -1;
    
    /** 底层存储结构 */
    private final LinkedList<RedisBytes> list;
    
    /** 关联的Redis键名 */
    private RedisBytes key;

    /**
     * 默认构造函数
     * 
     * <p>初始化一个空的Redis列表实例。
     */
    public RedisList() {
        this.list = new LinkedList<>();
    }

    /**
     * 获取数据过期时间
     * 
     * @return 过期时间戳，-1表示永不过期
     */
    @Override
    public long timeout() {
        return timeout;
    }

    /**
     * 设置数据过期时间
     * 
     * @param timeout 过期时间戳，-1表示永不过期
     */
    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }


    /**
     * 将列表转换为Redis协议格式
     * 
     * <p>将当前列表转换为LPUSH命令格式的RESP数组，
     * 用于持久化或网络传输。
     * 
     * @return Redis协议格式的命令列表，如果列表为空则返回空列表
     */
    @Override
    public List<Resp> convertToResp() {
        if (list == null || list.size() == 0) {
            return Collections.emptyList();
        }
        
        List<Resp> lpushCommand = new ArrayList<>();
        lpushCommand.add(new BulkString(RedisBytes.fromString("LPUSH")));
        lpushCommand.add(new BulkString(key.getBytesUnsafe()));
        
        // 反向遍历列表以保持与lpush操作相同的顺序
        for (int i = list.size() - 1; i >= 0; i--) {
            lpushCommand.add(new BulkString(list.get(i).getBytesUnsafe()));
        }
        
        return Collections.singletonList(new RespArray(lpushCommand.toArray(new Resp[0])));
    }

    /**
     * 获取列表大小
     * 
     * @return 列表中元素的数量
     */
    public  int size() {
        return list.size();
    }

    /**
     * 向列表左端(头部)推入一个或多个元素
     * 
     * @param values 要推入的元素
     */
    public void lpush(final RedisBytes... values) {
        if (values == null || values.length == 0) {
            return;
        }
        
        // 按原始顺序添加，每个新元素都会被添加到最前面
        for (RedisBytes value : values) {
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
        if (values == null || values.length == 0) {
            return;
        }
        
        for (RedisBytes value : values) {
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
    }

    /**
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
            return new ArrayList<>(list.subList(actualStart, actualStop + 1));
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
    public synchronized RedisBytes[] getAll() {
        return list.toArray(new RedisBytes[0]);
    }
}

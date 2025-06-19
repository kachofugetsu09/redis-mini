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
import java.util.Random;
import java.util.Set;

/**
 * Redis集合数据结构实现类
 * 
 * <p>实现了Redis的Set数据类型，提供无序且不重复的元素集合功能。
 * 使用线程安全的Dict作为底层存储结构，支持高效的集合操作。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>元素的添加和删除（SADD/SREM）</li>
 *     <li>随机元素弹出（SPOP）</li>
 *     <li>集合大小查询和成员检查</li>
 *     <li>支持Redis协议的序列化转换</li>
 *     <li>线程安全的并发访问</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Setter
@Getter
public class RedisSet implements RedisData {

    /** 数据过期时间，-1表示永不过期 */
    private volatile long timeout = -1;
    
    /** 底层集合存储结构，使用线程安全的Dict，值统一为null */
    private Dict<RedisBytes, Object> setCore;
    
    /** 关联的Redis键名 */
    private RedisBytes key;

    /**
     * 默认构造函数
     * 
     * <p>初始化一个空的Redis集合实例。
     */
    public RedisSet() {
        this.setCore = new Dict<>();
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
     * 将集合转换为Redis协议格式
     * 
     * <p>将集合中的所有元素转换为SADD命令格式的RESP数组，
     * 用于持久化或网络传输。优化了单个命令包含所有元素的方式。
     * 
     * @return Redis协议格式的命令列表，如果集合为空则返回空列表
     */
    @Override
    public List<Resp> convertToResp() {
        List<Resp> result = new ArrayList<>();
        if (setCore.size() == 0) {
            return Collections.emptyList();
        }
        
        List<Resp> saddCommand = new ArrayList<>();
        // 使用 RedisBytes 缓存 SADD 命令，提高性能
        saddCommand.add(new BulkString(RedisBytes.fromString("SADD")));
        saddCommand.add(new BulkString(key.getBytesUnsafe()));
        
        // 将所有成员添加到单个SADD命令中
        for (RedisBytes member : setCore.keySet()) {
            saddCommand.add(new BulkString(member.getBytesUnsafe()));
        }
        
        result.add(new RespArray(saddCommand.toArray(new Resp[0])));
        return result;
    }    /** 用于标记集合中元素存在的常量值 */
    private static final Object PRESENT = new Object();

    /**
     * 向集合添加一个或多个成员
     * 
     * @param members 要添加的成员列表
     * @return 成功添加的新成员数量
     */
    public int add(List<RedisBytes> members) {
        int count = 0;
        for (RedisBytes member : members) {
            // 使用put的返回值判断是否为新元素
            // 返回null表示key不存在，是新插入的元素
            Object oldValue = setCore.put(member, PRESENT);
            if (oldValue == null) {
                count++;
            }
        }
        return count;
    }/**
     * 从集合中移除指定成员
     * 
     * @param member 要移除的成员
     * @return 如果成员存在并被移除返回1，否则返回0
     */
    public int remove(RedisBytes member) {
        // 使用remove的返回值判断元素是否存在
        // 返回非null表示元素存在并被移除
        Object removedValue = setCore.remove(member);
        return (removedValue != null) ? 1 : 0;
    }    /**
     * 随机弹出指定数量的集合成员
     * 
     * <p>从集合中随机选择并移除指定数量的成员。
     * 如果请求的数量超过集合大小，则弹出所有成员。
     * 
     * @param count 要弹出的成员数量
     * @return 被弹出的成员列表，如果集合为空则返回空列表
     */
    public List<RedisBytes> pop(int count) {
        if (count <= 0) {
            return Collections.emptyList();
        }

        // 1. 获取当前所有键的快照，避免并发修改异常
        Set<RedisBytes> keySet = setCore.keySet();
        RedisBytes[] keys = keySet.toArray(new RedisBytes[0]);
        
        if (keys.length == 0) {
            return Collections.emptyList();
        }

        // 2. 限制弹出数量不超过实际元素数量
        count = Math.min(count, keys.length);
        List<RedisBytes> poppedElements = new ArrayList<>(count);

        Random random = new Random();
        
        // 3. 随机弹出元素
        for (int i = 0; i < count && keys.length > 0; i++) {
            int randomIndex = random.nextInt(keys.length);
            RedisBytes member = keys[randomIndex];
            
            // 4. 尝试移除元素（可能已被其他操作移除）
            if (setCore.remove(member) != null) {
                poppedElements.add(member);
            }
            
            // 5. 从数组中移除已选择的元素，避免重复选择
            RedisBytes[] newKeys = new RedisBytes[keys.length - 1];
            System.arraycopy(keys, 0, newKeys, 0, randomIndex);
            System.arraycopy(keys, randomIndex + 1, newKeys, randomIndex, 
                           keys.length - randomIndex - 1);
            keys = newKeys;
        }
        
        return poppedElements;
    }

    /**
     * 获取集合大小
     * 
     * @return 集合中成员的数量
     */
    public int size() {
        return setCore.size();
    }    /**
     * 获取集合中的所有成员
     * 
     * <p>使用动态数组来避免并发读写时的TOCTOU问题。
     * 在持久化等场景下，可能会有后台线程读取数据的同时，
     * 主线程在修改数据结构。
     * 
     * @return 包含所有成员的数组
     */
    public RedisBytes[] getAll() {
        // 1. 使用动态列表收集所有键
        Set<RedisBytes> keySet = setCore.keySet();
        
        // 2. 直接使用keySet的toArray方法，这是线程安全的快照
        return keySet.toArray(new RedisBytes[0]);
    }
}

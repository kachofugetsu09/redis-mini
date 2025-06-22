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

/**
 * Redis字符串数据结构实现类
 * 
 * <p>实现了Redis的String数据类型，是Redis中最基本的数据结构。
 * 使用高效的SDS（Simple Dynamic String）作为底层存储，
 * 支持字符串的基本操作以及数值递增等功能。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>字符串的存储和获取</li>
 *     <li>数值的递增操作（INCR）</li>
 *     <li>过期时间管理</li>
 *     <li>支持Redis协议的序列化转换</li>
 *     <li>内存优化的缓存机制</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Setter
@Getter
public class RedisString implements RedisData {

    /** 数据过期时间，-1表示永不过期 */
    private volatile long timeout;
    
    /** 底层SDS字符串存储结构 */
    private Sds value;
    
    /** 关联的Redis键名 */
    private RedisBytes key;
    
    /** 缓存的RedisBytes值，避免重复转换 */
    private RedisBytes cachedValue;

    /**
     * 构造函数
     * 
     * @param value SDS字符串值
     */
    public RedisString(Sds value) {
        this.value = value;
        this.timeout = -1;
        this.cachedValue = null;
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
     * 将字符串转换为Redis协议格式
     * 
     * <p>将当前字符串转换为SET命令格式的RESP数组，
     * 用于持久化或网络传输。
     * 
     * @return Redis协议格式的命令列表，如果值为空则返回空列表
     */
    @Override
    public List<Resp> convertToResp() {
        if (value == null) {
            return Collections.emptyList();
        }        
        List<Resp> setCommand = new ArrayList<>();
        setCommand.add(BulkString.SET);  // 使用预分配的常量
        setCommand.add(BulkString.wrapTrusted(key.getBytesUnsafe()));
        setCommand.add(BulkString.wrapTrusted(value.getBytes()));
        
        return Collections.singletonList(new RespArray(setCommand.toArray(new Resp[0])));
    }

    /**
     * 获取字符串值的RedisBytes表示
     * 
     * <p>使用缓存机制避免重复转换，提高性能。
     * 
     * @return 字符串的RedisBytes表示
     */
    public  RedisBytes getValue() {
        if (cachedValue == null) {
            cachedValue = new RedisBytes(value.getBytes());
        }
        return cachedValue;
    }

    /**
     * 获取内部SDS对象的直接引用
     * 
     * <p>用于高效的字符串操作如append、length等。
     * 
     * @return SDS对象引用
     */
    public  Sds getSds() {
        return value;
    }

    /**
     * 设置内部SDS对象
     * 
     * @param sds 新的SDS对象
     */
    public  void setSds(Sds sds) {
        this.value = sds;
        this.cachedValue = null;
    }

    /**
     * 对字符串值执行递增操作
     * 
     * <p>将字符串解析为整数并加1，如果字符串不是有效的整数则抛出异常。
     * 
     * @return 递增后的新值
     * @throws IllegalStateException 如果字符串不是有效的数字
     */
    public  long incr() {
        try {
            // 1. 解析当前值为长整型
            long cur = Long.parseLong(value.toString());
            
            // 2. 执行递增操作
            long newValue = cur + 1;
            
            // 3. 更新内部值并清除缓存
            value = Sds.create(String.valueOf(newValue).getBytes());
            cachedValue = null;
            
            return newValue;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("value is not a number");
        }
    }


}

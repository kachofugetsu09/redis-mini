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
import java.util.Map;

/**
 * Redis哈希表数据结构实现类
 * 
 * <p>实现了Redis的Hash数据类型，提供高效的键值对存储功能。
 * 使用线程安全的Dict作为底层存储结构，支持字段的增删改查操作。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>字段的设置和获取（HSET/HGET）</li>
 *     <li>字段的批量删除（HDEL）</li>
 *     <li>哈希表大小查询</li>
 *     <li>支持Redis协议的序列化转换</li>
 *     <li>线程安全的并发访问</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Setter
@Getter
public class RedisHash implements RedisData {

    /** 数据过期时间，-1表示永不过期 */
    private volatile long timeout = -1;
    
    /** 底层哈希表存储结构，使用线程安全的Dict */
    private Dict<RedisBytes, RedisBytes> hash;
    
    /** 关联的Redis键名 */
    private RedisBytes key;

    /**
     * 默认构造函数
     * 
     * <p>初始化一个空的Redis哈希表实例。
     */
    public RedisHash() {
        this.hash = new Dict<>();
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
    }    /**
     * 将哈希表转换为Redis协议格式
     * 
     * <p>将哈希表中的所有字段转换为HSET命令格式的RESP数组列表，
     * 用于持久化或网络传输。
     * 
     * @return Redis协议格式的命令列表，如果哈希表为空则返回空列表
     */
    @Override
    public List<Resp> convertToResp() {
        if (hash == null || hash.size() == 0) {
            return Collections.emptyList();
        }
        
        List<Resp> result = new ArrayList<>();
        
        // 1. 遍历所有字段，为每个字段生成HSET命令
        for (Map.Entry<Object, Object> entry : hash.entrySet()) {
            Object field = entry.getKey();
            Object value = entry.getValue();
            
            List<Resp> hsetCommand = new ArrayList<>();
            hsetCommand.add(new BulkString(RedisBytes.fromString("HSET")));
            hsetCommand.add(new BulkString(key.getBytesUnsafe()));
            
            // 2. 处理字段名
            if (field instanceof RedisBytes) {
                hsetCommand.add(new BulkString(((RedisBytes) field).getBytesUnsafe()));
            } else {
                hsetCommand.add(new BulkString(field.toString().getBytes()));
            }
            
            // 3. 处理字段值
            if (value instanceof RedisBytes) {
                hsetCommand.add(new BulkString(((RedisBytes) value).getBytesUnsafe()));
            } else {
                hsetCommand.add(new BulkString(value.toString().getBytes()));
            }
            
            result.add(new RespArray(hsetCommand.toArray(new Resp[0])));
        }
        
        return result;    }

    /**
     * 设置哈希表字段值
     * 
     * @param field 字段名
     * @param value 字段值
     * @return 如果是新字段返回1，如果是更新已存在字段返回0
     */
    public int put(RedisBytes field, RedisBytes value) {
        return hash.put(field, value) == null ? 1 : 0;
    }

    /**
     * 获取底层哈希表
     * 
     * @return 底层Dict实例
     */
    public Dict<RedisBytes, RedisBytes> getHash() {
        return hash;
    }

    /**
     * 删除哈希表中的指定字段
     * 
     * @param fields 要删除的字段列表
     * @return 成功删除的字段数量
     */
    public int del(List<RedisBytes> fields) {
        return (int) fields.stream()
                .filter(field -> hash.remove(field) != null)
                .count();
    }
}

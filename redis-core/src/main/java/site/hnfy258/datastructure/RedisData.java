package site.hnfy258.datastructure;

import site.hnfy258.protocal.Resp;

import java.util.List;

/**
 * Redis数据结构基础接口
 * 
 * <p>定义了所有Redis数据类型必须实现的基本功能，包括过期时间管理
 * 和Redis协议转换能力。所有Redis数据结构（String、List、Set、Hash、Zset）
 * 都需要实现此接口。
 * 
 * <p>核心功能：
 * <ul>
 *     <li>过期时间的获取和设置</li>
 *     <li>数据结构到Redis协议的转换</li>
 *     <li>为持久化和网络传输提供统一接口</li>
 * </ul>
 * 
 * <p>线程安全性：
 * <ul>
 *     <li>所有客户端命令在单个线程中串行执行</li>
 *     <li>后台持久化线程通过底层数据结构(Dict)的线程安全机制获取数据</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public interface RedisData {

    /**
     * 获取数据过期时间
     * 
     * @return 过期时间戳（毫秒），-1表示永不过期
     */
    long timeout();

    /**
     * 设置数据过期时间
     * 
     * @param timeout 过期时间戳（毫秒），-1表示永不过期
     */
    void setTimeout(long timeout);

    /**
     * 将数据结构转换为Redis协议格式
     * 
     * <p>将当前数据结构转换为对应的Redis命令序列，
     * 用于AOF持久化、RDB快照或网络传输。
     * 
     * @return Redis协议格式的命令列表，如果数据为空则返回空列表
     */
    List<Resp> convertToResp();
}

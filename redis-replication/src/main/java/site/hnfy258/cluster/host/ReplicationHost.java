package site.hnfy258.cluster.host;

import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.Resp;

/**
 * 复制主机接口 - 简化版本，只包含复制模块真正需要的功能
 * 
 * <p>依赖倒置原则：
 * 复制模块定义它需要的最小服务接口，由上层模块（server层）来实现。
 * 这样避免了复制模块对server层的直接依赖。</p>
 * 
 * @author Redis Team
 * @since 1.0.0
 */
public interface ReplicationHost {
    
    /**
     * 获取Redis核心数据接口
     * 用于复制模块访问数据和执行命令
     * 
     * @return Redis核心接口实例
     */
    RedisCore getRedisCore();
    
    /**
     * 执行命令并返回结果
     * 用于从节点执行从主节点接收到的复制命令
     * 
     * @param command 要执行的命令
     * @return 命令执行结果
     */
    Resp executeCommand(final Resp command);
    
    /**
     * 生成RDB快照数据
     * 用于全量同步时发送给从节点
     * 
     * @return RDB数据字节数组
     * @throws Exception 生成失败时抛出异常
     */
    byte[] generateRdbSnapshot() throws Exception;
    
    /**
     * 获取当前节点的复制ID
     * 用于复制握手和部分重同步验证
     * 
     * @return 节点复制ID
     */
    String getReplicationId();
    
    /**
     * 获取当前复制偏移量
     * 用于复制进度跟踪
     * 
     * @return 复制偏移量
     */
    long getReplicationOffset();
    
    /**
     * 设置复制偏移量
     * 用于更新复制进度
     * 
     * @param offset 新的复制偏移量
     */
    void setReplicationOffset(final long offset);
}

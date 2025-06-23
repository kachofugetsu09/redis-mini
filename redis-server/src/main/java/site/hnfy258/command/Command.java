package site.hnfy258.command;

import site.hnfy258.protocal.Resp;

/**
 * Redis命令接口，定义了所有Redis命令的基本行为。
 * 
 * <p>该接口是Redis命令系统的核心抽象，主要职责包括：
 * <ul>
 *   <li>定义命令的基本操作规范
 *   <li>提供命令执行的统一接口
 *   <li>支持命令的类型识别
 *   <li>管理命令的执行上下文
 *   <li>区分读写命令类型
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>采用命令模式解耦调用者和实现者
 *   <li>支持命令的动态扩展
 *   <li>实现命令的参数验证
 *   <li>提供命令执行的生命周期管理
 *   <li>支持命令的权限控制
 * </ul>
 * 
 * <p>实现要求：
 * <ul>
 *   <li>所有命令实现类必须是线程安全的
 *   <li>命令执行应该是原子性的
 *   <li>命令应该处理好异常情况
 *   <li>写命令需要考虑持久化
 *   <li>读命令需要考虑一致性
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
public interface Command {
    
    /**
     * 获取命令类型。
     * 
     * <p>命令类型用于：
     * <ul>
     *   <li>命令的分类管理
     *   <li>权限控制
     *   <li>统计分析
     *   <li>性能优化
     * </ul>
     * 
     * @return 命令类型枚举值
     */
    CommandType getType();
    
    /**
     * 设置命令执行的上下文。
     * 
     * <p>上下文包含：
     * <ul>
     *   <li>命令的参数列表
     *   <li>执行环境信息
     *   <li>客户端状态
     *   <li>会话数据
     * </ul>
     * 
     * @param array RESP协议格式的参数数组
     */
    void setContext(Resp[] array);
    
    /**
     * 执行命令并返回结果。
     * 
     * <p>执行过程：
     * <ul>
     *   <li>参数解析和验证
     *   <li>业务逻辑处理
     *   <li>结果格式化
     *   <li>错误处理
     * </ul>
     * 
     * @return RESP协议格式的执行结果
     */
    Resp handle();
    
    /**
     * 判断是否为写命令。
     * 
     * <p>用于：
     * <ul>
     *   <li>区分读写操作
     *   <li>控制持久化策略
     *   <li>主从复制控制
     *   <li>性能优化
     * </ul>
     * 
     * @return 如果是写命令返回true，读命令返回false
     */
    boolean isWriteCommand();
}

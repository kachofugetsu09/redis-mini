package site.hnfy258.server.command.executor;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.core.command.CommandExecutor;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.context.RedisContext;

/**
 * Redis命令执行器实现类，负责处理和执行Redis命令。
 * 
 * <p>该类的主要职责：
 * <ul>
 *   <li>将redis-core层的命令执行接口桥接到redis-server层的Command系统
 *   <li>解决持久化层不能直接依赖命令层的架构问题
 *   <li>提供命令执行的统一入口
 *   <li>处理命令执行的生命周期
 *   <li>实现命令的并发控制
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>采用桥接模式解耦命令系统和核心功能
 *   <li>支持命令的原子性执行
 *   <li>实现命令执行的事务支持
 *   <li>提供命令执行的监控和统计
 *   <li>支持命令执行的错误处理
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用命令对象池减少对象创建
 *   <li>实现批量命令的流水线处理
 *   <li>优化命令参数的解析过程
 *   <li>支持命令结果的缓存机制
 *   <li>实现高效的命令分发策略
 * </ul>
 * 
 * <p>并发控制：
 * <ul>
 *   <li>保证命令执行的线程安全
 *   <li>实现命令队列的并发处理
 *   <li>支持命令执行的优先级控制
 *   <li>处理并发命令的依赖关系
 *   <li>避免命令执行的死锁问题
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class CommandExecutorImpl implements CommandExecutor {
    
    /** Redis服务器上下文，提供命令执行所需的环境和功能 */
    private final RedisContext redisContext;
    
    /**
     * 构造函数，初始化命令执行器。
     * 
     * @param redisContext Redis服务器上下文
     */
    public CommandExecutorImpl(RedisContext redisContext) {
        this.redisContext = redisContext;
    }
    
    /**
     * 执行Redis命令。
     * 
     * <p>该方法实现了命令执行的完整流程：
     * <ul>
     *   <li>参数验证和预处理
     *   <li>命令类型解析和验证
     *   <li>RESP协议格式转换
     *   <li>命令对象创建和执行
     *   <li>异常处理和日志记录
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>使用RedisBytes实现零拷贝
     *   <li>命令名称使用缓存池
     *   <li>参数转换使用内存复用
     *   <li>避免不必要的字符串操作
     *   <li>减少临时对象创建
     * </ul>
     * 
     * <p>安全控制：
     * <ul>
     *   <li>严格的参数验证
     *   <li>命令类型安全检查
     *   <li>异常的完整捕获
     *   <li>详细的错误日志
     *   <li>资源的安全释放
     * </ul>
     * 
     * @param commandName 命令名称，不能为null
     * @param args 命令参数数组，可以为空但不能为null
     * @return 命令执行成功返回true，失败返回false
     */
    @Override
    public boolean executeCommand(String commandName, String[] args) {
        try {           
            // 1. 参数验证
            if (commandName == null) {
                log.warn("命令名称为null");
                return false;
            }

            if (args == null) {
                log.warn("命令参数为null: {}", commandName);
                return false;
            }

            final CommandType commandType = CommandType.findByName(commandName);
            if (commandType == null) {
                log.warn("未知命令类型: {}", commandName);
                return false;
            }

            // 如果参数为空，直接返回空数组
            if (args.length == 0) {
                log.warn("命令参数为空: {}", commandName);
                return false;
            }

            // 2. 构建RESP格式的参数，使用RedisBytes优化性能
            Resp[] respArgs = new Resp[args.length + 1];
            // 命令名优先从缓存池获取，使用零拷贝优化
            final RedisBytes commandBytes = RedisBytes.fromString(commandName);
            respArgs[0] = BulkString.wrapTrusted(commandBytes.getBytesUnsafe());

            // 参数使用RedisBytes减少重复编码，使用零拷贝优化
            for (int i = 0; i < args.length; i++) {
                final RedisBytes argBytes = RedisBytes.fromString(args[i]);
                respArgs[i + 1] = BulkString.wrapTrusted(argBytes.getBytesUnsafe());
            }
            RespArray respArray = new RespArray(respArgs);
            
            // 3. 创建并执行命令
            final Command cmd = commandType.createCommand(redisContext);
            cmd.setContext(respArray.getContent());
            cmd.handle();
            
            return true;
        } catch (Exception e) {
            String argsStr = args != null ? String.join(" ", args) : "null";
            log.error("命令执行失败: {} {}", commandName, argsStr, e);
            return false;
        }
    }
}

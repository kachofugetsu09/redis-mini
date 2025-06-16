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
 * 命令执行器实现类
 * 
 * 用于将redis-core层的命令执行接口桥接到redis-server层的Command系统
 * 解决持久化层不能直接依赖命令层的架构问题
 * 
 * @author hnfy258
 */
@Slf4j
public class CommandExecutorImpl implements CommandExecutor {
    
    private final RedisContext redisContext;
    
    public CommandExecutorImpl(RedisContext redisContext) {
        this.redisContext = redisContext;
    }
    
    @Override
    public boolean executeCommand(String commandName, String[] args) {
        try {           

            final CommandType commandType = CommandType.findByName(commandName);
            if (commandType == null) {
                log.warn("未知命令类型: {}", commandName);
                return false;
            }            // 2. 构建RESP格式的参数，使用RedisBytes优化性能
            Resp[] respArgs = new Resp[args.length + 1];
            
            // 命令名优先从缓存池获取
            final RedisBytes commandBytes = RedisBytes.fromString(commandName);
            respArgs[0] = new BulkString(commandBytes.getBytesUnsafe());
            
            // 参数使用RedisBytes减少重复编码
            for (int i = 0; i < args.length; i++) {
                final RedisBytes argBytes = RedisBytes.fromString(args[i]);
                respArgs[i + 1] = new BulkString(argBytes.getBytesUnsafe());
            }
            RespArray respArray = new RespArray(respArgs);
            
            // 3. 创建并执行命令
            final Command cmd = commandType.createCommand(redisContext);
            cmd.setContext(respArray.getContent());
            cmd.handle();
            
            return true;
        } catch (Exception e) {
            log.error("命令执行失败: {} {}", commandName, String.join(" ", args), e);
            return false;
        }
    }
}

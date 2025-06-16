package site.hnfy258.core.command;

/**
 * 命令执行器接口
 * 
 * 用于解耦持久化层和命令层：
 * - redis-core 定义接口
 * - redis-server 实现接口
 * - redis-persistence 通过接口执行命令
 * 
 * @author hnfy258
 */
public interface CommandExecutor {
    
    /**
     * 执行Redis命令
     * 
     * @param commandName 命令名称（如 SET, GET 等）
     * @param args 命令参数
     * @return 是否执行成功
     */
    boolean executeCommand(String commandName, String[] args);
}

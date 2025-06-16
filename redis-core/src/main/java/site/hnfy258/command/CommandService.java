package site.hnfy258.command;

/**
 * 命令执行服务抽象接口
 * 
 * 为持久化层提供命令执行能力，避免循环依赖
 * 
 * @author hnfy258
 * @since 1.0
 */
public interface CommandService {
    
    /**
     * 执行命令字节数组
     * 
     * @param commandBytes 命令字节数组
     * @return 执行结果
     */
    Object executeCommand(byte[] commandBytes);
    
    /**
     * 执行命令字符串
     * 
     * @param command 命令字符串
     * @return 执行结果
     */
    Object executeCommand(String command);
    
    /**
     * 执行命令参数数组
     * 
     * @param args 命令参数数组
     * @return 执行结果
     */
    Object executeCommand(String[] args);
    
    /**
     * 检查命令是否为写命令
     * 
     * @param command 命令名称
     * @return true如果是写命令，false如果是只读命令
     */
    boolean isWriteCommand(String command);
}

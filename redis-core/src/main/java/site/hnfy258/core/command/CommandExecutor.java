package site.hnfy258.core.command;

/**
 * Redis命令执行器接口
 * 
 * <p>定义了Redis命令执行的标准接口，用于解耦不同模块之间的依赖关系：
 * <ul>
 *     <li>redis-core模块：定义接口标准</li>
 *     <li>redis-server模块：提供具体实现</li>
 *     <li>redis-persistence模块：通过接口执行命令重放</li>
 * </ul>
 * 
 * <p>这种设计模式实现了依赖倒置原则，使得持久化层可以独立于具体的命令实现，
 * 提高了系统的可测试性和可维护性。
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public interface CommandExecutor {

    /**
     * 执行Redis命令
     * 
     * <p>根据命令名称和参数执行相应的Redis操作。
     * 主要用于AOF文件加载、RDB恢复等场景下的命令重放。
     * 
     * @param commandName 命令名称（如SET、GET、LPUSH等）
     * @param args 命令参数数组
     * @return 如果命令执行成功返回true，否则返回false
     */
    boolean executeCommand(String commandName, String[] args);
}

package site.hnfy258.command.impl.aof;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.core.RedisCore;
import site.hnfy258.server.context.RedisContext;

@Slf4j
public class Bgsave implements Command {
    private RedisContext redisContext;  // 新增：RedisContext支持

    
    /**
     * 新增构造函数：支持RedisContext的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param redisContext Redis统一上下文，提供所有核心功能的访问接口
     */
    public Bgsave(final RedisContext redisContext) {
        this.redisContext = redisContext;

        
        log.debug("Bgsave命令使用RedisContext模式初始化");
    }
    @Override
    public CommandType getType() {
        return CommandType.BGSAVE;
    }

    @Override
    public void setContext(Resp[] array) {

    }    @Override
    public Resp handle() {
        // 使用新的兼容方式执行RDB保存，避免循环依赖
        final boolean result = executeRdbSave();
        if (result) {
            return new SimpleString("OK");
        }
        return new Errors("RDB持久化失败");
    }
      /**
     * 执行RDB保存操作
     * 支持新旧两种模式的兼容性，避免循环依赖
     * 
     * @return 保存是否成功
     */
    private boolean executeRdbSave() {
        // 1. 优先使用RedisContext模式（避免循环依赖）
        if (redisContext != null) {
            return redisContext.saveRdb();
        }
        
        // 2. RedisCore模式已移除循环依赖，无法再获取RdbManager
        // 必须使用RedisContext模式
        log.error("无法执行RDB保存：请使用RedisContext模式的Bgsave命令");
        return false;
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

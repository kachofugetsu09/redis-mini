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
public class Bgrewriteaof implements Command {
    private RedisContext redisContext;  // 新增：RedisContext支持

    
    /**
     * 新增构造函数：支持RedisContext的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param redisContext Redis统一上下文，提供所有核心功能的访问接口
     */
    public Bgrewriteaof(final RedisContext redisContext) {
        this.redisContext = redisContext;
        log.debug("Bgrewriteaof命令使用RedisContext模式初始化");
    }
    @Override
    public CommandType getType() {
        return CommandType.BGREWRITEAOF;
    }

    @Override
    public void setContext(Resp[] array) {

    }    @Override
    public Resp handle() {
        try {
            // 使用新的兼容方式执行AOF重写，避免循环依赖
            final boolean result = executeAofRewrite();
            if (result) {
                return new SimpleString("Background append only file rewriting started");
            }
            return new Errors("Background append only file rewriting failed");
        } catch (Exception e) {
            log.error("AOF重写执行异常: {}", e.getMessage(), e);
            return new Errors("Background append only file rewriting failed");
        }
    }
      /**
     * 执行AOF重写操作
     * 支持新旧两种模式的兼容性，避免循环依赖
     * 
     * @return 重写是否成功
     */    private boolean executeAofRewrite() {
        // 1. 优先使用RedisContext的直接接口（推荐）
        if (redisContext != null) {
            return redisContext.rewriteAof();
        }        
        // 2. 原有模式已移除循环依赖，必须使用RedisContext
        log.error("无法执行AOF重写：请使用RedisContext模式并扩展相关接口");
        return false;
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
}

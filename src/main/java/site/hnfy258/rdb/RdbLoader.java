package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.server.context.RedisContext;

import java.io.*;

/**
 * RDB加载器 - 负责从RDB文件加载Redis数据
 * 
 * <p>重构说明：原依赖RedisCore，现改为依赖RedisContext，实现彻底解耦</p>
 */
@Slf4j
public class RdbLoader {
    // ========== 核心依赖：使用RedisContext替代RedisCore ==========
    private final RedisContext redisContext;

    /**
     * 构造函数：基于RedisContext的解耦架构
     * 
     * @param redisContext Redis统一上下文
     */
    public RdbLoader(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    public boolean loadRdb(File file){
        if(!file.exists()) {
            log.info("RDB文件不存在: {}", file.getAbsolutePath());
            return true;
        }
        try(DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))){
            //1.检查头部
            if(!RdbUtils.checkRdbHeader(dis)){
                log.info("RDB文件头不正确");
                return false;
            }
            //2.读取所有数据库
            loadAllDatabases(dis);
        }catch(Exception e){
            log.error("RDB文件加载失败", e);
            return false;
        }
        return true;
    }

    private void loadAllDatabases(DataInputStream dis) throws IOException {
        int currentDbIndex = 0;
        while(true){
            int type = dis.read();
            if(type == -1 || (byte)type == RdbConstants.RDB_OPCODE_EOF){
                break;
            }
            switch((byte)type){                case RdbConstants.RDB_OPCODE_SELECTDB:
                    currentDbIndex = dis.read();
                    redisContext.getRedisCore().selectDB(currentDbIndex);
                    log.info("选择数据库: {}", currentDbIndex);
                    break;
                case RdbConstants.STRING_TYPE:
                    RdbUtils.loadString(dis, redisContext.getRedisCore(), currentDbIndex);
                    break;
                case RdbConstants.LIST_TYPE:
                    RdbUtils.loadList(dis, redisContext.getRedisCore(), currentDbIndex);
                    break;
                case RdbConstants.SET_TYPE:
                    RdbUtils.loadSet(dis, redisContext.getRedisCore(), currentDbIndex);
                    break;
                case RdbConstants.HASH_TYPE:
                    RdbUtils.loadHash(dis, redisContext.getRedisCore(), currentDbIndex);
                    break;
                case RdbConstants.ZSET_TYPE:
                    RdbUtils.loadZSet(dis, redisContext.getRedisCore(), currentDbIndex);
                    break;

                default:
                    log.warn("不支持的类型: {}", type);
                    break;
            }
        }
    }

}

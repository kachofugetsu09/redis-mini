package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;

import java.io.*;

/**
 * RDB加载器 - 负责从RDB文件加载Redis数据
 * 
 * <p>重构说明：原依赖RedisCore，现改为依赖RedisCore接口，实现彻底解耦</p>
 */
@Slf4j
public class RdbLoader {
    // ========== 核心依赖：使用RedisCore接口实现解耦 ==========
    private final RedisCore redisCore;

    /**
     * 构造函数：基于RedisCore接口的解耦架构
     * 
     * @param redisCore Redis核心接口
     */
    public RdbLoader(RedisCore redisCore) {
        this.redisCore = redisCore;
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
            }            switch((byte)type){
                case RdbConstants.RDB_OPCODE_SELECTDB:
                    currentDbIndex = dis.read();
                    redisCore.selectDB(currentDbIndex);
                    log.info("选择数据库: {}", currentDbIndex);
                    break;
                case RdbConstants.STRING_TYPE:
                    RdbUtils.loadString(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.LIST_TYPE:
                    RdbUtils.loadList(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.SET_TYPE:
                    RdbUtils.loadSet(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.HASH_TYPE:
                    RdbUtils.loadHash(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.ZSET_TYPE:
                    RdbUtils.loadZSet(dis, redisCore, currentDbIndex);
                    break;

                default:
                    log.warn("不支持的类型: {}", type);
                    break;
            }
        }
    }

}

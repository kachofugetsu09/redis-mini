package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.server.core.RedisCore;

import java.io.*;

@Slf4j
public class RdbLoader {
    private RedisCore redisCore;

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
            }
            switch((byte)type){
                case RdbConstants.RDB_OPCODE_SELECTDB:
                    currentDbIndex = dis.read();
                    redisCore.selectDB(currentDbIndex);
                    log.info("选择数据库: {}", currentDbIndex);
                    break;
                case RdbConstants.STRING_TYPE:
                    RdbUtils.loadString(dis, redisCore,currentDbIndex);
                    break;
                case RdbConstants.LIST_TYPE:
                    RdbUtils.loadList(dis, redisCore,currentDbIndex);
                    break;
                case RdbConstants.SET_TYPE:
                    RdbUtils.loadSet(dis, redisCore,currentDbIndex);
                    break;
                case RdbConstants.HASH_TYPE:
                    RdbUtils.loadHash(dis, redisCore,currentDbIndex);
                    break;
                case RdbConstants.ZSET_TYPE:
                    RdbUtils.loadZSet(dis, redisCore,currentDbIndex);
                    break;

                default:
                    log.warn("不支持的类型: {}", type);
                    break;
            }
        }
    }

}

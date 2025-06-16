package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.*;

import java.io.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RDB写入器 - 负责将Redis数据持久化到RDB文件
 * 
 * <p>重构说明：原依赖RedisCore，现改为依赖RedisCore接口，实现彻底解耦</p>
 */
@Slf4j
public class RdbWriter {
    // ========== 核心依赖：使用RedisCore接口实现解耦 ==========
    private final RedisCore redisCore;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 构造函数：基于RedisCore接口的解耦架构
     * 
     * @param redisCore Redis核心接口
     */
    public RdbWriter(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public boolean writeRdb(String fileName) {
        if (running.get()) {
            return false;
        }
        running.set(true);
        File tempFile = new File(fileName);
        try(DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))){
            //1.写文件头
            RdbUtils.writeRdbHeader(dos);
            //2.写所有数据库
            saveAllDatabases(dos);
            //3.写文件末
            RdbUtils.writeRdbFooter(dos);
        }catch(Exception e){
            log.error("RDB写入失败", e);
            return false;
        }
        running.set(false);
        return true;
    }    private void saveAllDatabases(DataOutputStream dos) throws IOException {
        RedisDB[] databases = redisCore.getDataBases();
        for(RedisDB db : databases){
           log.info("正在保存数据库: {}", db.getId());
           if(db.size() > 0){
               writeDb(dos,db);
           }
        }
    }

    private void writeDb(DataOutputStream dos, RedisDB db) throws IOException {
        //1.写数据库id
        int databaseId = db.getId();
        RdbUtils.writeSelectDB(dos, databaseId);
        //2.写数据库数据
        for(Map.Entry<Object,Object> entry : db.getData().entrySet()){
            RedisBytes key = (RedisBytes)entry.getKey();
            RedisData value = (RedisData)entry.getValue();
            rdbSaveObject(dos,key,value);
        }
    }

    private void rdbSaveObject(DataOutputStream dos, RedisBytes key, RedisData value) throws IOException {
        switch(value.getClass().getSimpleName()){
            case "RedisString":
                RdbUtils.saveString(dos,key,(RedisString)value);
                log.info("保存字符串: {}", key);
                break;
            case "RedisList":
                RdbUtils.saveList(dos,key,(RedisList)value);
                log.info("保存列表: {}", key);
                break;
            case "RedisSet":
                RdbUtils.saveSet(dos,key,(RedisSet)value);
                log.info("保存集合: {}", key);
                break;
            case "RedisHash":
                RdbUtils.saveHash(dos,key,(RedisHash)value);
                log.info("保存哈希: {}", key);
                break;
            case "RedisZset":
                RdbUtils.saveZset(dos,key,(RedisZset)value);
                log.info("保存有序集合: {}", key);
                break;
        }
    }

    public void close() {
        if (running.get()) {
            running.set(false);
        }
    }
}

package site.hnfy258.core;

import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.Set;

public interface RedisCore {
    Set<RedisBytes> keys();
    void put(RedisBytes key, RedisData value);
    RedisData get(RedisBytes key);
    void selectDB(int dbIndex);
    int getDBNum();
    int getCurrentDBIndex();

    RedisDB[] getDataBases();

    void flushAll();

    boolean delete(RedisBytes key);
    
    /**
     * 执行Redis命令，用于AOF加载等场景
     * @param commandName 命令名称
     * @param args 命令参数
     * @return 是否执行成功
     */
    boolean executeCommand(String commandName, String[] args);
}

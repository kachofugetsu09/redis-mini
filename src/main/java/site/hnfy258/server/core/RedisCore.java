package site.hnfy258.server.core;

import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.Set;

public interface RedisCore {
    Set<RedisBytes> keys();
    void put(RedisBytes key, RedisData value);
    RedisData get(RedisBytes key);    void selectDB(int dbIndex);
    int getDBNum();
    int getCurrentDBIndex();

    RedisDB[] getDataBases();

    void flushAll();

    boolean delete(RedisBytes key);
}

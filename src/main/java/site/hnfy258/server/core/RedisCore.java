package site.hnfy258.server.core;

import site.hnfy258.datastructure.RedisData;

import java.util.Set;

public interface RedisCore {
    Set<byte[]> keys();
    void put(byte[] key, RedisData value);
    RedisData get(byte[] key);
    void selectDB(int dbIndex);
    int getDBNum();
    int getCurrentDBIndex();
}

package site.hnfy258.server.core;

import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.server.RedisServer;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RedisCoreImpl implements RedisCore{
    private final List<RedisDB> databases;
    private RedisServer server;

    private final int dbNum;
    private int currentDBIndex =0;

    public RedisCoreImpl(int dbNum,RedisServer server) {
        this.dbNum = dbNum;
        this.server = server;
        this.databases = new java.util.ArrayList<>(dbNum);
        for(int i =0; i < dbNum; i++){
            databases.add(new RedisDB(i));
        }
    }

    @Override
    public Set<RedisBytes> keys() {
        int dbIndex = getCurrentDBIndex();
        RedisDB db = databases.get(dbIndex);
        return db.keys();
    }

    @Override
    public void put(RedisBytes key, RedisData value) {
        RedisDB db = databases.get(getCurrentDBIndex());
        db.put(key, value);
    }

    @Override
    public RedisData get(RedisBytes key) {
        RedisDB db = databases.get(getCurrentDBIndex());
        if(db.exist(key)){
            return db.get(key);
        }
        return null;
    }


    @Override
    public void selectDB(int dbIndex) {
        if(dbIndex >= 0 && dbIndex < dbNum){
            currentDBIndex = dbIndex;
        }
        else{
            throw new RuntimeException("dbIndex out of range");
        }
    }

    @Override
    public int getDBNum() {
        return dbNum;
    }


    @Override
    public int getCurrentDBIndex() {
        return currentDBIndex;
    }

    @Override
    public RedisDB[] getDataBases() {
        return databases.toArray(new RedisDB[0]);
    }

    @Override
    public RedisServer getServer() {
        return server;
    }

    @Override
    public void flushAll() {
        for(RedisDB db : databases){
            db.clear();
        }
    }
}

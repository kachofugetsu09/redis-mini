package site.hnfy258.core;

import site.hnfy258.core.command.CommandExecutor;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;

import java.util.List;
import java.util.Set;

/**
 * Redis核心数据操作实现类
 * 
 * 注意：已移除与RedisServer的循环依赖，现在只负责纯数据操作
 * 
 * @author hnfy258
 * @since 1.0
 */
public class RedisCoreImpl implements RedisCore {
    private final List<RedisDB> databases;
    private final int dbNum;
    private int currentDBIndex = 0;
    private CommandExecutor commandExecutor;  // 命令执行器，由上层注入

    /**
     * 构造函数：初始化指定数量的数据库
     * 
     * @param dbNum 数据库数量
     */
    public RedisCoreImpl(final int dbNum) {
        this.dbNum = dbNum;
        this.databases = new java.util.ArrayList<>(dbNum);
        for (int i = 0; i < dbNum; i++) {
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
    public RedisDB[] getDataBases() {        return databases.toArray(new RedisDB[0]);
    }

    @Override
    public void flushAll() {
        for (RedisDB db : databases) {
            db.clear();
        }
    }    @Override
    public boolean delete(RedisBytes key) {
        RedisDB db = databases.get(getCurrentDBIndex());
        if (db.exist(key)) {
            db.delete(key);
            return true;
        }
        return false;
    }

    @Override
    public boolean executeCommand(String commandName, String[] args) {
        if (commandExecutor == null) {
            throw new IllegalStateException("CommandExecutor not set. Please call setCommandExecutor first.");
        }
        return commandExecutor.executeCommand(commandName, args);
    }

    /**
     * 设置命令执行器（由redis-server层注入）
     * 
     * @param commandExecutor 命令执行器
     */
    public void setCommandExecutor(CommandExecutor commandExecutor) {
        this.commandExecutor = commandExecutor;
    }
}

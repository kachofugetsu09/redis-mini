package site.hnfy258.rdb;

import site.hnfy258.server.core.RedisCore;

import java.io.File;

public class RdbManager {
    private RedisCore redisCore;
    private String fileName = RdbConstants.RDB_FILE_NAME;
    private RdbWriter writer;
    private RdbLoader loader;

    public RdbManager(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.writer = new RdbWriter(redisCore);
        this.loader = new RdbLoader(redisCore);
    }

    public boolean saveRdb(){
        return writer.writeRdb(fileName);
    }

    public boolean loadRdb(){
        return loader.loadRdb(new File(fileName));
    }

    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}

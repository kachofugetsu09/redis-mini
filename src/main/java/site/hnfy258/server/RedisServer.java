package site.hnfy258.server;

import site.hnfy258.rdb.RdbManager;

public interface RedisServer {
    void start();

    void stop();

    RdbManager getRdbManager();
}

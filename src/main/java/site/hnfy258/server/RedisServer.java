package site.hnfy258.server;

import site.hnfy258.aof.AofManager;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.rdb.RdbManager;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.core.RedisCore;

public interface RedisServer {
    void start();

    void stop();

    RdbManager getRdbManager();

    AofManager getAofManager();

    void setRedisNode(RedisNode masterNode);

    RedisCore getRedisCore();

    RedisNode getRedisNode();

    RedisContext getRedisContext();
}

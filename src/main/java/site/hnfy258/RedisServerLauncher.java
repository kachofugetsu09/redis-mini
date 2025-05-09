package site.hnfy258;

import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;

public class RedisServerLauncher {
    public static void main(String[] args) {
        RedisServer redisServer =  new RedisMiniServer("localhost", 6379);
        redisServer.start();
    }
}

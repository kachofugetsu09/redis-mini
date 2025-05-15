package site.hnfy258;

import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;

import java.io.FileNotFoundException;

public class RedisServerLauncher {
    public static void main(String[] args) throws FileNotFoundException {
        RedisServer redisServer =  new RedisMiniServer("localhost", 6379);
        redisServer.start();
    }
}

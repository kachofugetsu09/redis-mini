package site.hnfy258;

import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;
import site.hnfy258.server.config.RedisServerConfig;

public class RedisServerLauncher {
    public static void main(String[] args) throws Exception {
        RedisServerConfig config = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .build();
                
        RedisServer redisServer = new RedisMiniServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                redisServer.stop();
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        
        redisServer.start();
    }
}

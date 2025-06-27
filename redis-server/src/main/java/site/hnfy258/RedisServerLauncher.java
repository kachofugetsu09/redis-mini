package site.hnfy258;

import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;
import site.hnfy258.server.config.RedisServerConfig;

public class RedisServerLauncher {
    public static void main(String[] args) throws Exception {
        RedisServerConfig config = RedisServerConfig.builder()
                .host("localhost")
                .port(6379).aofEnabled(true)
                .build();
                
        RedisServer redisServer = new RedisMiniServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n正在关闭服务器...");
            try {
                redisServer.stop();
                // 给足够的时间让所有关闭操作完成
                Thread.sleep(3000);
                System.out.println("服务器已安全关闭");
            } catch (Exception e) {
                System.err.println("关闭服务器时发生错误：");
                e.printStackTrace();
            }
        }));
        
        redisServer.start();
    }
}

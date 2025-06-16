package site.hnfy258;

import site.hnfy258.server.RedisMiniServer;
import site.hnfy258.server.RedisServer;

public class RedisServerLauncher {
    public static void main(String[] args) throws Exception {
        RedisServer redisServer =  new RedisMiniServer("localhost", 6379);

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try{
                redisServer.stop();
                Thread.sleep(500);
            }catch(Exception e){
                e.printStackTrace();
            }
        }));
        redisServer.start();
    }
}

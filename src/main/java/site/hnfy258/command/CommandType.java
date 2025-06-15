package site.hnfy258.command;

import lombok.Getter;
import site.hnfy258.command.impl.aof.Bgrewriteaof;
import site.hnfy258.command.impl.aof.Bgsave;
import site.hnfy258.command.impl.Ping;
import site.hnfy258.command.impl.Select;
import site.hnfy258.command.impl.cluster.Psync;
import site.hnfy258.command.impl.hash.Hdel;
import site.hnfy258.command.impl.hash.Hget;
import site.hnfy258.command.impl.hash.Hset;
import site.hnfy258.command.impl.list.Lpop;
import site.hnfy258.command.impl.list.Lpush;
import site.hnfy258.command.impl.list.Lrange;
import site.hnfy258.command.impl.list.Rpop;
import site.hnfy258.command.impl.list.Rpush;
import site.hnfy258.command.impl.set.Sadd;
import site.hnfy258.command.impl.set.Spop;
import site.hnfy258.command.impl.set.Srem;
import site.hnfy258.command.impl.string.Append;
import site.hnfy258.command.impl.string.Get;
import site.hnfy258.command.impl.string.Getrange;
import site.hnfy258.command.impl.string.Incr;
import site.hnfy258.command.impl.string.Mset;
import site.hnfy258.command.impl.string.Set;
import site.hnfy258.command.impl.string.Strlen;
import site.hnfy258.command.impl.zset.Zadd;
import site.hnfy258.command.impl.zset.Zrange;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.server.core.RedisCore;

import java.util.function.Function;

@Getter
public enum CommandType {    PING("PING", core -> new Ping()),    
    SET("SET", Set::new),
    GET("GET", Get::new),
    INCR("INCR", Incr::new),
    MSET("MSET", Mset::new),
    APPEND("APPEND", Append::new),
    STRLEN("STRLEN", Strlen::new),
    GETRANGE("GETRANGE", Getrange::new),
    SADD("SADD", Sadd::new),
    SPOP("SPOP", Spop::new),
    SREM("SREM", Srem::new),    LPUSH("LPUSH", Lpush::new),
    LPOP("LPOP", Lpop::new),
    RPUSH("RPUSH", Rpush::new),
    RPOP("RPOP", Rpop::new),
    LRANGE("LRANGE", Lrange::new),
    HSET("HSET", Hset::new),
    HGET("HGET", Hget::new),
    HDEL("HDEL", Hdel::new),
    ZADD("ZADD", Zadd::new),
    ZRANGE("ZRANGE", Zrange::new),
    SELECT("SELECT", Select::new),
    BGSAVE("BGSAVE", Bgsave::new),
    BGREWRITEAOF("BGREWRITEAOF", Bgrewriteaof::new),
    PSYNC("PSYNC", Psync::new);

    private final Function<RedisCore, Command> supplier;
    private final RedisBytes commandBytes;

    CommandType(String commandName, Function<RedisCore, Command> supplier) {
        this.supplier = supplier;
        this.commandBytes = RedisBytes.fromString(commandName);
    }

    public boolean matchesBytes(byte[] bytes) {
        return commandBytes.equalsIgnoreCase(new RedisBytes(bytes));
    }

    public boolean matchesRedisBytes(RedisBytes other) {
        return commandBytes.equalsIgnoreCase(other);
    }

    public Command createCommand(RedisCore core) {
        return supplier.apply(core);
    }
}

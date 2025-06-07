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
import site.hnfy258.command.impl.set.Sadd;
import site.hnfy258.command.impl.set.Spop;
import site.hnfy258.command.impl.set.Srem;
import site.hnfy258.command.impl.string.Append;
import site.hnfy258.command.impl.string.Get;
import site.hnfy258.command.impl.string.Getrange;
import site.hnfy258.command.impl.string.Set;
import site.hnfy258.command.impl.string.Strlen;
import site.hnfy258.command.impl.zset.Zadd;
import site.hnfy258.command.impl.zset.Zrange;
import site.hnfy258.server.core.RedisCore;

import java.util.function.Function;
@Getter
public enum CommandType {
    PING(core ->new Ping()),    SET(Set::new),
    GET(Get::new),
    APPEND(Append::new),
    STRLEN(Strlen::new),
    GETRANGE(Getrange::new),
    SADD(Sadd::new),
    SPOP(Spop::new),
    SREM(Srem::new),
    LPUSH(Lpush::new),
    LPOP(Lpop::new),
    LRANGE(Lrange::new),
    HSET(Hset::new),
    HGET(Hget::new),
    HDEL(Hdel::new),
    ZADD(Zadd::new),
    ZRANGE(Zrange::new),
    SELECT(Select::new),
    BGSAVE(Bgsave::new),
    BGREWRITEAOF(Bgrewriteaof::new),
    PSYNC(Psync::new);

    private final Function<RedisCore, Command> supplier;

    CommandType(Function<RedisCore, Command> supplier) {
        this.supplier = supplier;
    }

}

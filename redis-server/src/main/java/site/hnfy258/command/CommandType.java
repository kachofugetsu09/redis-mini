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
import site.hnfy258.command.impl.list.*;
import site.hnfy258.command.impl.set.Sadd;
import site.hnfy258.command.impl.set.Scard;
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
import site.hnfy258.command.impl.zset.Zcard;
import site.hnfy258.command.impl.zset.Zrange;
import site.hnfy258.command.impl.server.*;
import site.hnfy258.command.impl.key.*;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.server.context.RedisContext;

import java.util.HashMap;
import java.util.Map;


@Getter
public enum CommandType {
    PING("PING"),
    SET("SET"),
    GET("GET"),
    INCR("INCR"),
    MSET("MSET"),
    APPEND("APPEND"),
    STRLEN("STRLEN"),
    GETRANGE("GETRANGE"),
    SADD("SADD"),
    SPOP("SPOP"),
    SREM("SREM"),    
    LPUSH("LPUSH"),
    LPOP("LPOP"),
    RPUSH("RPUSH"),
    RPOP("RPOP"),
    LRANGE("LRANGE"),
    HSET("HSET"),
    HGET("HGET"),
    HDEL("HDEL"),
    ZADD("ZADD"),
    ZRANGE("ZRANGE"),
    SELECT("SELECT"),
    BGSAVE("BGSAVE"),
    BGREWRITEAOF("BGREWRITEAOF"),
    PSYNC("PSYNC"),
    SCAN("SCAN"),
    KEYS("KEYS"),
    LLEN("LLEN"),
    SCARD("SCARD"),
    ZCARD("ZCARD"),
    INFO("INFO"),
    CONFIG_GET("CONFIG"),
    DBSIZE("DBSIZE"),
    TYPE("TYPE"),
    TTL("TTL");

    private final RedisBytes commandBytes;

    private static final Map<RedisBytes, CommandType> COMMAND_CACHE = new HashMap<>();

    private final int bytesHashCode;
    
    static {
        // 1. 静态初始化时构建命令查找缓存
        for (final CommandType type : CommandType.values()) {
            COMMAND_CACHE.put(type.commandBytes, type);
        }
    }

    CommandType(final String commandName) {
        this.commandBytes = RedisBytes.fromString(commandName);
        this.bytesHashCode = this.commandBytes.hashCode();
    }

    /**
     * 
     * @param commandBytes 命令字节数组
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByBytes(final RedisBytes commandBytes) {
        return COMMAND_CACHE.get(commandBytes);
    }
    
    /**
     * 
     * @param bytes 命令字节数组
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByBytes(final byte[] bytes) {
        final RedisBytes redisBytes = new RedisBytes(bytes);
        return COMMAND_CACHE.get(redisBytes);
    }

    /**
     *  兼容性方法：支持字符串查找（用于AOF加载等场景）
     * 
     * @param commandName 命令名称字符串
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByName(final String commandName) {
        if (commandName == null || commandName.isEmpty()) {
            return null;
        }
        
        // 2. 转换为大写并查找
        final String upperCommandName = commandName.toUpperCase();
        try {
            return CommandType.valueOf(upperCommandName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public boolean matchesBytes(byte[] bytes) {
        return commandBytes.equalsIgnoreCase(new RedisBytes(bytes));
    }

    public boolean matchesRedisBytes(RedisBytes other) {
        return commandBytes.equalsIgnoreCase(other);
    }/**
     * 使用RedisContext创建命令实例
     * 
     * @param context Redis统一上下文
     * @return 命令实例
     */
    public Command createCommand(final RedisContext context) {
        switch (this) {
            case PING:
                return new Ping();
            case SET:
                return new Set(context);
            case GET:
                return new Get(context);
            case INCR:
                return new Incr(context);
            case MSET:
                return new Mset(context);
            case APPEND:
                return new Append(context);
            case STRLEN:
                return new Strlen(context);
            case GETRANGE:
                return new Getrange(context);
            case SADD:
                return new Sadd(context);
            case SPOP:
                return new Spop(context);
            case SREM:
                return new Srem(context);
            case LPUSH:
                return new Lpush(context);
            case LPOP:
                return new Lpop(context);
            case RPUSH:
                return new Rpush(context);
            case RPOP:
                return new Rpop(context);
            case LRANGE:
                return new Lrange(context);
            case HSET:
                return new Hset(context);
            case HGET:
                return new Hget(context);
            case HDEL:
                return new Hdel(context);
            case ZADD:
                return new Zadd(context);
            case ZRANGE:
                return new Zrange(context);
            case SELECT:
                return new Select(context);
            case BGSAVE:
                return new Bgsave(context);
            case BGREWRITEAOF:
                return new Bgrewriteaof(context);
            case PSYNC:
                return new Psync(context);
            case SCAN:
                return new Scan(context);
            case KEYS:
                return new Keys(context);
            case LLEN:
                return new Llen(context);
            case SCARD:
                return new Scard(context);
            case ZCARD:
                return new Zcard(context);
            case INFO:
                return new Info(context);
            case CONFIG_GET:
                return new ConfigGet(context);
            case DBSIZE:
                return new Dbsize(context);
            case TYPE:
                return new Type(context);
            case TTL:
                return new Ttl(context);
            default:
                throw new IllegalArgumentException("不支持的命令类型: " + this);
        }
    }
}

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

/**
 * Redis命令类型枚举，定义了系统支持的所有Redis命令。
 * 
 * <p>该枚举的主要职责：
 * <ul>
 *   <li>定义所有支持的Redis命令类型
 *   <li>提供命令查找和匹配功能
 *   <li>实现命令工厂方法
 *   <li>优化命令查找性能
 *   <li>支持命令的大小写不敏感匹配
 * </ul>
 * 
 * <p>设计特点：
 * <ul>
 *   <li>使用枚举确保命令类型的唯一性
 *   <li>实现命令字节数组的缓存优化
 *   <li>支持多种命令查找方式
 *   <li>提供命令实例的工厂方法
 *   <li>实现高效的命令匹配算法
 * </ul>
 * 
 * <p>性能优化：
 * <ul>
 *   <li>使用HashMap缓存命令映射
 *   <li>预计算命令字节数组的哈希值
 *   <li>实现零拷贝的字节数组比较
 *   <li>支持命令名称的快速匹配
 *   <li>优化大小写转换性能
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
@Getter
public enum CommandType {
    // ========== 字符串命令 ==========
    /** PING命令：测试服务器连接 */
    PING("PING"),
    /** SET命令：设置键值对 */
    SET("SET"),
    /** GET命令：获取键值 */
    GET("GET"),
    /** INCR命令：将键值加1 */
    INCR("INCR"),
    /** MSET命令：批量设置键值对 */
    MSET("MSET"),
    /** APPEND命令：追加字符串 */
    APPEND("APPEND"),
    /** STRLEN命令：获取字符串长度 */
    STRLEN("STRLEN"),
    /** GETRANGE命令：获取字符串子串 */
    GETRANGE("GETRANGE"),

    // ========== 集合命令 ==========
    /** SADD命令：添加集合成员 */
    SADD("SADD"),
    /** SPOP命令：随机移除集合成员 */
    SPOP("SPOP"),
    /** SREM命令：移除指定集合成员 */
    SREM("SREM"),

    // ========== 列表命令 ==========
    /** LPUSH命令：左侧插入列表 */
    LPUSH("LPUSH"),
    /** LPOP命令：左侧弹出列表 */
    LPOP("LPOP"),
    /** RPUSH命令：右侧插入列表 */
    RPUSH("RPUSH"),
    /** RPOP命令：右侧弹出列表 */
    RPOP("RPOP"),
    /** LRANGE命令：获取列表范围 */
    LRANGE("LRANGE"),

    // ========== 哈希命令 ==========
    /** HSET命令：设置哈希字段 */
    HSET("HSET"),
    /** HGET命令：获取哈希字段 */
    HGET("HGET"),
    /** HDEL命令：删除哈希字段 */
    HDEL("HDEL"),

    // ========== 有序集合命令 ==========
    /** ZADD命令：添加有序集合成员 */
    ZADD("ZADD"),
    /** ZRANGE命令：获取有序集合范围 */
    ZRANGE("ZRANGE"),

    // ========== 服务器命令 ==========
    /** SELECT命令：选择数据库 */
    SELECT("SELECT"),
    /** BGSAVE命令：异步保存数据 */
    BGSAVE("BGSAVE"),
    /** BGREWRITEAOF命令：重写AOF文件 */
    BGREWRITEAOF("BGREWRITEAOF"),
    /** PSYNC命令：主从同步 */
    PSYNC("PSYNC"),

    // ========== 键命令 ==========
    /** SCAN命令：迭代数据库中的键 */
    SCAN("SCAN"),
    /** KEYS命令：查找所有匹配的键 */
    KEYS("KEYS"),
    /** LLEN命令：获取列表长度 */
    LLEN("LLEN"),
    /** SCARD命令：获取集合成员数 */
    SCARD("SCARD"),
    /** ZCARD命令：获取有序集合成员数 */
    ZCARD("ZCARD"),
    /** INFO命令：获取服务器信息 */
    INFO("INFO"),
    /** CONFIG_GET命令：获取配置参数 */
    CONFIG_GET("CONFIG"),
    /** DBSIZE命令：获取数据库大小 */
    DBSIZE("DBSIZE"),
    /** TYPE命令：获取键的数据类型 */
    TYPE("TYPE"),
    /** TTL命令：获取键的过期时间 */
    TTL("TTL");

    /** 命令字节数组，使用RedisBytes优化性能 */
    private final RedisBytes commandBytes;

    /** 命令查找缓存，提高查找性能 */
    private static final Map<RedisBytes, CommandType> COMMAND_CACHE = new HashMap<>();

    /** 命令字节数组的哈希值，用于快速比较 */
    private final int bytesHashCode;
    
    static {
        // 初始化命令查找缓存
        for (final CommandType type : CommandType.values()) {
            COMMAND_CACHE.put(type.commandBytes, type);
        }
    }

    /**
     * 构造函数，初始化命令类型。
     * 
     * @param commandName 命令名称
     */
    CommandType(final String commandName) {
        this.commandBytes = RedisBytes.fromString(commandName);
        this.bytesHashCode = this.commandBytes.hashCode();
    }

    /**
     * 根据字节数组查找对应的命令类型。
     * 
     * <p>查找过程：
     * <ul>
     *   <li>首先在缓存中直接查找
     *   <li>如果未找到，转换为大写再查找
     *   <li>支持大小写不敏感的匹配
     * </ul>
     *
     * @param commandBytes 命令字节数组
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByBytes(final RedisBytes commandBytes) {
        if (commandBytes == null) {
            return null;
        }
        
        // 1. 直接查找缓存
        CommandType result = COMMAND_CACHE.get(commandBytes);
        if (result != null) {
            return result;
        }
        
        // 2. 大小写不敏感查找：将输入转换为大写后再查找
        final String commandStr = commandBytes.getString();
        final String upperCommandStr = commandStr.toUpperCase();
        
        // 3. 使用大写字符串创建RedisBytes进行查找
        final RedisBytes upperCommandBytes = RedisBytes.fromString(upperCommandStr);
        result = COMMAND_CACHE.get(upperCommandBytes);
        
        return result;
    }
    
    /**
     * 根据原始字节数组查找命令类型。
     * 
     * @param bytes 命令字节数组
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByBytes(final byte[] bytes) {
        final RedisBytes redisBytes = new RedisBytes(bytes);
        return COMMAND_CACHE.get(redisBytes);
    }

    /**
     * 根据命令名称字符串查找命令类型。
     * 
     * <p>该方法主要用于：
     * <ul>
     *   <li>AOF文件加载
     *   <li>命令行解析
     *   <li>配置文件读取
     *   <li>兼容性处理
     * </ul>
     * 
     * @param commandName 命令名称字符串
     * @return 对应的CommandType，如果不存在则返回null
     */
    public static CommandType findByName(final String commandName) {
        if (commandName == null || commandName.isEmpty()) {
            return null;
        }
        
        // 转换为大写并查找
        final String upperCommandName = commandName.toUpperCase();
        try {
            return CommandType.valueOf(upperCommandName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * 检查命令是否匹配给定的字节数组。
     * 
     * @param bytes 要匹配的字节数组
     * @return 如果匹配返回true，否则返回false
     */
    public boolean matchesBytes(byte[] bytes) {
        return commandBytes.equalsIgnoreCase(new RedisBytes(bytes));
    }

    /**
     * 检查命令是否匹配给定的RedisBytes。
     * 
     * @param other 要匹配的RedisBytes
     * @return 如果匹配返回true，否则返回false
     */
    public boolean matchesRedisBytes(RedisBytes other) {
        return commandBytes.equalsIgnoreCase(other);
    }

    /**
     * 使用Redis上下文创建命令实例。
     * 
     * <p>工厂方法特点：
     * <ul>
     *   <li>根据命令类型创建对应的实例
     *   <li>注入Redis上下文
     *   <li>确保线程安全
     *   <li>支持命令的扩展
     * </ul>
     * 
     * @param context Redis统一上下文
     * @return 命令实例
     * @throws IllegalArgumentException 如果命令类型不支持
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

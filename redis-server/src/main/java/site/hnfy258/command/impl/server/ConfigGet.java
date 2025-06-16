package site.hnfy258.command.impl.server;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ConfigGet implements Command {
    private final RedisContext context;
    private Resp[] array;
    private String pattern;
    private final Map<String, String> config;

    public ConfigGet(RedisContext context) {
        this.context = context;
        this.config = new HashMap<>();
        initializeConfig();
    }

    private void initializeConfig() {
        // Server configuration
        config.put("port", String.valueOf(context.getServerPort()));
        config.put("bind", context.getServerHost());
        config.put("databases", String.valueOf(context.getDBNum()));
        
        // Persistence configuration
        config.put("appendonly", String.valueOf(context.isAofEnabled()));
        config.put("save", context.isRdbEnabled() ? "3600 1 300 100 60 10000" : "");
        
        // Memory configuration
        config.put("maxmemory", String.valueOf(Runtime.getRuntime().maxMemory()));
        config.put("maxmemory-policy", "noeviction");
        
        // General configuration
        config.put("timeout", "0");
        config.put("tcp-keepalive", "300");
        config.put("daemonize", "no");
        config.put("pidfile", "");
        config.put("loglevel", "notice");
        config.put("logfile", "");
        config.put("databases", "16");
    }

    @Override
    public CommandType getType() {
        return CommandType.CONFIG_GET;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.pattern = ((BulkString) array[1]).toString();
        }
    }

    @Override
    public Resp handle() {
        if (pattern == null) {
            return new RespArray(new Resp[0]);
        }

        List<Resp> result = new ArrayList<>();
        Pattern regex = Pattern.compile(pattern.replace("*", ".*"));

        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (regex.matcher(entry.getKey()).matches()) {
                result.add(new BulkString(entry.getKey().getBytes()));
                result.add(new BulkString(entry.getValue().getBytes()));
            }
        }

        return new RespArray(result.toArray(new Resp[0]));
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
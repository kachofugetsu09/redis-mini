package site.hnfy258.command.impl.server;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;

import java.lang.management.ManagementFactory;

public class Info implements Command {
    private final RedisContext context;
    private Resp[] array;
    private String section;

    public Info(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.INFO;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.section = ((BulkString) array[1]).toString();
        }
    }

    @Override
    public Resp handle() {
        StringBuilder info = new StringBuilder();
        
        // Server section
        info.append("# Server\r\n");
        info.append("redis_version:1.0.0\r\n");
        info.append("redis_mode:standalone\r\n");
        info.append("os:").append(System.getProperty("os.name")).append("\r\n");
        info.append("arch_bits:").append(System.getProperty("os.arch")).append("\r\n");
        info.append("process_id:").append(getProcessId()).append("\r\n");
        info.append("tcp_port:").append(context.getServerPort()).append("\r\n");
        info.append("uptime_in_seconds:").append(System.currentTimeMillis() / 1000).append("\r\n");
        
        // Memory section
        info.append("\r\n# Memory\r\n");
        info.append("used_memory:").append(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).append("\r\n");
        info.append("used_memory_human:").append(formatBytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())).append("\r\n");
        info.append("total_system_memory:").append(Runtime.getRuntime().maxMemory()).append("\r\n");
        info.append("total_system_memory_human:").append(formatBytes(Runtime.getRuntime().maxMemory())).append("\r\n");
        
        // Persistence section
        info.append("\r\n# Persistence\r\n");
        info.append("aof_enabled:").append(context.isAofEnabled()).append("\r\n");
        info.append("rdb_enabled:").append(context.isRdbEnabled()).append("\r\n");
        
        // Stats section
        info.append("\r\n# Stats\r\n");
        info.append("total_connections_received:0\r\n");
        info.append("total_commands_processed:0\r\n");
        info.append("instantaneous_ops_per_sec:0\r\n");
        info.append("total_net_input_bytes:0\r\n");
        info.append("total_net_output_bytes:0\r\n");
        
        // Replication section
        info.append("\r\n# Replication\r\n");
        info.append("role:").append(context.isMaster() ? "master" : "slave").append("\r\n");
        info.append("connected_slaves:0\r\n");
        
        // CPU section
        info.append("\r\n# CPU\r\n");
        info.append("used_cpu_sys:0\r\n");
        info.append("used_cpu_user:0\r\n");
        info.append("used_cpu_sys_children:0\r\n");
        info.append("used_cpu_user_children:0\r\n");
        
        // Cluster section
        info.append("\r\n# Cluster\r\n");
        info.append("cluster_enabled:no\r\n");
        
        // Keyspace section
        info.append("\r\n# Keyspace\r\n");
        int currentDb = context.getCurrentDBIndex(); // 保存当前数据库索引
        try {
            for (int i = 0; i < context.getDBNum(); i++) {
                context.selectDB(i);
                long keys = context.keys().size();
                if (keys > 0) {
                    info.append("db").append(i).append(":keys=").append(keys).append("\r\n");
                }
            }        
        } finally {
            context.selectDB(currentDb); // 恢复原来的数据库索引
        }
        
        // 🚀 优化：使用 RedisBytes.fromString 获得更好的性能
        return new BulkString(RedisBytes.fromString(info.toString()));
    }

    private String formatBytes(long bytes) {
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = bytes;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return String.format("%.2f%s", size, units[unitIndex]);
    }

    private long getProcessId() {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(jvmName.split("@")[0]);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
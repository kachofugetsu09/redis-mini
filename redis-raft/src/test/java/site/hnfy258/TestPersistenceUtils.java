package site.hnfy258;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;

/**
 * 测试持久化工具类
 * 提供激进的持久化文件清理功能，确保测试间的完全隔离
 */
public class TestPersistenceUtils {
    
    /**
     * 可能的持久化文件和目录模式
     */
    private static final List<String> PERSISTENCE_PATTERNS = Arrays.asList(
        "node-*",           // Raft节点状态文件
        "raft-*",           // Raft相关文件
        "redis-node-*",     // Redis节点数据目录
        "*.log",            // 日志文件
        "*.rdb",            // Redis持久化文件
        "*.aof",            // AOF文件
        "temp-*",           // 临时文件
        "test-*"            // 测试文件
    );
    
    /**
     * 需要清理的根目录列表
     */
    private static final List<String> CLEANUP_DIRECTORIES = Arrays.asList(
        ".",                // 当前目录
        "./data",           // 数据目录
        "./logs",           // 日志目录
        "./temp",           // 临时目录
        System.getProperty("java.io.tmpdir") + "/raft-test"  // 系统临时目录
    );
    
    /**
     * 激进清理所有可能的持久化文件
     * 在每个测试开始前和结束后调用
     */
    public static void aggressiveCleanup() {
        System.out.println("=== Starting aggressive persistence cleanup ===");
        
        int totalDeleted = 0;
        
        // 1. 清理指定目录中的匹配文件
        for (String dirPath : CLEANUP_DIRECTORIES) {
            File dir = new File(dirPath);
            if (dir.exists() && dir.isDirectory()) {
                totalDeleted += cleanupDirectory(dir);
            }
        }
        
        // 2. 清理当前工作目录下的持久化文件（按编号范围）
        totalDeleted += cleanupNodeFiles(20); // 清理 node-0 到 node-19
        
        // 3. 清理可能的 Raft 数据目录
        totalDeleted += cleanupRaftDataDirectories();
        
        // 4. 强制垃圾回收，释放文件句柄
        System.gc();
        
        System.out.println("=== Cleanup completed, deleted " + totalDeleted + " files/directories ===");
        
        // 5. 等待文件系统同步
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 清理指定目录中匹配的文件
     */
    private static int cleanupDirectory(File directory) {
        int deleted = 0;
        
        try {
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String fileName = file.getFileName().toString();
                    
                    // 检查是否匹配任何持久化模式
                    if (matchesAnyPattern(fileName)) {
                        try {
                            Files.delete(file);
                            System.out.println("Deleted file: " + file);
                            return FileVisitResult.CONTINUE;
                        } catch (IOException e) {
                            System.err.println("Failed to delete file " + file + ": " + e.getMessage());
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
                
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    // 删除空的测试相关目录
                    String dirName = dir.getFileName().toString();
                    if (matchesAnyPattern(dirName) && isDirectoryEmpty(dir)) {
                        try {
                            Files.delete(dir);
                            System.out.println("Deleted empty directory: " + dir);
                        } catch (IOException e) {
                            System.err.println("Failed to delete directory " + dir + ": " + e.getMessage());
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("Error walking directory " + directory + ": " + e.getMessage());
        }
        
        return deleted;
    }
    
    /**
     * 清理节点文件（node-0 到 node-N）
     */
    private static int cleanupNodeFiles(int maxNodeId) {
        int deleted = 0;
        
        for (int i = 0; i <= maxNodeId; i++) {
            // 清理节点状态文件
            File nodeFile = new File("node-" + i);
            if (nodeFile.exists()) {
                if (nodeFile.delete()) {
                    System.out.println("Deleted node file: node-" + i);
                    deleted++;
                } else {
                    System.err.println("Failed to delete node file: node-" + i);
                }
            }
            
            // 清理节点数据目录
            File nodeDir = new File("redis-node-" + i);
            if (nodeDir.exists()) {
                deleted += deleteDirectoryRecursively(nodeDir);
            }
        }
        
        return deleted;
    }
    
    /**
     * 清理 Raft 数据目录
     */
    private static int cleanupRaftDataDirectories() {
        int deleted = 0;
        
        // 清理可能的 data 目录
        File dataDir = new File("data");
        if (dataDir.exists() && dataDir.isDirectory()) {
            deleted += deleteDirectoryRecursively(dataDir);
        }
        
        // 清理可能的 logs 目录中的测试文件
        File logsDir = new File("logs");
        if (logsDir.exists() && logsDir.isDirectory()) {
            File[] logFiles = logsDir.listFiles((dir, name) -> 
                name.startsWith("test-") || name.startsWith("raft-") || name.contains("node-"));
            if (logFiles != null) {
                for (File logFile : logFiles) {
                    if (logFile.delete()) {
                        System.out.println("Deleted log file: " + logFile);
                        deleted++;
                    }
                }
            }
        }
        
        return deleted;
    }
    
    /**
     * 递归删除目录及其所有内容
     */
    private static int deleteDirectoryRecursively(File directory) {
        int deleted = 0;
        
        try {
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    System.out.println("Deleted file: " + file);
                    return FileVisitResult.CONTINUE;
                }
                
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    System.out.println("Deleted directory: " + dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            deleted++;
        } catch (IOException e) {
            System.err.println("Failed to delete directory " + directory + ": " + e.getMessage());
        }
        
        return deleted;
    }
    
    /**
     * 检查文件名是否匹配任何持久化模式
     */
    private static boolean matchesAnyPattern(String fileName) {
        for (String pattern : PERSISTENCE_PATTERNS) {
            if (matchesPattern(fileName, pattern)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 简单的通配符匹配
     */
    private static boolean matchesPattern(String fileName, String pattern) {
        if (pattern.contains("*")) {
            String prefix = pattern.substring(0, pattern.indexOf("*"));
            String suffix = pattern.substring(pattern.indexOf("*") + 1);
            return fileName.startsWith(prefix) && fileName.endsWith(suffix);
        } else {
            return fileName.equals(pattern);
        }
    }
    
    /**
     * 检查目录是否为空
     */
    private static boolean isDirectoryEmpty(Path directory) {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return !dirStream.iterator().hasNext();
        } catch (IOException e) {
            return false;
        }
    }
    
    /**
     * 清理特定节点的所有相关文件
     */
    public static void cleanupNodeSpecificFiles(int nodeId) {
        System.out.println("Cleaning up files for node " + nodeId);
        
        // 节点状态文件
        File nodeFile = new File("node-" + nodeId);
        if (nodeFile.exists() && nodeFile.delete()) {
            System.out.println("Deleted node-" + nodeId);
        }
        
        // 节点数据目录
        File nodeDir = new File("redis-node-" + nodeId);
        if (nodeDir.exists()) {
            deleteDirectoryRecursively(nodeDir);
        }
        
        // 节点日志文件
        File logsDir = new File("logs");
        if (logsDir.exists()) {
            File[] nodeLogFiles = logsDir.listFiles((dir, name) -> name.contains("node-" + nodeId));
            if (nodeLogFiles != null) {
                for (File logFile : nodeLogFiles) {
                    if (logFile.delete()) {
                        System.out.println("Deleted log file: " + logFile);
                    }
                }
            }
        }
    }
    
    /**
     * 验证清理是否彻底
     */
    public static boolean verifyCleanState() {
        boolean isClean = true;
        
        // 检查是否还有残留的持久化文件
        for (String dirPath : CLEANUP_DIRECTORIES) {
            File dir = new File(dirPath);
            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles((d, name) -> matchesAnyPattern(name));
                if (files != null && files.length > 0) {
                    System.err.println("Warning: Found " + files.length + " persistence files in " + dirPath);
                    for (File file : files) {
                        System.err.println("  - " + file.getName());
                    }
                    isClean = false;
                }
            }
        }
        
        return isClean;
    }
}

package site.hnfy258.rdb;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * RDB持久化管理器
 * 
 * <p>统一管理RDB文件的读写操作，提供同步和异步的持久化功能。
 * 基于RedisCore接口设计，支持数据快照、文件复制、临时文件处理等
 * 高级功能，是Redis持久化系统的核心组件。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>RDB文件保存 - 支持同步和异步两种模式</li>
 *     <li>RDB文件加载 - 从文件恢复Redis数据</li>
 *     <li>快照生成 - 用于主从复制的数据同步</li>
 *     <li>临时文件处理 - 支持内存到文件的双向转换</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
@Getter
public class RdbManager {
    
    /** RDB文件名 */
    private String fileName;
    
    /** RDB写入器 */
    private RdbWriter writer;
    
    /** RDB加载器 */
    private RdbLoader loader;
    
    /** Redis核心接口，提供数据库访问功能 */
    private final RedisCore redisCore;

    /**
     * 构造函数（使用默认文件名）
     * 
     * @param redisCore Redis核心接口
     */
    public RdbManager(final RedisCore redisCore) {
        this(redisCore, RdbConstants.RDB_FILE_NAME);
    }

    /**
     * 构造函数（指定文件名）
     * 
     * @param redisCore Redis核心接口
     * @param fileName RDB文件名
     */    public RdbManager(final RedisCore redisCore, final String fileName) {
        this.redisCore = redisCore;
        this.fileName = fileName;
        
        // 1. 初始化RDB相关组件
        this.writer = new RdbWriter(redisCore);
        this.loader = new RdbLoader(redisCore);
        
        log.info("RdbManager初始化完成，文件: {}", fileName);
    }

    /**
     * 同步保存RDB文件
     * 
     * @return 保存是否成功
     */
    public boolean saveRdb() {
        return writer.writeRdb(fileName);
    }

    /**
     * 异步保存RDB文件（BGSAVE）
     * 
     * @return CompletableFuture，可用于监控保存状态
     */
    public CompletableFuture<Boolean> bgSaveRdb() {
        return writer.bgSaveRdb(fileName);
    }

    /**
     * 从RDB文件加载数据
     * 
     * @return 加载是否成功
     */
    public boolean loadRdb() {
        return loader.loadRdb(new File(fileName));
    }

    /**
     * 关闭管理器并释放资源
     */
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * 为主从复制创建临时RDB文件
     * 
     * <p>生成临时RDB文件并返回文件内容的字节数组，
     * 用于主从复制过程中的数据同步。
     * 
     * @return RDB文件内容的字节数组，失败时返回null
     */
    public byte[] createTempRdbForReplication() {        //1. 创建一个临时的rdb文件
        String tempFileName = "temp-repl-" + System.currentTimeMillis() + ".rdb";
        String originalFileName = this.fileName;
        try {
            //2. 临时设置文件名为临时文件
            this.fileName = tempFileName;

            if (!saveRdb()) {
                return null;
            }
            File tempRdbFile = new File(tempFileName);
            try {
                byte[] content = new byte[(int) tempRdbFile.length()];
                try (FileInputStream fis = new FileInputStream(tempRdbFile)) {
                    fis.read(content);
                }
                return content;
            } catch (Exception e) {
                log.error("读取临时RDB文件失败", e);
                return null;
            } finally {
                //3. 删除临时文件
                if (tempRdbFile.exists()) {
                    if (!tempRdbFile.delete()) {
                        log.warn("无法删除临时RDB文件: {}", tempRdbFile.getAbsolutePath());
                    }
                }
            }

        } finally {
            // 4. 恢复原始文件名
            this.fileName = originalFileName;
        }
    }

    /**
     * 从字节数组加载RDB数据
     * 
     * <p>将字节数组写入临时文件，然后加载到Redis中。
     * 主要用于主从复制中从节点接收数据的场景。
     * 
     * @param rdbContent RDB文件内容的字节数组
     * @return 加载是否成功
     */
    public boolean loadRdbFromBytes(byte[] rdbContent) {
        if (rdbContent == null || rdbContent.length == 0) {            log.error("RDB内容为空，无法加载");
            return false;
        }
        //生成唯一临时文件名
        String tempFileName = "temp-rdb-" + UUID.randomUUID().toString() + ".rdb";
        File tempFile = new File(tempFileName);
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(rdbContent);
            fos.flush();
            log.info("临时RDB文件创建成功: {}", tempFileName);
        } catch (Exception e) {
            log.error("创建临时RDB文件失败", e);
            return false;
        }

        try {
            log.info("开始加载临时RDB文件: {}", tempFileName);
            boolean success = loader.loadRdb(tempFile);

            if (!success) {
                log.error("加载临时RDB文件失败: {}", tempFileName);
                return false;
            }
            log.info("临时RDB文件加载成功: {}", tempFileName);
            return true;
        } finally {
            //删除临时文件
            if (tempFile.exists()) {
                if (!tempFile.delete()) {
                    log.warn("无法删除临时RDB文件: {}", tempFile.getAbsolutePath());
                } else {
                    log.info("临时RDB文件已删除: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * 生成RDB快照数据
     * 
     * <p>将当前Redis数据生成RDB快照并返回字节数组。
     * 与createTempRdbForReplication类似，但更通用，
     * 可用于备份、导出等场景。
     * 
     * @return RDB快照的字节数组
     * @throws Exception 生成失败时抛出异常
     */
    public byte[] generateSnapshot() throws Exception {
        // 1. 生成临时文件名
        String tempFileName = fileName + ".temp." + UUID.randomUUID().toString();
        
        try {
            // 2. 写入临时文件
            boolean success = writer.writeRdb(tempFileName);            if (!success) {
                throw new Exception("生成RDB快照失败");
            }
            
            // 3. 读取文件内容
            File tempFile = new File(tempFileName);
            try (FileInputStream fis = new FileInputStream(tempFile)) {
                long fileLength = tempFile.length();
                if (fileLength > Integer.MAX_VALUE) {
                    throw new Exception("RDB文件过大，超过2GB限制");
                }
                
                byte[] data = new byte[(int) fileLength];
                int totalBytesRead = 0;
                int bytesRead;
                
                // 确保读取完整文件
                while (totalBytesRead < data.length) {
                    bytesRead = fis.read(data, totalBytesRead, data.length - totalBytesRead);
                    if (bytesRead == -1) {
                        break;
                    }
                    totalBytesRead += bytesRead;
                }
                
                log.info("生成RDB快照成功，大小: {} 字节", data.length);
                return data;
            }
        } finally {
            // 4. 清理临时文件
            File tempFile = new File(tempFileName);
            if (tempFile.exists() && !tempFile.delete()) {
                log.warn("无法删除临时RDB文件: {}", tempFile.getAbsolutePath());
            }
        }
    }
}


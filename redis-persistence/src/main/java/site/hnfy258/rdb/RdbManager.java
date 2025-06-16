package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;

@Slf4j
public class RdbManager {
    private String fileName = RdbConstants.RDB_FILE_NAME;
    private RdbWriter writer;
    private RdbLoader loader;
      // 核心依赖：统一使用RedisCore接口
    private final RedisCore redisCore;    /**
     * 新增构造函数：支持RedisCore接口的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param redisCore Redis核心接口，提供所有核心功能的访问接口
     */
    public RdbManager(final RedisCore redisCore) {
        this(redisCore, RdbConstants.RDB_FILE_NAME);
    }

    /**
     * 新增构造函数：支持RedisCore接口的完整版本
     * 
     * @param redisCore Redis核心接口
     * @param fileName RDB文件名
     */
    public RdbManager(final RedisCore redisCore, final String fileName) {
        this.redisCore = redisCore;
        this.fileName = fileName;
        
        // 1. 初始化RDB相关组件
        this.writer = new RdbWriter(redisCore);
        this.loader = new RdbLoader(redisCore);
        
        log.info("RdbManager使用RedisContext模式初始化完成，文件: {}", fileName);    }

    public boolean saveRdb() {
        return writer.writeRdb(fileName);
    }

    public boolean loadRdb() {
        return loader.loadRdb(new File(fileName));
    }

    public void close() {
        if (writer != null) {
            writer.close();
        }
    }

    public byte[] createTempRdbForReplication() {
        //1.创建一个临时的rdb文件
        String tempFileName = "temp-repl-" + System.currentTimeMillis() + ".rdb";
        String originalFileName = this.fileName;
        try {
            //2.临时设置文件名为临时文件
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
                //3.删除临时文件
                if (tempRdbFile.exists()) {
                    if (!tempRdbFile.delete()) {
                        log.warn("无法删除临时RDB文件: {}", tempRdbFile.getAbsolutePath());
                    }
                }
            }

        } finally {
            // 4.恢复原始文件名
            this.fileName = originalFileName;
        }

    }

    public boolean loadRdbFromBytes(byte[] rdbContent) {
        if (rdbContent == null || rdbContent.length == 0) {
            log.error("RDB内容为空，无法加载");
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
     * @return RDB快照的字节数组
     * @throws Exception 生成失败时抛出异常
     */
    public byte[] generateSnapshot() throws Exception {
        // 1. 生成临时文件名
        String tempFileName = fileName + ".temp." + UUID.randomUUID().toString();
        
        try {
            // 2. 写入临时文件
            boolean success = writer.writeRdb(tempFileName);
            if (!success) {
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


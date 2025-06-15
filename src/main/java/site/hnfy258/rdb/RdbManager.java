package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.server.context.RedisContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;

@Slf4j
public class RdbManager {
    private String fileName = RdbConstants.RDB_FILE_NAME;
    private RdbWriter writer;
    private RdbLoader loader;
    
    // 核心依赖：统一使用RedisContext
    private final RedisContext redisContext;

    /**
     * 新增构造函数：支持RedisContext的版本
     * 这是为了逐步迁移到统一上下文模式，解决循环依赖问题
     * 
     * @param redisContext Redis统一上下文，提供所有核心功能的访问接口
     */
    public RdbManager(final RedisContext redisContext) {
        this(redisContext, RdbConstants.RDB_FILE_NAME);
    }

    /**
     * 新增构造函数：支持RedisContext的完整版本
     * 
     * @param redisContext Redis统一上下文
     * @param fileName RDB文件名
     */    public RdbManager(final RedisContext redisContext, final String fileName) {
        this.redisContext = redisContext;
        this.fileName = fileName;
        
        // 1. 初始化RDB相关组件
        this.writer = new RdbWriter(redisContext);
        this.loader = new RdbLoader(redisContext);
        
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
}


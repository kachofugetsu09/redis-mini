package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.*;
import site.hnfy258.rdb.crc.Crc64OutputStream;

import java.io.*;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RDB文件写入器
 * 
 * <p>负责将Redis数据持久化到RDB文件，支持同步和异步两种保存模式。
 * 基于RedisCore接口设计，实现与具体Redis实现的解耦，
 * 提供高性能的数据持久化功能。
 * 
 * <p>支持的保存模式：
 * <ul>
 *     <li>同步保存 - {@link #writeRdb(String)} 直接在当前线程执行</li>
 *     <li>异步保存 - {@link #bgSaveRdb(String)} 在后台线程执行，类似Redis的BGSAVE</li>
 * </ul>
 * 
 * <p>异步保存特性：
 * <ul>
 *     <li>非阻塞执行 - 不影响主线程的正常服务</li>
 *     <li>数据一致性 - 使用数据库快照确保数据一致性</li>
 *     <li>状态监控 - 返回CompletableFuture便于状态跟踪</li>
 *     <li>CRC64校验 - 确保文件完整性</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class RdbWriter {
    
    /** Redis核心接口，提供数据库操作功能 */
    private final RedisCore redisCore;
    
    /** 运行状态标志，防止并发写入冲突 */
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    /** 后台保存线程池，支持异步BGSAVE操作 */
    private final ExecutorService bgSaveExecutor = new ThreadPoolExecutor(
        RdbConstants.BGSAVE_CORE_POOL_SIZE,
        RdbConstants.BGSAVE_MAX_POOL_SIZE,
        RdbConstants.BGSAVE_KEEP_ALIVE_TIME,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(RdbConstants.BGSAVE_QUEUE_SIZE),
            r -> new Thread(r, "RDB-BGSave-Thread")
    );

    /**
     * 构造函数
     * 
     * @param redisCore Redis核心接口
     */    public RdbWriter(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    /**
     * 同步写入RDB文件
     * 
     * <p>在当前线程中执行RDB文件写入，包含完整的CRC64校验流程。
     * 写入过程中会阻塞当前线程，适用于需要立即完成持久化的场景。
     * 
     * @param fileName RDB文件名
     * @return 写入是否成功
     * @throws IllegalArgumentException 如果文件名为null或空字符串
     */
    public boolean writeRdb(String fileName) {
        // 1. 参数验证
        if (fileName == null) {
            throw new IllegalArgumentException("文件名不能为null");
        }
        if (fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("文件名不能为空字符串");
        }
        
        // 2. 检查运行状态
        if (running.get()) {
            return false;
        }
        running.set(true);
        
        try (final Crc64OutputStream crc64Stream = new Crc64OutputStream(
                new BufferedOutputStream(new FileOutputStream(fileName)))) {
            
            final DataOutputStream dos = crc64Stream.getDataOutputStream();
            
            // 3. 写文件头
            RdbUtils.writeRdbHeader(dos);
            
            // 4. 写所有数据库
            saveAllDatabases(dos);
            
            // 5. 写文件末尾（包含CRC64校验和）
            RdbUtils.writeRdbFooter(crc64Stream);
            
            log.info("RDB同步保存完成，文件: {}，CRC64: 0x{}", fileName, Long.toHexString(crc64Stream.getCrc64()));
            
        } catch (Exception e) {
            log.error("RDB写入失败", e);
            return false;
        } finally {
            running.set(false);
        }
          return true;
    }

    /**
     * 异步后台保存RDB文件（BGSAVE）
     * 
     * <p>在后台线程中执行RDB文件写入，不阻塞当前线程。
     * 使用数据库快照确保数据一致性，适用于生产环境的持久化需求。
     * 类似于Redis的BGSAVE命令。
     * 
     * @param fileName RDB文件名
     * @return CompletableFuture，可用于检查保存状态和结果
     * @throws IllegalArgumentException 如果文件名为null或空字符串
     */
    public CompletableFuture<Boolean> bgSaveRdb(final String fileName) {
        // 1. 参数验证
        if (fileName == null) {
            throw new IllegalArgumentException("文件名不能为null");
        }
        if (fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("文件名不能为空字符串");
        }
        
        // 2. 原子性检查并设置运行状态
        if (!running.compareAndSet(false, true)) {
            log.warn("RDB保存正在进行中，跳过BGSAVE请求");
            return CompletableFuture.completedFuture(false);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // 3. 创建数据库快照
                log.info("开始创建数据库快照用于BGSAVE");
                final Map<Integer, Map<RedisBytes, RedisData>> dbSnapshots = createDatabaseSnapshots();
                
                // 4. 写入RDB文件（不再需要设置running状态，已经在上面设置了）
                return writeRdbFromSnapshotsInternal(fileName, dbSnapshots);
            } catch (final Exception e) {
                log.error("BGSAVE执行失败", e);
                return Boolean.FALSE;
            } finally {
                // 4. 确保运行状态被重置
                running.set(false);
            }        }, bgSaveExecutor);
    }

    /**
     * 创建所有数据库的快照
     * 
     * <p>遍历所有数据库，为每个非空数据库创建线程安全的数据快照。
     * 快照用于异步BGSAVE操作，确保数据一致性。
     * 
     * @return 数据库快照映射，键为数据库ID，值为数据快照
     */
    private Map<Integer, Map<RedisBytes, RedisData>> createDatabaseSnapshots() {
        final Map<Integer, Map<RedisBytes, RedisData>> snapshots = new java.util.HashMap<>();
        final RedisDB[] databases = redisCore.getDataBases();
        
        if (databases == null) {
            log.warn("数据库数组为null，返回空快照");
            return snapshots;
        }
        
        for (final RedisDB db : databases) {
            if (db != null && db.size() > 0 && db.getData() != null) {
                // 创建数据库快照（已经是线程安全的）
                final Map<RedisBytes, RedisData> snapshot = db.getData().createSafeSnapshot();
                if (snapshot != null) {
                    snapshots.put(db.getId(), snapshot);
                    log.debug("创建数据库{}快照，包含{}个键", db.getId(), snapshot.size());
                }
            }
        }
          return snapshots;
    }

    /**
     * 从快照写入RDB文件
     * 
     * <p>使用数据库快照写入RDB文件，不检查运行状态。
     * 内部方法，仅在快照已创建的情况下调用。
     * 
     * @param fileName RDB文件名
     * @param dbSnapshots 数据库快照映射
     * @return 写入是否成功
     */
    private boolean writeRdbFromSnapshotsInternal(final String fileName, 
                                                 final Map<Integer, Map<RedisBytes, RedisData>> dbSnapshots) {
        try (final Crc64OutputStream crc64Stream = new Crc64OutputStream(
                new BufferedOutputStream(new FileOutputStream(fileName)))) {
            
            final DataOutputStream dos = crc64Stream.getDataOutputStream();
            
            // 1. 写入RDB头部
            RdbUtils.writeRdbHeader(dos);
            
            // 2. 写入所有数据库快照
            for (final Map.Entry<Integer, Map<RedisBytes, RedisData>> entry : dbSnapshots.entrySet()) {
                final int dbId = entry.getKey();
                final Map<RedisBytes, RedisData> snapshot = entry.getValue();
                
                log.info("正在保存数据库快照: {}", dbId);
                writeDbFromSnapshot(dos, dbId, snapshot);
            }
            
            // 3. 写入RDB尾部（包含CRC64校验和）
            RdbUtils.writeRdbFooter(crc64Stream);
            
            log.info("BGSAVE完成，文件: {}，CRC64: 0x{}", fileName, Long.toHexString(crc64Stream.getCrc64()));
            return true;
            
        } catch (final IOException e) {
            log.error("BGSAVE写入失败", e);
            return false;
        }
    }    /**
     * 从快照写入单个数据库
     * 
     * @param dos 数据输出流
     * @param dbId 数据库ID
     * @param snapshot 数据库快照
     * @throws IOException IO异常
     */
    private void writeDbFromSnapshot(final DataOutputStream dos, final int dbId, 
                                   final Map<RedisBytes, RedisData> snapshot) throws IOException {
        // 1. 写入数据库选择命令
        RdbUtils.writeSelectDB(dos, dbId);
        
        // 2. 写入所有键值对
        for (final Map.Entry<RedisBytes, RedisData> entry : snapshot.entrySet()) {
            final RedisBytes key = entry.getKey();
            final RedisData value = entry.getValue();
            rdbSaveObject(dos, key, value);
        }
    }private void saveAllDatabases(DataOutputStream dos) throws IOException {
        RedisDB[] databases = redisCore.getDataBases();
        for(RedisDB db : databases){
           log.info("正在保存数据库: {}", db.getId());
           if(db.size() > 0){
               writeDb(dos,db);
           }
        }
    }    private void writeDb(DataOutputStream dos, RedisDB db) throws IOException {
        //1.写数据库id
        int databaseId = db.getId();
        RdbUtils.writeSelectDB(dos, databaseId);
        
        //2.写数据库数据 - 使用线程安全的快照
        Map<RedisBytes, RedisData> snapshot = db.getData().createSafeSnapshot();
        for (Map.Entry<RedisBytes, RedisData> entry : snapshot.entrySet()) {
            RedisBytes key = entry.getKey();
            RedisData value = entry.getValue();
            rdbSaveObject(dos, key, value);
        }
    }

    private void rdbSaveObject(DataOutputStream dos, RedisBytes key, RedisData value) throws IOException {
        switch(value.getClass().getSimpleName()){
            case "RedisString":
                RdbUtils.saveString(dos,key,(RedisString)value);
                log.info("保存字符串: {}", key);
                break;
            case "RedisList":
                RdbUtils.saveList(dos,key,(RedisList)value);
                log.info("保存列表: {}", key);
                break;
            case "RedisSet":
                RdbUtils.saveSet(dos,key,(RedisSet)value);
                log.info("保存集合: {}", key);
                break;
            case "RedisHash":
                RdbUtils.saveHash(dos,key,(RedisHash)value);
                log.info("保存哈希: {}", key);
                break;
            case "RedisZset":
                RdbUtils.saveZset(dos,key,(RedisZset)value);
                log.info("保存有序集合: {}", key);
                break;        }
    }

    /**
     * 关闭RDB写入器并清理资源
     * 
     * <p>优雅地关闭后台线程池，等待正在进行的BGSAVE任务完成。
     * 如果任务在超时时间内未完成，将强制终止。
     */
    public void close() {
        if (running.get()) {
            running.set(false);
        }
          // 1. 关闭后台保存线程池
        bgSaveExecutor.shutdown();
        
        try {
            // 2. 等待正在进行的任务完成
            if (!bgSaveExecutor.awaitTermination(
                    RdbConstants.BGSAVE_TIMEOUT_MS / 1000, TimeUnit.SECONDS)) {
                log.warn("BGSAVE线程池在{}秒内未能正常关闭，强制关闭", 
                        RdbConstants.BGSAVE_TIMEOUT_MS / 1000);
                bgSaveExecutor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            bgSaveExecutor.shutdownNow();
        }
    }
}

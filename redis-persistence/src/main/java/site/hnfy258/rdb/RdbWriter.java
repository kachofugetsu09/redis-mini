package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Dict;
import site.hnfy258.rdb.crc.Crc64OutputStream;

import java.io.*;
import java.util.*;
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
     * 注意：此方法会阻塞主线程，建议使用bgSaveRdb()进行异步保存。
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
        
        // 3. 尝试获取快照锁
        if (!redisCore.tryAcquireSnapshotLock("RDB")) {
            String currentType = redisCore.getCurrentSnapshotType();
            log.warn("无法获取快照锁，当前有{}快照操作正在进行", currentType != null ? currentType : "其他");
            return false;
        }
        
        running.set(true);
        
        try (final Crc64OutputStream crc64Stream = new Crc64OutputStream(
                new BufferedOutputStream(new FileOutputStream(fileName)))) {
            
            final DataOutputStream dos = crc64Stream.getDataOutputStream();
            
            // 4. 写文件头
            RdbUtils.writeRdbHeader(dos);
            
            // 5. 写所有数据库（同步方式，会阻塞主线程）
            saveAllDatabasesSync(dos);
            
            // 6. 写文件末尾（包含CRC64校验和）
            RdbUtils.writeRdbFooter(crc64Stream);
            
            log.info("RDB同步保存完成，文件: {}，CRC64: 0x{}", fileName, Long.toHexString(crc64Stream.getCrc64()));
            
        } catch (Exception e) {
            log.error("RDB写入失败", e);
            return false;
        } finally {
            running.set(false);
            
            // 释放快照锁
            if (redisCore != null) {
                redisCore.releaseSnapshotLock("RDB");
                log.debug("释放RDB快照锁");
            }
        }
          return true;
    }

    /**
     * 异步后台保存RDB文件（BGSAVE）
     * 
     * <p>完全在后台线程中执行快照创建和文件写入，不阻塞主线程。
     * 使用异步数据库快照确保数据一致性，适用于生产环境的持久化需求。
     * 类似于Redis的BGSAVE命令，真正实现非阻塞式持久化。
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
        
        // 3. 尝试获取快照锁
        if (!redisCore.tryAcquireSnapshotLock("RDB")) {
            String currentType = redisCore.getCurrentSnapshotType();
            log.warn("无法获取快照锁，当前有{}快照操作正在进行", currentType != null ? currentType : "其他");
            running.set(false);
            return CompletableFuture.completedFuture(false);
        }

        // 4. 在主线程中启动快照状态，确保时机正确
        try {
            final RedisDB[] databases = redisCore.getDataBases();
            if (databases != null) {
                for (final RedisDB db : databases) {
                    if (db != null && db.size() > 0 && db.getData() != null) {
                        // 在主线程中启动快照状态
                        db.getData().startSnapshot();
                        log.debug("数据库{}快照状态已启动", db.getId());
                    }
                }
            }
        } catch (Exception e) {
            log.error("启动快照状态失败", e);
            running.set(false);
            redisCore.releaseSnapshotLock("RDB");
            return CompletableFuture.completedFuture(false);
        }
        
        // 5. 在后台线程中执行快照迭代和文件写入
        return CompletableFuture.supplyAsync(() -> {
            log.info("开始在后台线程中直接迭代快照并写入RDB文件: {}", fileName);
            
            try {
                // 直接在后台线程中迭代快照并写入RDB文件
                boolean result = writeRdbFromDirectSnapshots(fileName);
                
                // 重要：写入完成后，统一调用finishSnapshot清理ForwardNode
                if (result) {
                    finishAllSnapshots();
                    log.debug("所有数据库快照状态已清理");
                } else {
                    log.error("BGSAVE失败，清理快照状态");
                    finishAllSnapshots();
                }
                
                return result;
                
            } catch (Exception e) {
                log.error("BGSAVE执行过程中发生异常", e);
                // 异常时也要清理快照状态
                finishAllSnapshots();
                return false;
            }
        }, bgSaveExecutor)
        .whenComplete((result, throwable) -> {
            // 5. 确保资源清理
            running.set(false);
            
            // 6. 释放快照锁
            if (redisCore != null) {
                redisCore.releaseSnapshotLock("RDB");
                log.debug("释放RDB快照锁");
            }
            
            if (throwable != null) {
                log.error("BGSAVE执行失败", throwable);
            } else {
                log.info("BGSAVE完成，结果: {}", result);
            }
        });
    }

    
    /**
     * 异步创建所有数据库的快照
     * 
     * <p>使用Dict的异步快照功能，完全在后台线程中执行，不阻塞主线程。
     * 每个数据库的快照创建都是异步的，可以并行执行。
     * 
     * @return 异步快照结果，包含Map和DictSnapshot的Future
     */
    private AsyncSnapshotResult createDatabaseSnapshotsAsync() {
        log.info("开始创建异步数据库快照");
        
        final Map<Integer, CompletableFuture<Map<RedisBytes, RedisData>>> snapshotFutures = new HashMap<>();
        final Map<Integer, CompletableFuture<site.hnfy258.internal.Dict.DictSnapshot<RedisBytes, RedisData>>> dictSnapshotFutures = new HashMap<>();
        final RedisDB[] databases = redisCore.getDataBases();
        
        if (databases == null) {
            log.warn("数据库数组为null，返回空快照Future映射");
            return new AsyncSnapshotResult(snapshotFutures, dictSnapshotFutures);
        }
        
        // 为每个数据库创建异步快照
        for (final RedisDB db : databases) {
            if (db != null && db.size() > 0 && db.getData() != null) {
                try {
                    // 使用Dict的异步快照功能，不阻塞主线程
                    CompletableFuture<site.hnfy258.internal.Dict.DictSnapshot<RedisBytes, RedisData>> snapshotFuture = 
                        db.getData().createRdbSnapshot();
                    
                    // 保存DictSnapshot的Future，用于后续调用finishSnapshot
                    dictSnapshotFutures.put(db.getId(), snapshotFuture);
                    
                    // 将DictSnapshot转换为Map，但不调用finishSnapshot()
                    CompletableFuture<Map<RedisBytes, RedisData>> mapFuture = 
                        snapshotFuture.thenApply(dictSnapshot -> {
                            try {
                                Map<RedisBytes, RedisData> result = dictSnapshot.toMap();
                                log.debug("数据库{}异步快照创建完成，包含{}个键", db.getId(), result.size());
                                
                                // 关键：不在这里调用finishSnapshot()，保持快照的冻结状态
                                // 快照的ForwardNode机制会确保主线程看到的是新值，快照看到的是旧值
                                
                                return result;
                            } catch (Exception e) {
                                log.error("转换数据库{}快照失败", db.getId(), e);
                                throw new RuntimeException("转换数据库快照失败", e);
                            }
                        });
                    
                    snapshotFutures.put(db.getId(), mapFuture);
                    
                } catch (Exception e) {
                    log.error("创建数据库{}异步快照失败", db.getId(), e);
                    throw new RuntimeException("创建数据库异步快照失败", e);
                }
            }
        }
        
        log.info("所有数据库异步快照启动完成，共{}个数据库", snapshotFutures.size());
        return new AsyncSnapshotResult(snapshotFutures, dictSnapshotFutures);
    }
    
    /**
     * 异步快照结果包装类
     */
    private static class AsyncSnapshotResult {
        final Map<Integer, CompletableFuture<Map<RedisBytes, RedisData>>> snapshotFutures;
        final Map<Integer, CompletableFuture<site.hnfy258.internal.Dict.DictSnapshot<RedisBytes, RedisData>>> dictSnapshotFutures;
        
        AsyncSnapshotResult(Map<Integer, CompletableFuture<Map<RedisBytes, RedisData>>> snapshotFutures,
                           Map<Integer, CompletableFuture<site.hnfy258.internal.Dict.DictSnapshot<RedisBytes, RedisData>>> dictSnapshotFutures) {
            this.snapshotFutures = snapshotFutures;
            this.dictSnapshotFutures = dictSnapshotFutures;
        }
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
        // 确保快照不为空
        if (dbSnapshots == null || dbSnapshots.isEmpty()) {
            log.error("数据库快照为空，无法写入RDB文件");
            return false;
        }

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
    }


    /**
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
    }

    /**
     * 同步方式保存所有数据库（会阻塞主线程）
     */
    private void saveAllDatabasesSync(DataOutputStream dos) throws IOException {
        RedisDB[] databases = redisCore.getDataBases();
        for(RedisDB db : databases){
           log.info("正在保存数据库: {}", db.getId());
           if(db.size() > 0){
               writeDbSync(dos,db);
           }
        }
    }
    
    /**
     * 同步方式写入单个数据库（会阻塞主线程）
     */
    private void writeDbSync(DataOutputStream dos, RedisDB db) throws IOException {
        //1.写数据库id
        int databaseId = db.getId();
        RdbUtils.writeSelectDB(dos, databaseId);
        
        //2.写数据库数据 - 使用线程安全的快照
        Map<RedisBytes, RedisData> snapshot = db.getData().createSnapshot();
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
//                log.info("保存字符串: {}", key);
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
    
    /**
     * 完成所有数据库的快照，清理ForwardNode状态
     * 
     * <p>遍历所有数据库，调用它们的finishSnapshot方法来清理
     * 快照期间产生的ForwardNode，应用新的写入操作。
     */
    private void finishAllSnapshots() {
        try {
            final RedisDB[] databases = redisCore.getDataBases();
            if (databases == null) {
                log.warn("数据库数组为null，无法完成快照清理");
                return;
            }
            
            int finishedCount = 0;
            for (final RedisDB db : databases) {
                if (db != null && db.getData() != null) {
                    try {
                        db.getData().finishSnapshot();
                        finishedCount++;
                        log.debug("数据库{}快照状态清理完成", db.getId());
                    } catch (Exception e) {
                        log.error("清理数据库{}快照状态失败", db.getId(), e);
                    }
                }
            }
            
            log.info("完成{}个数据库的快照状态清理", finishedCount);
            
        } catch (Exception e) {
            log.error("完成所有快照清理时发生异常", e);
        }
    }
    

    
    /**
     * 直接从快照迭代写入RDB文件
     * 
     * <p>仿照Dict内部的迭代逻辑，直接在快照状态下迭代数据并写入RDB文件。
     * 避免创建中间Map对象，减少内存占用，确保快照一致性。
     * 
     * @param fileName RDB文件名
     * @return 写入是否成功
     */
    private boolean writeRdbFromDirectSnapshots(final String fileName) {
        final RedisDB[] databases = redisCore.getDataBases();
        if (databases == null) {
            log.warn("数据库数组为null，无法写入RDB文件");
            return false;
        }

        try (final Crc64OutputStream crc64Stream = new Crc64OutputStream(
                new BufferedOutputStream(new FileOutputStream(fileName)))) {
            
            final DataOutputStream dos = crc64Stream.getDataOutputStream();
            
            // 1. 写入RDB头部
            RdbUtils.writeRdbHeader(dos);
            
            // 2. 为每个数据库直接迭代快照并写入
            for (final RedisDB db : databases) {
                if (db != null && db.size() > 0 && db.getData() != null) {
                    try {
                        log.info("开始直接迭代数据库{}快照并写入", db.getId());
                        
                        // 注意：不再调用startSnapshot()，因为已经在主线程中启动了
                        // 快照状态已经在主线程中启动，这里直接使用
                        
                        // 写入数据库选择命令
                        RdbUtils.writeSelectDB(dos, db.getId());
                        
                        // 直接迭代快照数据并写入
                        int writtenCount = writeDbFromDirectSnapshot(dos, db);
                        
                        log.info("数据库{}快照迭代完成，写入{}个键值对", db.getId(), writtenCount);
                        
                    } catch (Exception e) {
                        log.error("直接迭代数据库{}快照失败", db.getId(), e);
                        throw e;
                    }
                }
            }
            
            // 3. 写入RDB尾部（包含CRC64校验和）
            RdbUtils.writeRdbFooter(crc64Stream);
            
            log.info("直接快照迭代BGSAVE完成，文件: {}，CRC64: 0x{}", fileName, Long.toHexString(crc64Stream.getCrc64()));
            return true;
            
        } catch (final IOException e) {
            log.error("直接快照迭代BGSAVE写入失败", e);
            return false;
        }
    }
    
    /**
     * 直接从快照迭代写入单个数据库
     * 
     * <p>仿照Dict内部的快照迭代逻辑，直接遍历哈希表，
     * 对ForwardNode使用快照视图，对普通值直接使用。
     * 
     * @param dos 数据输出流
     * @param db 数据库对象
     * @return 写入的键值对数量
     * @throws IOException IO异常
     */
    private int writeDbFromDirectSnapshot(final DataOutputStream dos, final RedisDB db) throws IOException {
        int writtenCount = 0;
        
        // 获取Dict对象
        final Dict<RedisBytes, RedisData> dict = db.getData();
        

        final Map<RedisBytes, RedisData> snapshotData = new HashMap<>();

        try {
            // 直接调用Dict的内部迭代逻辑
            collectSnapshotData(dict, snapshotData);
            
            // 写入收集到的快照数据
            for (final Map.Entry<RedisBytes, RedisData> entry : snapshotData.entrySet()) {
                final RedisBytes key = entry.getKey();
                final RedisData value = entry.getValue();
                rdbSaveObject(dos, key, value);
                writtenCount++;
            }
            
        } catch (Exception e) {
            log.error("收集数据库{}快照数据失败", db.getId(), e);
            throw new IOException("收集快照数据失败", e);
        }
        
        return writtenCount;
    }
    
    /**
     * 收集快照数据
     * 
     * <p>使用反射或者Dict提供的接口来收集快照时刻的数据。
     * 这里仿照Dict内部的快照逻辑来实现。
     * 
     * @param dict Dict对象
     * @param snapshotData 用于收集快照数据的Map
     */
    private void collectSnapshotData(final site.hnfy258.internal.Dict<RedisBytes, RedisData> dict, 
                                    final Map<RedisBytes, RedisData> snapshotData) {
        try {

            final Map<RedisBytes, RedisData> tempSnapshot = dict.createSnapshotWithoutFinish();
            snapshotData.putAll(tempSnapshot);
            
        } catch (Exception e) {
            log.error("收集快照数据时发生异常", e);
            throw new RuntimeException("收集快照数据失败", e);
        }
    }
}

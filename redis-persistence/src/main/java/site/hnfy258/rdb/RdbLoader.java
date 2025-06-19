package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.rdb.crc.Crc64InputStream;

import java.io.*;

/**
 * RDB文件加载器
 * 
 * <p>负责从RDB文件加载Redis数据到内存中，支持CRC64校验和验证。
 * 基于RedisCore接口设计，实现与具体Redis实现的解耦，
 * 提供可靠的数据恢复功能。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>RDB文件格式验证 - 检查文件头和版本兼容性</li>
 *     <li>数据完整性校验 - 使用CRC64算法验证数据完整性</li>
 *     <li>多数据库支持 - 支持Redis的多数据库结构</li>
 *     <li>数据类型还原 - 支持字符串、列表、集合、哈希、有序集合</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class RdbLoader {
    
    /** Redis核心接口，提供数据库操作功能 */
    private final RedisCore redisCore;

    /**
     * 构造函数
     * 
     * @param redisCore Redis核心接口
     */
    public RdbLoader(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    /**
     * 加载RDB文件
     * 
     * <p>从指定文件加载Redis数据，包含完整的CRC64校验流程。
     * 如果文件不存在，返回true（视为空数据库）；
     * 如果加载失败或校验失败，返回false。
     * 
     * @param file RDB文件
     * @return 加载是否成功
     */
    public boolean loadRdb(File file) {
        if (!file.exists()) {
            log.info("RDB文件不存在: {}", file.getAbsolutePath());
            return true;
        }
        
        try (final Crc64InputStream crc64Stream = new Crc64InputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            
            // 1. 检查头部
            if (!RdbUtils.checkRdbHeader(crc64Stream)) {
                log.error("RDB文件头不正确");
                return false;
            }
            
            // 2. 读取所有数据库
            loadAllDatabases(crc64Stream.getDataInputStream());
            
            // 3. 验证CRC64校验和
            if (!RdbUtils.verifyCrc64Checksum(crc64Stream)) {
                log.error("RDB文件CRC64校验失败，数据可能已损坏");
                return false;
            }
            
            log.info("RDB文件加载成功，CRC64校验通过: {}", file.getAbsolutePath());
            
        } catch (Exception e) {
            log.error("RDB文件加载失败", e);
            return false;
        }
          return true;
    }

    /**
     * 加载所有数据库
     * 
     * <p>解析RDB文件中的数据库内容，根据操作码处理不同类型的数据。
     * 支持数据库切换和各种Redis数据类型的加载。
     * 
     * @param dis 数据输入流
     * @throws IOException 如果读取失败
     */
    private void loadAllDatabases(DataInputStream dis) throws IOException {        int currentDbIndex = 0;
        while (true) {
            int type = dis.read();
            if (type == -1 || (byte) type == RdbConstants.RDB_OPCODE_EOF) {
                break;
            }
            
            switch ((byte) type) {
                case RdbConstants.RDB_OPCODE_SELECTDB:
                    currentDbIndex = dis.read();
                    redisCore.selectDB(currentDbIndex);
                    log.info("选择数据库: {}", currentDbIndex);
                    break;
                case RdbConstants.STRING_TYPE:
                    RdbUtils.loadString(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.LIST_TYPE:
                    RdbUtils.loadList(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.SET_TYPE:
                    RdbUtils.loadSet(dis, redisCore, currentDbIndex);
                    break;
                case RdbConstants.HASH_TYPE:
                    RdbUtils.loadHash(dis, redisCore, currentDbIndex);
                    break;                case RdbConstants.ZSET_TYPE:
                    RdbUtils.loadZSet(dis, redisCore, currentDbIndex);
                    break;
                    
                default:
                    log.warn("不支持的数据类型: {}", type);
                    break;
            }
        }
    }

}

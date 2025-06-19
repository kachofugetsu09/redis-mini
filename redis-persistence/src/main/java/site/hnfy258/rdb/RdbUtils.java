package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Dict;
import site.hnfy258.internal.Sds;
import site.hnfy258.rdb.crc.Crc64InputStream;
import site.hnfy258.rdb.crc.Crc64OutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RDB文件操作工具类
 * 
 * <p>提供RDB文件读写的底层工具方法，包括文件头尾处理、
 * 数据类型序列化和反序列化、CRC64校验等功能。
 * 所有方法均为静态方法，支持各种Redis数据类型的RDB格式转换。
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>文件格式处理 - RDB文件头、尾部、数据库选择</li>
 *     <li>数据类型转换 - 字符串、列表、集合、哈希、有序集合</li>
 *     <li>校验和验证 - CRC64校验和计算和验证</li>
 *     <li>编码格式支持 - 长度编码、字符串编码等</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class RdbUtils {

    /**
     * 写入RDB文件头部
     * 
     * @param dos 数据输出流
     * @throws IOException 如果发生IO错误
     */    public static void writeRdbHeader(DataOutputStream dos) throws IOException {
        dos.writeBytes("REDIS0009");
    }

    /**
     * 写入RDB文件尾部
     * 
     * <p>包含EOF标记和CRC64校验和的写入。
     * EOF标记会被计入CRC64计算，而校验和本身不会。
     * 
     * @param crc64OutputStream CRC64输出流，用于获取校验和并写入
     * @throws IOException 如果发生IO错误
     */
    public static void writeRdbFooter(Crc64OutputStream crc64OutputStream) throws IOException {
        DataOutputStream dos = crc64OutputStream.getDataOutputStream();
        // 1. 写入EOF标记
        dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
        
        // 2. 写入CRC64校验和（小端序，8字节）
        crc64OutputStream.writeCrc64Checksum();
          log.debug("RDB尾部写入完成，CRC64校验和: 0x{}", Long.toHexString(crc64OutputStream.getCrc64()));
    }

    /**
     * 写入数据库选择指令
     * 
     * @param dos 数据输出流
     * @param databaseId 数据库ID
     * @throws IOException 如果发生IO错误
     */
    public static void writeSelectDB(DataOutputStream dos, int databaseId) throws IOException {
        dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);        writeLength(dos, databaseId);
    }

    /**
     * 写入长度编码
     * 
     * <p>Redis RDB使用特殊的长度编码格式：
     * - 0-63: 直接用1字节表示
     * - 64-16383: 用2字节表示（高2位为01）
     * - 其他: 用5字节表示（首字节0x80 + 4字节整数）
     * 
     * @param dos 数据输出流
     * @param length 要编码的长度
     * @throws IOException 如果发生IO错误
     */
    private static void writeLength(DataOutputStream dos, int length) throws IOException {        if (length < 0x40) {
            dos.writeByte(length);
        } else if (length < 0x4000) {
            dos.writeByte(length | 0x4000);
        } else {
            dos.writeByte(0x80);
            dos.writeByte(length);
        }
    }

    /**
     * 读取长度编码
     * 
     * <p>解析Redis RDB的长度编码格式。
     * 
     * @param dis 数据输入流
     * @return 解析出的长度值
     * @throws IOException 如果发生IO错误
     */
    private static long readLength(DataInputStream dis) throws IOException {        int firstByte = dis.readByte() & 0xFF;
        int type = (firstByte & 0xC0) >> 6;
        switch (type) {
            case 0:
                return firstByte & 0x3F;
            case 1:
                int secondByte = dis.readByte() & 0xFF;
                return ((firstByte & 0x3F) << 8) | secondByte;
            case 2:
                return dis.readInt();
            default:
                throw new RuntimeException("长度编码读取错误");
        }
    }

    /**
     * 保存字符串类型数据
     * 
     * @param dos 数据输出流
     * @param key 键
     * @param value 字符串值
     * @throws IOException 如果发生IO错误
     */    public static void saveString(DataOutputStream dos, RedisBytes key, RedisString value) throws IOException {
        dos.writeByte(RdbConstants.STRING_TYPE);
        writeString(dos, key.getBytes());
        writeString(dos, value.getValue().getBytes());
    }

    /**
     * 加载字符串类型数据
     * 
     * @param dis 数据输入流
     * @param redisCore Redis核心接口
     * @param currentDBIndex 当前数据库索引
     * @throws IOException 如果发生IO错误
     */
    public static void loadString(DataInputStream dis, RedisCore redisCore, int currentDBIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        RedisBytes value = new RedisBytes(RdbUtils.readString(dis));
        RedisString redisString = new RedisString(Sds.create(value.getBytes()));        redisCore.selectDB(currentDBIndex);
        redisCore.put(key, redisString);
        log.info("加载键值对到数据库{}:{}->{}",currentDBIndex, key.getString(), value.getString());
    }

    /**
     * 写入字符串数据
     * 
     * @param dos 数据输出流
     * @param bytes 字节数组
     * @throws IOException 如果发生IO错误
     */
    private static void writeString(DataOutputStream dos, byte[] bytes) throws IOException {        writeLength(dos, bytes.length);
        dos.write(bytes);
    }

    /**
     * 读取字符串数据
     * 
     * @param dis 数据输入流
     * @return 读取的字节数组
     * @throws IOException 如果发生IO错误
     */
    private static byte[] readString(DataInputStream dis) throws IOException {
        long length = readLength(dis);
        if (length < 0) {
            throw new RuntimeException("字符串长度读取错误");
        }
        int len = (int) length;
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return bytes;
    }

    /**
     * 检查RDB文件头部
     * 
     * @param dis 数据输入流
     * @return 文件头是否正确
     * @throws IOException 如果发生IO错误
     */
    public static boolean checkRdbHeader(DataInputStream dis) throws IOException {
        byte[] header = new byte[9];
        int read = dis.read(header);
        return read == 9 && new String(header).equals("REDIS0009");
    }

    /**
     * 检查RDB文件头部（使用CRC64流）
     * 
     * @param crc64InputStream CRC64输入流
     * @return 文件头是否正确
     * @throws IOException 如果发生IO错误
     */
    public static boolean checkRdbHeader(Crc64InputStream crc64InputStream) throws IOException {
        DataInputStream dis = crc64InputStream.getDataInputStream();
        byte[] header = new byte[9];
        int read = dis.read(header);
        boolean valid = read == 9 && new String(header).equals("REDIS0009");
        
        if (valid) {
            log.debug("RDB头部验证成功: REDIS0009");
        } else {
            log.warn("RDB头部验证失败，读取到: {}", read == 9 ? new String(header) : "不完整数据");
        }
        
        return valid;
    }

    /**
     * 验证RDB文件的CRC64校验和
     * 
     * <p>读取文件末尾的校验和并与计算值比较，
     * 确保文件在传输或存储过程中没有损坏。
     * 
     * @param crc64InputStream CRC64输入流
     * @return 校验和是否正确
     * @throws IOException 如果发生IO错误
     */
    public static boolean verifyCrc64Checksum(Crc64InputStream crc64InputStream) throws IOException {
        // 读取文件末尾的CRC64校验和
        long expectedChecksum = crc64InputStream.readCrc64Checksum();
        
        // 获取实际计算的CRC64值
        long actualChecksum = crc64InputStream.getCrc64();
        
        boolean valid = expectedChecksum == actualChecksum;
        
        if (valid) {
            log.info("CRC64校验成功: 0x{}", Long.toHexString(actualChecksum));
        } else {
            log.error("CRC64校验失败! 期望: 0x{}, 实际: 0x{}", 
                     Long.toHexString(expectedChecksum), Long.toHexString(actualChecksum));
        }
          return valid;
    }

    /**
     * 保存列表类型数据
     * 
     * @param dos 数据输出流
     * @param key 键
     * @param value 列表值
     * @throws IOException 如果发生IO错误
     */
    public static void saveList(DataOutputStream dos, RedisBytes key, RedisList value) throws IOException {        dos.writeByte(RdbConstants.LIST_TYPE);
        writeString(dos, key.getBytes());
        writeLength(dos, value.size());
        for (RedisBytes bytes : value.getAll()) {
            writeString(dos, bytes.getBytesUnsafe());
        }
    }

    /**
     * 加载列表类型数据
     * 
     * @param dis 数据输入流
     * @param redisCore Redis核心接口
     * @param currentDbIndex 当前数据库索引
     * @throws IOException 如果发生IO错误
     */
    public static void loadList(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisList redisList = new RedisList();
        for (int i = 0; i < size; i++) {
            redisList.lpush(new RedisBytes(RdbUtils.readString(dis)));
        }
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisList);
        log.info("加载列表到数据库{}:{}->{}",currentDbIndex, key.getString(), redisList.lrange(0, (int) (size-1)));
    }

    /**
     * 保存哈希表类型数据
     * 
     * @param dos 数据输出流
     * @param key 键
     * @param value 哈希表值
     * @throws IOException 如果发生IO错误
     */
    public static void saveHash(DataOutputStream dos, RedisBytes key, RedisHash value) throws IOException {        dos.writeByte(RdbConstants.HASH_TYPE);
        writeString(dos, key.getBytes());
        Dict<RedisBytes, RedisBytes> hash = value.getHash();
        writeLength(dos, hash.size());

        // 使用线程安全的快照避免并发问题
        Map<RedisBytes, RedisBytes> snapshot = hash.createSafeSnapshot();
        for (Map.Entry<RedisBytes, RedisBytes> entry : snapshot.entrySet()) {
            writeString(dos, entry.getKey().getBytesUnsafe());
            writeString(dos, entry.getValue().getBytesUnsafe());
        }
        log.info("保存哈希表: {}", key);
    }

    /**
     * 加载哈希表类型数据
     * 
     * @param dis 数据输入流
     * @param redisCore Redis核心接口
     * @param currentDbIndex 当前数据库索引
     * @throws IOException 如果发生IO错误
     */
    public static void loadHash(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisHash redisHash = new RedisHash();
        for (int i = 0; i < size; i++) {
            RedisBytes field = new RedisBytes(RdbUtils.readString(dis));
            RedisBytes value = new RedisBytes(RdbUtils.readString(dis));
            redisHash.put(field, value);
        }
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisHash);
        log.info("加载哈希表到数据库{}:{}->{}",currentDbIndex, key.getString(), redisHash.getHash());
    }

    /**
     * 保存集合类型数据
     * 
     * @param dos 数据输出流
     * @param key 键
     * @param value 集合值
     * @throws IOException 如果发生IO错误
     */
    public static void saveSet(DataOutputStream dos, RedisBytes key, RedisSet value) throws IOException {        dos.writeByte(RdbConstants.SET_TYPE);
        writeString(dos, key.getBytes());
        writeLength(dos, value.size());
        for (RedisBytes bytes : value.getAll()) {
            writeString(dos, bytes.getBytesUnsafe());
        }
        log.info("保存集合: {}", key);
    }

    /**
     * 加载集合类型数据
     * 
     * @param dis 数据输入流
     * @param redisCore Redis核心接口
     * @param currentDbIndex 当前数据库索引
     * @throws IOException 如果发生IO错误
     */
    public static void loadSet(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisSet redisSet = new RedisSet();
        List<RedisBytes> temp = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            temp.add(new RedisBytes(readString(dis)));
        }
        redisSet.add(temp);
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisSet);
        log.info("加载集合到数据库{}:{}->{}",currentDbIndex, key.getString(), redisSet.getAll());
    }

    /**
     * 保存有序集合类型数据
     * 
     * @param dos 数据输出流
     * @param key 键
     * @param value 有序集合值
     * @throws IOException 如果发生IO错误
     */
    public static void saveZset(DataOutputStream dos, RedisBytes key, RedisZset value) throws IOException {        dos.writeByte(RdbConstants.ZSET_TYPE);
        writeString(dos, key.getBytes());
        int size = value.size();
        writeLength(dos, size);

        Iterable<? extends Map.Entry<String, Double>> entries = value.getAll();
        for (Map.Entry<String, Double> entry : entries) {
            // 保存分数
            writeString(dos, String.valueOf(entry.getValue()).getBytes());
            // 保存成员（现在entry.getKey()是成员，entry.getValue()是分数）
            writeString(dos, RedisBytes.fromString(entry.getKey()).getBytesUnsafe());
        }
        log.info("保存有序集合: {}", key);
    }

    /**
     * 加载有序集合类型数据
     * 
     * @param dis 数据输入流
     * @param redisCore Redis核心接口
     * @param currentDbIndex 当前数据库索引
     * @throws IOException 如果发生IO错误
     */
    public static void loadZSet(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisZset redisZset = new RedisZset();
        for(int i=0;i<size;i++){
            RedisBytes score = new RedisBytes(RdbUtils.readString(dis));
            RedisBytes member = new RedisBytes(RdbUtils.readString(dis));
            redisZset.add(Double.parseDouble(new String(score.getBytes())), member);
        }
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisZset);
        log.info("加载有序集合到数据库{}:{}->{}",currentDbIndex,key.getString(), redisZset.getAll());
    }
}

package site.hnfy258.rdb;

import lombok.extern.slf4j.Slf4j;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Dict;
import site.hnfy258.internal.Sds;
import site.hnfy258.server.core.RedisCore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RdbUtils {
    public static void writeRdbHeader(DataOutputStream dos) throws IOException {
        dos.writeBytes("REDIS0009");
    }

    public static void writeRdbFooter(DataOutputStream dos) throws IOException {
        dos.writeByte(RdbConstants.RDB_OPCODE_EOF);
        //todo 添加校验和机制
        dos.writeLong(0);
    }

    public static void writeSelectDB(DataOutputStream dos, int databaseId) throws IOException {
        dos.writeByte(RdbConstants.RDB_OPCODE_SELECTDB);
        writeLength(dos, databaseId);
    }

    private static void writeLength(DataOutputStream dos, int length) throws IOException {
        if(length <0x40){
            dos.writeByte(length);
        }
        else if(length <0x4000){
            dos.writeByte(length|0x4000);
        }
        else{
            dos.writeByte(0x80);
            dos.writeByte(length);
        }
    }

    private static long readLength(DataInputStream dis) throws IOException {
        int firstByte = dis.readByte()&0xFF;
        int type = (firstByte & 0xC0)>>6;
        switch(type){
            case 0:
                return firstByte & 0x3F;
            case 1:
                int secondByte = dis.readByte() & 0xFF;
                return ((firstByte & 0x3F) << 8) | secondByte;
            case 2:
                return dis.readInt();
            default:
                throw new RuntimeException("read length error");
        }
    }

    public static void saveString(DataOutputStream dos, RedisBytes key, RedisString value) throws IOException {
        dos.writeByte(RdbConstants.STRING_TYPE);
        writeString(dos,key.getBytes());
        writeString(dos,value.getValue().getBytes());
    }

    public static void loadString(DataInputStream dis, RedisCore redisCore,int currentDBIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        RedisBytes value = new RedisBytes(RdbUtils.readString(dis));
        RedisString redisString = new RedisString(new Sds(value.getBytes()));
        redisCore.selectDB(currentDBIndex);
        redisCore.put(key, redisString);
        log.info("加载键值对到数据库{}:{}->{}",currentDBIndex,key.getString(), value.getString());
    }



    private static void writeString(DataOutputStream dos, byte[] bytes) throws IOException {
        writeLength(dos,bytes.length);
        dos.write(bytes);
    }

    private static byte[] readString(DataInputStream dis) throws IOException {
        long length  = readLength(dis);
        if(length < 0){
            throw new RuntimeException("read string length error");
        }
        int len = (int) length;
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return bytes;
    }



    public static boolean checkRdbHeader(DataInputStream dis) throws IOException {
        byte[] header = new byte[9];
        int read = dis.read(header);
        return read == 9 && new String(header).equals("REDIS0009");
    }


    public static void saveList(DataOutputStream dos, RedisBytes key, RedisList value) throws IOException {
        dos.writeByte(RdbConstants.LIST_TYPE);
        writeString(dos,key.getBytes());
        writeLength(dos,value.size());
       for(RedisBytes bytes: value.getAll()){
            writeString(dos,bytes.getBytes());
       }
    }

    public static void loadList(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisList redisList = new RedisList();
        for(int i=0;i<size;i++){
            redisList.lpush(new RedisBytes(RdbUtils.readString(dis)));
        }
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisList);
        log.info("加载列表到数据库{}:{}->{}",currentDbIndex,key.getString(), redisList.lrange(0, (int) (size-1)));

    }

    public static void saveHash(DataOutputStream dos, RedisBytes key, RedisHash value) throws IOException {
        dos.writeByte(RdbConstants.HASH_TYPE);
        writeString(dos,key.getBytes());
        Dict<RedisBytes,RedisBytes> hash = value.getHash();
        writeLength(dos,hash.size());
        for(Map.Entry<Object, Object> entry : hash.entrySet()){
            writeString(dos,((RedisBytes)entry.getKey()).getBytes());
            writeString(dos,((RedisBytes)entry.getValue()).getBytes());
        }
        log.info("保存哈希表: {}", key);
    }

    public static void loadHash(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisHash redisHash = new RedisHash();
        for(int i=0;i<size;i++){
            RedisBytes field = new RedisBytes(RdbUtils.readString(dis));
            RedisBytes value = new RedisBytes(RdbUtils.readString(dis));
            redisHash.put(field, value);
        }
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisHash);
        log.info("加载哈希表到数据库{}:{}->{}",currentDbIndex,key.getString(), redisHash.getHash());
    }

    public static void saveSet(DataOutputStream dos, RedisBytes key, RedisSet value) throws IOException {
        dos.writeByte(RdbConstants.SET_TYPE);
        writeString(dos,key.getBytes());
        writeLength(dos,value.size());
        for(RedisBytes bytes: value.getAll()){
            writeString(dos,bytes.getBytes());
        }
        log.info("保存集合: {}", key);
    }

    public static void loadSet(DataInputStream dis, RedisCore redisCore, int currentDbIndex) throws IOException {
        RedisBytes key = new RedisBytes(RdbUtils.readString(dis));
        long size = RdbUtils.readLength(dis);
        RedisSet redisSet = new RedisSet();
        List<RedisBytes> temp = new ArrayList();
        for(int i=0;i<size;i++){
            temp.add(new RedisBytes(readString(dis)));
        }
        redisSet.add(temp);
        redisCore.selectDB(currentDbIndex);
        redisCore.put(key, redisSet);
        log.info("加载集合到数据库{}:{}->{}",currentDbIndex,key.getString(), redisSet.getAll());
    }

    public static void saveZset(DataOutputStream dos, RedisBytes key, RedisZset value) throws IOException {
        dos.writeByte(RdbConstants.ZSET_TYPE);
        writeString(dos,key.getBytes());
        int size = value.size();
        writeLength(dos,size);
        @SuppressWarnings("unchecked")
        Iterable<? extends Map.Entry<Double, Object>> entries = value.getAll();
        for(Map.Entry<Double, Object> entry : entries){
            writeString(dos,String.valueOf(entry.getKey()).getBytes());
            writeString(dos,((RedisBytes)entry.getValue()).getBytes());
        }
        log.info("保存有序集合: {}", key);
    }


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

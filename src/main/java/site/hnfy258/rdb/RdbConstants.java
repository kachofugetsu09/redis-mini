package site.hnfy258.rdb;

public class RdbConstants {
    public static final String RDB_FILE_NAME = "dump.rdb";
    public static final byte RDB_OPCODE_EOF = (byte) 255;
    public static final byte STRING_TYPE = (byte)0;
    public static final byte LIST_TYPE = (byte)1;
    public static final byte SET_TYPE = (byte)2;
    public static final byte ZSET_TYPE = (byte)3;
    public static final byte HASH_TYPE = (byte)4;
    public static final byte RDB_OPCODE_SELECTDB = (byte)254;
}

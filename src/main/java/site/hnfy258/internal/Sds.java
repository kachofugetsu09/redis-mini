package site.hnfy258.internal;

import site.hnfy258.datastructure.RedisBytes;

public class Sds {
    private byte[] bytes;
    private int len;
    private int alloc;

    private static final int SDS_MAX_PREALLOC = 1024 * 1024; // 1M

    public Sds(byte[] bytes) {
        this.len = bytes.length;
        this.alloc = calculateAlloc(bytes.length);
        this.bytes = new byte[this.alloc];
        System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
    }

    private int calculateAlloc(int length) {
        if(length <= SDS_MAX_PREALLOC){
            return Math.max(length*2, 8);
        }
        return length+SDS_MAX_PREALLOC;
    }


    public String toString(){
        return new String(this.bytes,  RedisBytes.CHARSET);
    }

    public int length(){
        return len;
    }

    public void setBytes(byte[] bytes){
        this.bytes = bytes;
    }

    public void clear(){
        this.len = 0;
    }

    public Sds append(byte[] extra){
        int newLen = len + extra.length;
        if(newLen > this.alloc){
            int newAlloc = calculateAlloc(newLen);
            byte[] newBytes = new byte[newAlloc];
            System.arraycopy(bytes, 0, newBytes, 0, len);
            bytes = newBytes;
            this.alloc = newAlloc;
            this.len = newLen;
        }
        return this;
    }

    public Sds append(String str){
        return append(str.getBytes());
    }

    public byte[] getBytes(){
        byte[] result = new byte[len];
        System.arraycopy(bytes, 0, result, 0, len);
        return result;
    }

}


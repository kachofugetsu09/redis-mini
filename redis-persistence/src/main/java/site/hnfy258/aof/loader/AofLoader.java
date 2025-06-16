package site.hnfy258.aof.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
@Slf4j
public class AofLoader {
    private final String fileName;
    private final RedisCore redisCore;
    private FileChannel channel;
    private RandomAccessFile raf;
    private static final int BUFFER_SIZE = 4096;

    private static final byte[] REDIS_PREFIX = {'*', '$', '+', '-', ':'};

    /**
     * 构造函数：基于RedisCore接口的解耦架构
     * 
     * @param fileName AOF文件名
     * @param redisCore Redis核心接口
     * @throws Exception 文件操作异常
     */
    public AofLoader(final String fileName, final RedisCore redisCore) throws Exception {
        this.fileName = fileName;
        this.redisCore = redisCore;
        openFile();
    }
    /**
     * 安全打开文件
     */
    public void openFile() throws IOException {
        final File file = new File(fileName);
        if (!file.exists() || file.length() == 0) {
            log.info("AOF文件不存在或为空: {}", fileName);
            return;
        }
        
        try {
            this.raf = new RandomAccessFile(file, "r");
            this.channel = raf.getChannel();
            log.info("AOF文件打开成功: {}, 文件大小: {} bytes", fileName, channel.size());
        } catch (IOException e) {
            // 如果打开失败，确保资源被清理
            closeFile();
            throw new IOException("打开AOF文件失败: " + fileName, e);
        }
    }

    public void load(){
        if(channel == null){
            log.info("channel为空");
            return;
        }
        ByteBuf commands = null;
        try{
            log.info("开始加载aof文件");
            commands = readFileContent();
            int succesCount = processCommands(commands);
            log.info("加载aof文件成功,成功加载{}条命令",succesCount);
        }catch(Exception e){
            log.error("加载aof文件失败");
            throw new RuntimeException(e);
        }finally {
            if(commands !=null && commands.refCnt() >0){
                commands.release();
            }
            closeFile();
        }
    }


    private int processCommands(ByteBuf commands) {
        int succesCount = 0;
        while(commands.isReadable()){
            int position = commands.readerIndex();
            commands.markReaderIndex();
            try{
                Resp command = Resp.decode(commands);
                if(executeCommand(command,position)){
                    succesCount++;
                }
            }catch(Exception e){
                handleCommandError(commands, position,e);
            }
        }
        return succesCount;
    }

    private void handleCommandError(ByteBuf commands, int position, Exception e) {
        if (!commands.isReadable()) {
            return;
        }
        
        log.warn("命令执行错误，在{}", position);
        commands.resetReaderIndex();

        while(commands.isReadable()) {
            byte b = commands.readByte();
            if(isRespPrefix(b)) {
                commands.readerIndex(commands.readerIndex() - 1);
                break;
            }
        }
    }

    private boolean isRespPrefix(byte b){
        for(byte prefix: REDIS_PREFIX){
            if(prefix == b) return true;
        }
        return false;
    }

    private boolean executeCommand(Resp respArray,int position) {
        if(!(respArray instanceof RespArray)){
            log.warn("命令不是RespArray类型，在{}",position);
            return false;
        }
        RespArray command = (RespArray) respArray;
        if(!isValiedCommand(command)){
            log.warn("命令无效，在{}",position);
            return false;
        }
        return executeRedisCommand(command,position);
    }    /**
     * 执行Redis命令 - 通过RedisCore接口实现解耦
     * 
     * @param command RESP数组格式的命令
     * @param position 命令在文件中的位置（用于日志）
     * @return 命令是否执行成功
     */
    private boolean executeRedisCommand(final RespArray command, final int position) {
        try {
            // 1. 解析命令名称
            final String commandName = ((BulkString) command.getContent()[0])
                    .getContent().getString().toUpperCase();
            
            // 2. 解析命令参数
            final Resp[] content = command.getContent();
            final String[] args = new String[content.length - 1];
            for (int i = 1; i < content.length; i++) {
                if (content[i] instanceof BulkString) {
                    args[i - 1] = ((BulkString) content[i]).getContent().getString();
                } else {
                    log.warn("命令参数格式错误，位置: {}", position);
                    return false;
                }
            }
            
            // 3. 通过RedisCore接口执行命令
            // 这样避免了对server层CommandType和Command的依赖
            final boolean success = redisCore.executeCommand(commandName, args);
            
            if (!success) {
                log.warn("命令执行失败: {} 在位置: {}", commandName, position);
            }
            
            return success;
        } catch (Exception e) {
            log.error("命令执行异常，位置: {}", position, e);
            return false;
        }
    }

    private boolean isValiedCommand(RespArray command) {
        Resp[] content = command.getContent();
        return content.length > 0 && content[0] instanceof BulkString;
    }

    private ByteBuf readFileContent() throws IOException{
        long fileSize = channel.size();
        if(fileSize == 0){
            return Unpooled.EMPTY_BUFFER;
        }

        if(fileSize > Integer.MAX_VALUE){
            return readLargeFile();
        }
        int size = (int) fileSize;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(size);
        try{
            ByteBuffer byteBuffer = buffer.nioBuffer(0, size);
            int totalRead = 0;
            while(totalRead < size){
                int read = channel.read(byteBuffer);
                if(read == -1){
                    break;
                }
                totalRead += read;
            }
            buffer.writerIndex(totalRead);
            return buffer;
        }catch(IOException e){
            buffer.release();
            throw e;
        }
    }

    private ByteBuf readLargeFile() {
        CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        ByteBuf currentBuf = null;
        try{
            long reamining = channel.size();
            while(reamining > 0){
                int chunkSize = Math.min(BUFFER_SIZE, (int) reamining);
                currentBuf = PooledByteBufAllocator.DEFAULT.directBuffer(chunkSize);
                ByteBuffer byteBuffer = currentBuf.nioBuffer(0, chunkSize);
                int read = channel.read(byteBuffer);
                if(read == -1){
                    currentBuf.release();
                    break;
                }
                currentBuf.writerIndex(read);
                composite.addComponent(true,currentBuf);
                reamining -= read;
                currentBuf = null;
            }
            return composite;
        }catch(IOException e){
            if(currentBuf != null){
                currentBuf.release();
            }
            composite.release();
            throw new RuntimeException(e);
        }
    }    /**
     * 安全关闭文件资源
     */
    public void closeFile() {
        try {
            if (channel != null) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (IOException e) {
                    log.warn("关闭FileChannel时发生错误: {}", e.getMessage());
                } finally {
                    channel = null;
                }
            }
            
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    log.warn("关闭RandomAccessFile时发生错误: {}", e.getMessage());
                } finally {
                    raf = null;
                }
            }
            
            log.debug("AOF文件资源已安全关闭");
            
        } catch (Exception e) {
            log.error("关闭AOF文件时发生意外错误: {}", e.getMessage());
        }
    }

    /**
     * 关闭加载器
     */
    public void close() {
        closeFile();
        log.info("AofLoader 已关闭");
    }

}

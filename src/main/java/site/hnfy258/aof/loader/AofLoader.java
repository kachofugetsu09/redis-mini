package site.hnfy258.aof.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.core.RedisCore;

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

    public AofLoader(String fileName, RedisCore redisCore) throws Exception {
        this.fileName = fileName;
        this.redisCore = redisCore;
       openFile();
    }

    public void openFile() throws IOException {
        File file = new File(fileName);
        if(!file.exists() || file.length() ==0){
            log.info("aof文件不存在");
            return;
        }
        this.raf = new RandomAccessFile(file, "r");
        this.channel = raf.getChannel();
    }

    public void load(){
        if(channel == null){
            log.info("channel为空");
            return;
        }
        try{
            log.info("开始加载aof文件");
            ByteBuf commands = readFileContent();
            int succesCount = processCommands(commands);
            log.info("加载aof文件成功,成功加载{}条命令",succesCount);
        }catch(Exception e){
            log.error("加载aof文件失败");
            throw new RuntimeException(e);
        }finally {
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
        log.warn("命令执行错误，在{}",position,e.getMessage());
        commands.resetReaderIndex();

        while(commands.isReadable()){
            byte b = commands.readByte();
            if(isRespPrefix(b)){
                commands.readerIndex(commands.readerIndex() -1);
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
    }

    private boolean executeRedisCommand(RespArray command,int position) {
        try{
            String commandName = ((BulkString) command.getContent()[0]).getContent().getString().toUpperCase();
            CommandType commandType;

            try{
                commandType = CommandType.valueOf(commandName);
            }catch (IllegalArgumentException e){
                return false;
            }

            Command cmd = commandType.getSupplier().apply(redisCore);
            cmd.setContext(command.getContent());
            Resp result = cmd.handle();
            return true;
        }catch(Exception e){
            log.error("命令执行失败，在{}",position,e);
        }
        return false;
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
    }

    public void closeFile(){
        try{
            if(channel != null) channel.close();
            if(raf != null) raf.close();
        }catch(IOException e){
            log.error("关闭AOF文件时发生错误",e);
        }
    }


    public void close(){
        closeFile();
    }

}

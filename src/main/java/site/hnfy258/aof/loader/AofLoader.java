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
import site.hnfy258.server.context.RedisContext;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
@Slf4j
public class AofLoader {
    private final String fileName;
    private final RedisContext redisContext;
    private FileChannel channel;
    private RandomAccessFile raf;
    private static final int BUFFER_SIZE = 4096;

    private static final byte[] REDIS_PREFIX = {'*', '$', '+', '-', ':'};

    /**
     * 构造函数：基于RedisContext的解耦架构
     * 
     * @param fileName AOF文件名
     * @param redisContext Redis统一上下文
     * @throws Exception 文件操作异常
     */
    public AofLoader(final String fileName, final RedisContext redisContext) throws Exception {
        this.fileName = fileName;
        this.redisContext = redisContext;
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
    }    private boolean executeRedisCommand(final RespArray command, final int position) {
        try {
            final String commandName = ((BulkString) command.getContent()[0])
                    .getContent().getString().toUpperCase();
            final CommandType commandType;

            try {
                commandType = CommandType.valueOf(commandName);
            } catch (IllegalArgumentException e) {
                log.warn("未知命令类型: {} 在位置: {}", commandName, position);
                return false;
            }              // 1. 直接使用当前的RedisContext创建命令
            // 这样确保AOF加载的数据存储到正确的数据库中
            final Command cmd = commandType.createCommand(redisContext);
            cmd.setContext(command.getContent());
            cmd.handle();  // 执行命令，不需要保存结果
            return true;
        } catch (Exception e) {
            log.error("命令执行失败，在{}", position, e);
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

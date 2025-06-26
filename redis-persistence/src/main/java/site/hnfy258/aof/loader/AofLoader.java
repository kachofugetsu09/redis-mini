package site.hnfy258.aof.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.core.RedisCore;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * AOF文件加载器 - 负责解析和加载AOF持久化文件
 *
 * <p>AOF（Append Only File）是Redis的一种持久化方式，通过记录每个写操作来保证数据的持久性。
 * 本加载器能够解析标准的RESP（Redis Serialization Protocol）格式的AOF文件，
 * 并将其中的命令重新执行到Redis数据库中。
 *
 * <p>主要功能包括：
 * <ul>
 *     <li>文件操作 - 安全的文件打开、读取和关闭</li>
 *     <li>命令解析 - 解析RESP格式的Redis命令</li>
 *     <li>错误恢复 - 处理损坏的AOF数据，支持跳过无效命令</li>
 *     <li>大文件支持 - 支持超过2GB的大型AOF文件</li>
 *     <li>内存管理 - 使用Netty ByteBuf进行高效的内存管理</li>
 * </ul>
 *
 * <p>错误处理策略：
 * 当遇到无法解析的命令时，加载器会自动跳过错误数据并尝试恢复解析。
 * 这种设计保证了即使AOF文件部分损坏，仍能最大程度地恢复有效数据。
 *
 * <p>使用示例：
 * <pre>{@code
 * AofLoader loader = new AofLoader("dump.aof", redisCore);
 * loader.load(); // 加载AOF文件到Redis
 * loader.close(); // 关闭资源
 * }</pre>
 *
 * @author hnfy258
 * @since 1.0.0
 */
@Slf4j
public class AofLoader {

    // ========== 文件操作相关常量 ==========
    private static final int BUFFER_SIZE = 4096;
    private static final byte[] REDIS_PREFIX = {'*', '$', '+', '-', ':'};

    // ========== 核心属性 ==========
    private final String fileName;
    private final RedisCore redisCore;
    private FileChannel channel;
    private RandomAccessFile raf;

    /**
     * 构造函数：基于RedisCore接口的解耦架构
     *
     * <p>通过依赖RedisCore接口而非具体实现，实现了AOF加载器与Redis核心逻辑的解耦。
     * 这种设计使得AOF加载器可以与不同的Redis实现配合使用。
     *
     * @param fileName AOF文件路径，支持相对路径和绝对路径
     * @param redisCore Redis核心接口，提供命令执行和数据库操作能力
     * @throws Exception 当文件操作失败时抛出异常
     */
    public AofLoader(final String fileName, final RedisCore redisCore) throws Exception {
        this.fileName = fileName;
        this.redisCore = redisCore;
        openFile();
    }

    /**
     * 安全打开AOF文件
     *
     * <p>采用防御性编程策略，对各种异常情况进行处理：
     * <ul>
     *     <li>文件不存在 - 记录日志但不抛出异常，支持首次启动场景</li>
     *     <li>空文件 - 正常处理，避免不必要的资源占用</li>
     *     <li>权限不足 - 抛出详细的异常信息</li>
     *     <li>其他IO异常 - 确保资源被正确清理</li>
     * </ul>
     *
     * @throws IOException 当文件打开失败时抛出
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

    /**
     * 加载AOF文件并执行其中的命令
     *
     * <p>这是AOF加载器的核心方法，负责整个加载流程的协调：
     * <ol>
     *     <li>检查文件通道的有效性</li>
     *     <li>读取完整的文件内容到内存中</li>
     *     <li>逐个解析和执行RESP命令</li>
     *     <li>统计成功执行的命令数量</li>
     *     <li>确保资源被正确释放</li>
     * </ol>
     *
     * <p>内存管理策略：
     * 使用Netty的ByteBuf进行内存管理，支持引用计数和自动释放，
     * 能够有效防止内存泄漏问题。
     *
     * @throws RuntimeException 当加载过程中发生不可恢复的错误时抛出
     */
    public void load() {
        if (channel == null) {
            log.info("AOF文件通道未初始化，跳过加载操作");
            return;
        }

        ByteBuf commands = null;
        try {
            log.info("开始加载AOF文件: {}", fileName);
            
            // 1. 读取AOF文件的完整内容
            commands = readFileContent();

            // 2. 逐个处理RESP命令
            int successCount = processCommands(commands);

            log.info("AOF文件加载完成，成功执行 {} 条命令", successCount);
        } catch (Exception e) {
            log.error("AOF文件加载失败: {}", fileName, e);
            throw new RuntimeException("AOF加载过程中发生错误", e);
        } finally {
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

        log.warn("命令解析错误，位置: {}, 错误: {}", position, e.getMessage());

        int startPosition = commands.readerIndex();
        boolean foundNextCommand = false;

        if (commands.isReadable()) {
            commands.readByte(); // 跳过当前错误字节
        }

        while(commands.isReadable()) {
            int currentPos = commands.readerIndex();
            byte b = commands.readByte();
            if(isRespPrefix(b)) {
                commands.readerIndex(currentPos);
                foundNextCommand = true;
                break;
            }
        }

        if (!foundNextCommand) {
            log.warn("未找到有效的下一个命令，跳过剩余数据");
            commands.readerIndex(commands.writerIndex());
        }

        int skippedBytes = commands.readerIndex() - startPosition;
        if (skippedBytes > 0) {
            log.debug("跳过了 {} 字节的无效数据", skippedBytes);
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

    /**
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
    }

    /**
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

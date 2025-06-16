package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.cluster.replication.utils.ReplicationOffsetCalculator;
import site.hnfy258.cluster.host.ReplicationHost;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ReplicationHandler extends ChannelInboundHandlerAdapter {
    private final RedisNode redisNode;

    private final ReplicationStateMachine stateMachine;

    private ByteBuf accumulator;

    private static final int MAX_ACCUMULATOR_SIZE = 512 * 1024 * 1024;

    // ========== Channel上下文管理 ==========
    private ChannelHandlerContext currentContext;

    public ReplicationHandler(RedisNode redisNode) {
        this.redisNode = redisNode;
        this.stateMachine = redisNode.getReplicationStateMachine();
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 1. 保存当前的Channel上下文，用于后续通信
        this.currentContext = ctx;
        
        log.debug("从节点连接到主节点");
        if(stateMachine.transitionTo(ReplicationState.CONNECTING)){
            log.debug("状态转换到 CONNECTING");
        }
        else{
            log.warn("无法转换到 CONNECTING 状态，当前状态: {}", stateMachine.getCurrentState());
            super.channelActive(ctx);
        }
    }    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 1. 清理当前的Channel上下文
        this.currentContext = null;
        
        cleanup();
        super.channelInactive(ctx);
    }



    public void cleanup(){
        saveConnectionInfoBeforeCleanup();
        cleanupBuffer();
        stateMachine.softReset();
        log.info("从节点连接已清理，状态重置为初始状态");

    }    private void saveConnectionInfoBeforeCleanup() {
        try{
            String currentMasterId = null;
            
            // 优先从复制日志获取 masterRunId
            if(redisNode.getNodeState().getReplBackLog() != null){
                currentMasterId = redisNode.getNodeState().getReplBackLog().getMasterRunId();
            }
            
            // 如果复制日志中没有，尝试从已保存的连接信息中获取
            if(currentMasterId == null && redisNode.getNodeState().hasSavedConnectionInfo()) {
                currentMasterId = redisNode.getNodeState().getLastKnownMasterId();
                log.debug("从已保存的连接信息中获取 masterId: {}", currentMasterId);
            }

            long currentOffset = stateMachine.getReplicationOffset();
            
            // 如果偏移量无效，尝试从节点状态获取
            if(currentOffset <= 0) {
                currentOffset = redisNode.getNodeState().getReplicationOffset();
                log.debug("从节点状态获取偏移量: {}", currentOffset);
            }

            log.info("准备保存连接信息 - masterId: {}, offset: {}", currentMasterId, currentOffset);

            if(currentMasterId != null && currentOffset >= 0){
                redisNode.getNodeState().saveConnectionInfo(currentMasterId, currentOffset);
                log.info("成功保存当前连接信息: masterId={}, replicationOffset={}",
                    currentMasterId, currentOffset);
            } else {
                log.warn("无法保存连接信息 - masterId: {}, offset: {} (条件不满足)", currentMasterId, currentOffset);
            }
        }catch(Exception e){
            log.error("保存连接信息时发生错误: {}", e.getMessage(), e);
        }
    }

    private void cleanupBuffer() {
        if(accumulator !=null && accumulator.refCnt()>0){
            accumulator.release();
            accumulator = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try{
            if(msg instanceof SimpleString){
                handleSimpleString(ctx, (SimpleString) msg);
            }else if(msg instanceof BulkString){
                handleBulkString(ctx, (BulkString) msg);
            }else if(msg instanceof RespArray){
                handleCommand(ctx, (RespArray) msg);
            }else{
                ctx.fireChannelRead(msg);
            }
        }catch(Exception e){
            log.error("处理消息时发生错误: {}", e.getMessage(), e);
            cleanup();
            throw e;
        }
    }    private void handleCommand(ChannelHandlerContext ctx, RespArray command) {
        if (command.getContent().length >= 2) {
            Resp firstElement = command.getContent()[0];
            if (firstElement instanceof SimpleString) {
                if(firstElement instanceof SimpleString){
                    String response = ((SimpleString) firstElement).getContent();
                    if(response.startsWith("FULLRESYNC")){
                        handleFullSyncResponse(ctx, command);
                        return;
                    }
                    else if(response.startsWith("CONTINUE")) {
                        handlePartialSyncResponse(ctx, command);
                        return;
                    }
                }
            }
            
            if(!stateMachine.isReadyForReplCommands()){
                log.warn("[从节点] 当前状态 {} 不允许处理命令", stateMachine.getCurrentState());
                return;
            }
            try{
                redisNode.resetHeartbeatFailedCount();
                long commandOffset = ReplicationOffsetCalculator.calculateCommandOffset(command);
                if(commandOffset <= 0){
                    log.warn("[从节点] 命令偏移量计算失败，无法处理命令");
                    return;
                }

                long currentOffset = stateMachine.getReplicationOffset();

                boolean executed = executeReplicationCommand(command);

                if(!executed){
                    log.warn("[从节点] 执行复制命令失败，命令: {}", command);
                    return;
                }
                long newOffset = stateMachine.updateReplicationOffset(commandOffset);

                log.info("[从节点] {} 偏移量: {} -> {}", 
                    redisNode.getNodeId(), currentOffset, newOffset);

                syncStateToNodeState();
            }catch(Exception e){
                log.error("处理命令时发生错误: {}", e.getMessage(), e);
                stateMachine.transitionTo(ReplicationState.ERROR);
            }
        }
    }

    private void handlePartialSyncResponse(ChannelHandlerContext ctx, RespArray response) {
        try {
            if (response.getContent().length < 1) {
                log.warn("部分同步响应格式不正确，缺少必要参数");
                return;
            }

            Resp firstElement = response.getContent()[0];
            if (firstElement instanceof SimpleString) {
                String content = ((SimpleString) firstElement).getContent();
                if("CONTINUE".equals(content)){
                    log.info("[从节点]收到部分重同步响应CONTINUE");

                    redisNode.pauseHeartbeat();

                    if(stateMachine.transitionTo(ReplicationState.STREAMING)){
                        log.debug("状态转换到 STREAMING");
                    } else {
                        log.warn("无法转换到 STREAMING 状态，当前状态: {}", stateMachine.getCurrentState());
                        return;
                    }
                }
            }

            if (response.getContent().length >= 2) {
                Resp secondElement = response.getContent()[1];
                if (secondElement instanceof BulkString) {
                    BulkString commands = (BulkString) secondElement;                    if (commands.getContent() != null) {
                        // 🚀 优化：使用零拷贝方法避免重复数组分配
                        byte[] commandsData = commands.getContent().getBytesUnsafe();
                        log.info("接收到部分同步命令，长度: {}", commandsData.length);

                        processIncrementalSyncCommands(commandsData);
                    }
                }
            }
            redisNode.resumeHeartbeat();
        }catch(Exception e){
            log.error("处理部分同步响应时发生错误: {}", e.getMessage(), e);
            stateMachine.transitionTo(ReplicationState.ERROR);
        }

    }

    private void processIncrementalSyncCommands(byte[] commandsData) {
        if (commandsData == null || commandsData.length == 0) {
            log.warn("接收到的增量同步命令数据为空，无法处理");
            return;
        }
        try {
            log.info("处理增量同步命令，长度: {}", commandsData.length);
            if (commandsData.length > 0) {
                StringBuilder hex = new StringBuilder();
                int printLen = Math.min(commandsData.length, 64);
                for (int i = 0; i < printLen; i++) {
                    hex.append(String.format("%02x ", commandsData[i]));
                }
                log.debug("增量同步命令数据: {}", hex.toString());
                try {
                    String preview = new String(commandsData, 0, Math.min(commandsData.length, 64), "UTF-8");
                    log.debug("增量同步命令预览: {}", preview);
                } catch (Exception e) {
                    log.debug("无法将增量同步命令数据转换为字符串: {}", e.getMessage());
                }
            }
            ByteBuf buffer = Unpooled.wrappedBuffer(commandsData);

            try {
                while (buffer.isReadable()) {
                    byte firstByte = buffer.getByte(buffer.readerIndex());
                    if (firstByte == '\n' || firstByte == '\r') {
                        buffer.readByte(); // 跳过无效的换行符
                        log.debug("跳过无效换行符");
                    } else {
                        break; // 找到有效的命令开始
                    }
                }

                List<RespArray> commands = new ArrayList<>();

                while (buffer.isReadable()) {
                    try {
                        Resp resp = Resp.decode(buffer);
                        if (resp instanceof RespArray) {
                            RespArray command = (RespArray) resp;
                            if (isValidCommand(command)) {
                                commands.add(command);
                            }
                        }
                    }catch(Exception e){
                        log.warn("解码增量同步命令时发生错误，可能是格式不正确: {}", e.getMessage());
                        break; // 停止处理后续命令
                    }
                }
                log.info("解码到 {} 个增量同步命令", commands.size());

                if(commands.isEmpty()) {
                    log.warn("没有有效的增量同步命令可处理");
                    return;
                }

                int successCount = 0;
                for(RespArray command : commands){
                    try{
                        long commandOffset = ReplicationOffsetCalculator.calculateCommandOffset(command);
                        if(commandOffset <= 0){
                            log.warn("命令偏移量计算失败，无法处理命令: {}", command);
                            continue;
                        }                        boolean executed = executeReplicationCommand(command);
                        if(executed){
                            successCount++;
                            stateMachine.updateReplicationOffset(commandOffset);
                        }
                    }catch(Exception e){
                        log.error("处理增量同步命令时发生错误: {}", e.getMessage(), e);
                    }
                }
                log.info("成功处理 {} 个增量同步命令", successCount);
                syncStateToNodeState();
            }finally {
                buffer.release();
            }
        }catch (Exception e){
            log.error("处理增量同步命令时发生错误: {}", e.getMessage(), e);
            stateMachine.transitionTo(ReplicationState.ERROR);
        }
    }

    private void handleFullSyncResponse(ChannelHandlerContext ctx, RespArray response) {
        try{
            if(response.getContent().length <2){
                log.warn("全量同步响应格式不正确，缺少必要参数");
                return;
            }
            Resp firstElement = response.getContent()[0];
            if(firstElement instanceof SimpleString){
                handleSimpleString(ctx, (SimpleString) firstElement);
            }
            Resp secondElement = response.getContent()[1];
            if(secondElement instanceof BulkString){
               handleBulkString(ctx, (BulkString) secondElement);
            }
        }catch(Exception e){
            log.error("处理全量同步响应时发生错误: {}", e.getMessage(), e);
            stateMachine.transitionTo(ReplicationState.ERROR);
        }
    }    /**
     * 执行复制命令
     * 
     * @param command 要执行的命令
     * @return 是否执行成功
     */
    private boolean executeReplicationCommand(RespArray command) {
        if (!isValidCommand(command)) {
            log.warn("[从节点] 无效的复制命令: {}", command);
            return false;
        }
        
        try {
            // 1. 通过ReplicationHost接口执行命令
            ReplicationHost host = redisNode.getReplicationHost();
            if (host == null) {
                log.error("[从节点] ReplicationHost未设置，无法执行命令");
                return false;
            }

            // 2. 添加调试日志
            String commandName = getCommandName(command);
            log.info("[从节点] 准备执行复制命令: {}", commandName);

            Resp result = host.executeCommand(command);

            if (result instanceof Errors) {
                log.warn("[从节点] 执行命令失败: {}, 命令: {}", 
                    ((Errors) result).getContent(), commandName);
                return false;
            }
            
            log.info("[从节点] 命令执行成功: {}, 结果类型: {}", 
                commandName, result != null ? result.getClass().getSimpleName() : "null");
            return true;
        } catch (Exception e) {
            log.error("[从节点] 执行复制命令时发生异常: {}", e.getMessage(), e);
            return false;
        }
    }

    private String getCommandName(RespArray command) {
        if(!isValidCommand(command)){
            return "UNKNOWN";
        }
        try{
            BulkString bulkString = (BulkString) command.getContent()[0];
            return bulkString.getContent().getString().toUpperCase();
        }catch(Exception e){
            log.error("获取命令名称时发生错误: {}", e.getMessage(), e);
            return "UNKNOWN";
        }
    }

    private boolean isValidCommand(RespArray command) {
        if(command == null || command.getContent() == null || command.getContent().length == 0) {
            log.warn("无效的复制命令: 命令或内容为空");
            return false;
        }

         Resp firstElement = command.getContent()[0];
        if(!(firstElement instanceof BulkString)){
            return false;
        }        
        BulkString firstBulkString = (BulkString) firstElement;
        if(firstBulkString.getContent() == null || firstBulkString.getContent().getBytesUnsafe().length == 0) {
            log.warn("无效的复制命令: 第一个元素为空");
            return false;
        }
        return true;
    }


    private void handleBulkString(ChannelHandlerContext ctx, BulkString bulkString) {

        RedisBytes content = bulkString.getContent();
        if (content == null) {
            log.error("接收到空的 BulkString 内容");
            return;
        }
        ReplicationState currentState = stateMachine.getCurrentState().get();        if( currentState== ReplicationState.SYNCING){
            // 🚀 优化：RDB 数据处理使用零拷贝
            processRdbData(content.getBytesUnsafe());
        }
        else if(currentState == ReplicationState.STREAMING){
            // 🚀 优化：增量同步命令处理使用零拷贝
            byte[] commandsData = content.getBytesUnsafe();
            processIncrementalSyncCommands(commandsData);
        }
        else{
            log.debug("当前状态 {} 不允许处理 BulkString 内容", currentState);
            ctx.fireChannelRead(content);
        }
    }

    private void processRdbData(byte[] rdbData) {
        if(rdbData == null || rdbData.length <=0 || rdbData.length > MAX_ACCUMULATOR_SIZE){
            log.error("RDB 数据无效或超过最大累积大小: {}", rdbData == null ? "null" : rdbData.length);
            stateMachine.transitionTo(ReplicationState.ERROR);
            return;
        }

        ReplicationState currentState = stateMachine.getCurrentState().get();
        if(currentState != ReplicationState.SYNCING) {
            log.warn("当前状态不是 SYNCING，无法处理 RDB 数据: {}", currentState);
            stateMachine.transitionTo(ReplicationState.ERROR);
            return;
        }

        boolean success = redisNode.receiveRdbFromMaster(rdbData);
        if(!success) {
            log.error("从主节点接收 RDB 数据失败，可能是数据格式错误或不完整");
            stateMachine.transitionTo(ReplicationState.ERROR);
            cleanupBuffer();
            return;
        }

        try{
            long masterOffset = stateMachine.getMasterReplicationOffset();
            long correctOffset = ReplicationOffsetCalculator.calculatePostRdbOffset(masterOffset, rdbData.length);

            if(ReplicationOffsetCalculator.validateOffset(correctOffset,"RDB后偏移量")){
                stateMachine.setReplicationOffset(correctOffset);
                log.info("RDB 数据处理成功，更新复制偏移量: {}", correctOffset);


            }else{
                log.error("计算的偏移量不合理{}，使用主偏移量{}作为复制偏移量", correctOffset, masterOffset);
                stateMachine.setReplicationOffset(masterOffset);
            }

            if(!stateMachine.transitionTo(ReplicationState.STREAMING)){
               log.error("无法转换到 STREAMING 状态，当前状态: {}", stateMachine.getCurrentState());
               throw new IllegalStateException("无法转换到 STREAMING 状态");
            }

            log.info("状态转换到 STREAMING，准备接收增量数据");
            syncStateToNodeState();
            notifyMasterToSyncBacklogOffset(correctOffset);

            redisNode.startHeartbeatManager();
        }catch(Exception e){
            log.error("处理 RDB 数据时发生错误: {}", e.getMessage(), e);
            stateMachine.transitionTo(ReplicationState.ERROR);
        }finally {
            cleanupBuffer();
        }
    }

    /**
     * 通知主节点同步backlog偏移量
     * 
     * @param correctOffset 正确的偏移量
     */
    private void notifyMasterToSyncBacklogOffset(final long correctOffset) {
        try {
            // 1. 获取当前的Channel上下文
            final ChannelHandlerContext currentCtx = getCurrentChannelContext();
            if (currentCtx == null || !currentCtx.channel().isActive()) {
                log.warn("无法通知主节点同步偏移量：主节点连接不可用，偏移量: {}", correctOffset);
                return;
            }
            
            // 2. 构造ACK命令通知主节点当前的复制偏移量
            final String ackCommand = String.format("REPLCONF ACK %d", correctOffset);
            final ByteBuf ackBuffer = Unpooled.wrappedBuffer(ackCommand.getBytes());
            
            // 3. 发送ACK命令到主节点
            currentCtx.writeAndFlush(ackBuffer).addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("成功发送偏移量ACK到主节点: {}", correctOffset);
                } else {
                    log.error("发送偏移量ACK失败: {}", future.cause().getMessage());
                }
            });
            
        } catch (Exception e) {
            log.error("通知主节点同步偏移量时发生错误: {}", e.getMessage(), e);
        }
    }
      /**
     * 获取当前的Channel上下文
     * 
     * @return ChannelHandlerContext或null
     */
    private ChannelHandlerContext getCurrentChannelContext() {
        return currentContext;
    }

    private void syncStateToNodeState() {
        try{
            ReplicationStateMachine.StateConsistencyResult consistencyResult = stateMachine.validateConsistency();
            if(!consistencyResult.isConsistent){
                log.warn("状态机不一致: {}", consistencyResult);
            }

            ReplicationState currentState = stateMachine.getCurrentState().get();

            boolean isConnected = currentState==ReplicationState.CONNECTING||
                                  currentState==ReplicationState.SYNCING||
                                  currentState==ReplicationState.STREAMING;
            redisNode.setConnected(isConnected);

            long replicationOffset = stateMachine.getReplicationOffset();
            redisNode.getNodeState().setReplicationOffset(replicationOffset);

            long masterReplicationOffset = stateMachine.getMasterReplicationOffset();
            redisNode.getNodeState().setMasterReplicationOffset(masterReplicationOffset);

            boolean ready = stateMachine.isReadyForReplCommands();
            redisNode.getNodeState().setReadyForReplCommands(ready);

            log.debug("状态同步到节点状态: 连接状态={}, 复制偏移量={}, 主复制偏移量={}, 准备状态={}",
                    isConnected, replicationOffset, masterReplicationOffset, ready);
        }catch(Exception e){
            log.error("同步状态到节点状态时发生错误: {}", e.getMessage(), e);
        }
    }    private void handleSimpleString(ChannelHandlerContext ctx, SimpleString simpleString) {
        String response = simpleString.getContent();
        log.info("接收到简单字符串响应: {}", response);

        if(response.startsWith("FULLRESYNC")){
            String[] parts = response.split(" ");
            if(parts.length >=3){
                String masterId = parts[1];
                long masterOffset = Long.parseLong(parts[2]);

                log.info("主节点全量同步: masterId={}, masterOffset={}", masterId, masterOffset);


                // 保存 masterRunId 到复制日志
                if(redisNode.getNodeState().getReplBackLog() != null) {
                    redisNode.getNodeState().getReplBackLog().setMasterRunId(masterId);
                    log.info("已将 masterRunId 保存到复制日志: {}", masterId);
                }

                // 设置主节点偏移量到状态机
                stateMachine.setMasterReplicationOffset(masterOffset);

                if(stateMachine.transitionTo(ReplicationState.SYNCING)){
                    log.debug("状态转换到 SYNCING");
                } else {
                    log.warn("无法转换到 SYNCING 状态，当前状态: {}", stateMachine.getCurrentState());
                }
            }
        }else if(response.startsWith("CONTINUE")) {
            log.info("主节点请求继续同步");
            if (stateMachine.transitionTo(ReplicationState.STREAMING)) {
                log.debug("状态转换到 STREAMING");
            } else {
                log.warn("无法转换到 STREAMING 状态，当前状态: {}", stateMachine.getCurrentState());
            }
        }else{
            ctx.fireChannelRead(simpleString);
        }
    }







}


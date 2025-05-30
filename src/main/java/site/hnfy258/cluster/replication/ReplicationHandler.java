package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.cluster.node.RedisNode;
import site.hnfy258.cluster.replication.utils.ReplicationOffsetCalculator;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;

@Slf4j
public class ReplicationHandler extends ChannelInboundHandlerAdapter {
    private final RedisNode redisNode;

    private final ReplicationStateMachine stateMachine;

    private ByteBuf accumulator;

    private static final int MAX_ACCUMULATOR_SIZE = 512 * 1024 * 1024;

    public ReplicationHandler(RedisNode redisNode) {
        this.redisNode = redisNode;
        this.stateMachine = redisNode.getReplicationStateMachine();
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("从节点连接到主节点");
        if(stateMachine.transitionTo(ReplicationState.CONNECTING)){
            log.debug("状态转换到 CONNECTING");
        }
        else{
            log.warn("无法转换到 CONNECTING 状态，当前状态: {}", stateMachine.getCurrentState());
            super.channelActive(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.channelInactive(ctx);
    }



    public void cleanup(){
        cleanupBuffer();

        stateMachine.reset();
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
            }            try{
                long commandOffset = ReplicationOffsetCalculator.calculateCommandOffset(command);
                if(commandOffset <= 0){
                    log.warn("[从节点] 命令偏移量计算失败，无法处理命令");
                    return;
                }

                long currentOffset = stateMachine.getReplicationOffset();
                executeReplicationCommand(command);

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
                handleSimpleString(ctx, (SimpleString) firstElement);
            }

            if (response.getContent().length >= 2) {
                Resp secondElement = response.getContent()[1];
                if (secondElement instanceof BulkString) {
                    BulkString commands = (BulkString) secondElement;
                    if (commands.getContent() != null) {
                        log.info("接收到部分同步命令，长度: {}", commands.getContent().getBytes().length);
                    }
                }
            }
        }catch(Exception e){
            log.error("处理部分同步响应时发生错误: {}", e.getMessage(), e);
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
    }    private void executeReplicationCommand(RespArray command) {
        if (command == null || command.getContent() == null || command.getContent().length == 0) {
            log.warn("[从节点] 无效的复制命令: 命令为空");
            return;
        }

        Resp[] content = command.getContent();
        if (!(content[0] instanceof BulkString)) {
            log.warn("[从节点] 无效的复制命令: 第一个元素不是BulkString");
            return;
        }        try {
            String commandName = new String(((BulkString) content[0]).getContent().getBytes()).toUpperCase();
            
            site.hnfy258.command.CommandType commandType = site.hnfy258.command.CommandType.valueOf(commandName);
            site.hnfy258.command.Command cmd = commandType.getSupplier().apply(redisNode.getRedisCore());
            cmd.setContext(content);
            cmd.handle();

        } catch (Exception e) {
            String cmdName = content[0] instanceof BulkString ?
                    new String(((BulkString) content[0]).getContent().getBytes()) : "unknown";
            log.error("[从节点] {} 复制命令执行失败: {} - {}", 
                redisNode.getNodeId(), cmdName, e.getMessage());
        }
        }
    

    private void handleBulkString(ChannelHandlerContext ctx, BulkString bulkString) {

        RedisBytes content = bulkString.getContent();
        if (content == null) {
            log.error("接收到空的 BulkString 内容");
            return;
        }

        if(stateMachine.getCurrentState().get() == ReplicationState.SYNCING){
            processRdbData(content.getBytes());
        }
        else{
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
        }catch(Exception e){
            log.error("处理 RDB 数据时发生错误: {}", e.getMessage(), e);
            stateMachine.transitionTo(ReplicationState.ERROR);
        }finally {
            cleanupBuffer();
        }
    }

    

    private void notifyMasterToSyncBacklogOffset(long correctOffset) {
        //todo
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
    }

    private void handleSimpleString(ChannelHandlerContext ctx, SimpleString simpleString) {
        String response = simpleString.getContent();
        log.info("接收到简单字符串响应: {}", response);

        if(response.startsWith("FULLRESYNC")){
            String[] parts = response.split(" ");
            if(parts.length >=3){
                String masterId = parts[1];
                long masterOffset = Long.parseLong(parts[2]);

                log.info("主节点全量同步: masterId={}, masterOffset={}", masterId, masterOffset);

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


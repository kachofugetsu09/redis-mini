package site.hnfy258.cluster.replication;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Getter
@Setter
public class ReplicationStateMachine {
    private final AtomicReference<ReplicationState> currentState;

    private final AtomicLong replicationOffset;

    private final AtomicLong masterReplicationOffset;

    private final AtomicLong slaveReplicationOffset;

    private final AtomicBoolean readyForReplCommands;

    public ReplicationStateMachine() {
        this.currentState = new AtomicReference<>(ReplicationState.DISCONNECTED);
        this.replicationOffset = new AtomicLong(0);
        this.masterReplicationOffset = new AtomicLong(0);
        this.slaveReplicationOffset = new AtomicLong(0);
        this.readyForReplCommands = new AtomicBoolean(false);
    }

    public boolean transitionTo(ReplicationState newState){
        if(newState == null) {
            log.error("无法转换到空状态");
            return false;
        }

        ReplicationState oldState = currentState.get();

        if(!isValidTransition(oldState, newState)) {
            log.warn("无法从 {} 转换到 {}，不合法的状态转换", oldState, newState);
            return false;
        }

        if(currentState.compareAndSet(oldState, newState)) {
            log.debug("状态从 {} 转换到 {}", oldState, newState);

            updateAttributesForState(newState);

            return true;
        }
        else{
            log.warn("状态转换失败，当前状态仍然是 {}", currentState.get());
            return false;
        }
    }

    private void updateAttributesForState(ReplicationState newState) {
        boolean shouldBeReady = false;
        switch(newState){
            case DISCONNECTED:
            case ERROR:
                shouldBeReady = false;
                break;
            case CONNECTING:
            case SYNCING:
                shouldBeReady = false;
                break;
            case STREAMING:
                shouldBeReady = true;
                break;
            default:
                shouldBeReady = false;
                break;
        }

        setReadyForReplCommands(shouldBeReady);
    }

    private void setReadyForReplCommands(boolean shouldBeReady) {
        this.readyForReplCommands.getAndSet(shouldBeReady);
    }

    public boolean isReadyForReplCommands(){
        boolean isReady = readyForReplCommands.get();
        boolean canAccept = currentState.get().canAcceptReplicationCommands();

        boolean result = isReady && canAccept;
        if(!result){
            log.debug("当前状态不允许接收复制命令: isReady={}, canAccept={}", isReady, canAccept);
        }
        return result;
    }

    private boolean isValidTransition(ReplicationState oldState, ReplicationState newState) {
        //1.检查空状态
        if(oldState == null || newState == null) {
            log.warn("状态转换中遇到空状态");
            return false;
        }
        //2.相同状态检查
        if(oldState == newState && oldState!= ReplicationState.ERROR){
            log.debug("状态未变化，仍然是 {}", oldState);
            return true;
        }
        //3.根据状态转换规则验证
        switch(oldState){
            case DISCONNECTED:
                return newState == ReplicationState.CONNECTING||newState == ReplicationState.ERROR;

            case CONNECTING:
                return newState == ReplicationState.SYNCING||newState == ReplicationState.DISCONNECTED||newState == ReplicationState.ERROR;

            case SYNCING:
                return newState == ReplicationState.STREAMING||newState == ReplicationState.DISCONNECTED||newState == ReplicationState.ERROR;

            case STREAMING:
                return newState == ReplicationState.STREAMING || newState == ReplicationState.DISCONNECTED || newState == ReplicationState.ERROR;

            case ERROR:
                return newState == ReplicationState.DISCONNECTED;

            default:
                log.warn("未知状态: {}", oldState);
                return false;
        }
    }

    public void setReplicationOffset(long offset) {
        if(offset < 0) {
            log.warn("尝试设置负的复制偏移量: {}", offset);
            return;
        }
        this.replicationOffset.set(offset);
    }

    public long getReplicationOffset() {
        return this.replicationOffset.get();
    }

    public void setMasterReplicationOffset(long offset) {
        if(offset < 0) {
            log.warn("尝试设置负的主节点复制偏移量: {}", offset);
            return;
        }
        this.masterReplicationOffset.set(offset);
    }

    public long getMasterReplicationOffset() {
        return this.masterReplicationOffset.get();
    }

    public void setSlaveReplicationOffset(long offset) {
        if(offset < 0) {
            log.warn("尝试设置负的从节点复制偏移量: {}", offset);
            return;
        }
        this.slaveReplicationOffset.set(offset);
    }

    public long getSlaveReplicationOffset() {
        return this.slaveReplicationOffset.get();
    }

    public void reset(){
        currentState.set(ReplicationState.DISCONNECTED);
        replicationOffset.set(0);
        masterReplicationOffset.set(0);
        slaveReplicationOffset.set(0);
        readyForReplCommands.set(false);
        log.info("复制状态机已重置");
    }

    public StateConsistencyResult validateConsistency() {
        ReplicationState state = currentState.get();
        boolean ready = readyForReplCommands.get();
        boolean canAccept = state!=null && state.canAcceptReplicationCommands();

        long replOffset = getReplicationOffset();
        long masterOffset = getMasterReplicationOffset();

        boolean isConsistent = true;
        StringBuilder issues = new StringBuilder();

        if(state == null){
            isConsistent = false;
            issues.append("状态机当前状态为null;");
        }
        if(ready && !canAccept) {
            isConsistent = false;
            issues.append("状态机准备就绪但不允许接收复制命令;");
        }

        if(replOffset < 0) {
            isConsistent = false;
            issues.append("复制偏移量小于0;");
        }

        if(masterOffset < 0) {
            isConsistent = false;
            issues.append("主节点复制偏移量小于0;");
        }

        switch(state){
            case DISCONNECTED:
                if(ready){
                    isConsistent =false;
                    issues.append("状态为DISCONNECTED但准备就绪;");

                }
                break;
            case CONNECTING:
                if(ready){
                    isConsistent = false;
                    issues.append("状态为CONNECTING但准备就绪;");
                }
                break;
            case SYNCING:
                if(ready){
                    isConsistent = false;
                    issues.append("状态为SYNCING但准备就绪;");
                }
            case STREAMING:
                if(!ready){
                    isConsistent = false;
                    issues.append("状态为STREAMING但未准备就绪;");
                }
                break;
            case ERROR:
                if(ready){
                    isConsistent = false;
                    issues.append("状态为ERROR但准备就绪;");
                }
                break;

        }
        return new StateConsistencyResult(
                isConsistent,
                issues.toString(),
                state,
                ready,
                replOffset,
                masterOffset
        );
    }    public long updateReplicationOffset(long commandOffset) {
        if(commandOffset < 0) {
            log.warn("尝试更新复制偏移量为负值: {}", commandOffset);
            return replicationOffset.get();
        }

        long currentOffset = replicationOffset.get();
        // 1. 从节点偏移量应该累加命令大小，而不是直接设置
        // 2. 这样才能与主节点的偏移量保持同步
        long newOffset = currentOffset + commandOffset;
        replicationOffset.set(newOffset);
        log.debug("更新复制偏移量: {} -> {} (增加 {} 字节)", currentOffset, newOffset, commandOffset);

        return replicationOffset.get();
    }

    public static class StateConsistencyResult{
        public final boolean isConsistent;
        public final String issues;
        public final ReplicationState state;
        public final boolean ready;
        public final long replicationOffset;
        public final long masterReplicationOffset;

        public StateConsistencyResult(boolean isConsistent, String issues, ReplicationState state, boolean ready,
                                      long replicationOffset, long masterReplicationOffset) {
            this.isConsistent = isConsistent;
            this.issues = issues;
            this.state = state;
            this.ready = ready;
            this.replicationOffset = replicationOffset;
            this.masterReplicationOffset = masterReplicationOffset;
        }

        @Override
        public String toString(){
            return String.format("一致性检查结果: 一致性=%s, 问题=%s, 状态=%s, 准备状态=%s, 复制偏移量=%d, 主复制偏移量=%d",
                    isConsistent, issues, state, ready, replicationOffset, masterReplicationOffset);
        }
    }
}

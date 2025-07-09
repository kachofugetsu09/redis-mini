package site.hnfy258.raft;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import site.hnfy258.aof.AofManager;
import site.hnfy258.config.RaftConfig;
import site.hnfy258.core.AppendResult;
import site.hnfy258.core.LogEntry;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RoleState;
import site.hnfy258.network.RaftNetwork;
import site.hnfy258.persistence.RaftLogManager;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.rpc.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class Raft {
    private final int selfId; // 当前节点ID
    private final List<Integer> peerIds; // 所有节点ID列表
    private final RaftNetwork network; // 网络层抽象

    // Raft算法状态
    private int currentTerm; // 当前任期
    private int votedFor; // 当前任期内投票给的候选人ID
    @Setter
    private final List<LogEntry> log; // 日志条目数组
    private RoleState state; // 当前角色状态

    // 所有服务器上的易失性状态
    private int commitIndex; // 已提交的日志条目索引
    private int lastApplied; // 最后应用到状态机的日志条目索引

    // 领导者上的易失性状态
    private final int[] nextIndex; // 每个从节点下一个应当被传递的条目
    private final int[] matchIndex; // 每个从节点已复制的最高日志条目索引

    // 选举超时相关
    private long lastHeartbeatTime;

    // 投票统计
    private int voteCount;

    private RaftConfig config;

    //逻辑数据库
    private RedisCore redisCore;

    // 持久化相关
    private RaftLogManager raftLogManager;
    private String raftStateFileName; // .log 文件名
    private String raftLogFileName;   // .raof 文件名

    // Applier线程相关
    private Thread applierThread;
    private volatile boolean applierRunning = false;
    private final Object applierLock = new Object(); // 专门用于applier的锁

    /**
     * -- SETTER --
     *  设置RaftNode引用
     */
    // RaftNode引用，用于控制定时器
    private RaftNode nodeRef;

    private final Object lock = new Object(); // 锁对象，用于同步

    public Raft(int selfId, int[] peerIds,
                RaftNetwork network,
                RedisCore redisCore,
                RaftLogManager raftLogManager) {
        this.selfId = selfId;
        this.network = network;
        this.peerIds = new ArrayList<>();
        for (int peerId : peerIds) {
            this.peerIds.add(peerId);
        }
        this.currentTerm = 0;
        this.votedFor = -1;
        this.log = new ArrayList<>();
        log.add(new LogEntry(-1,-1,null));
        this.state = RoleState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new int[peerIds.length];
        this.matchIndex = new int[peerIds.length];

        for(int i=0; i < peerIds.length; i++){
            nextIndex[i] = 1;
            matchIndex[i] = 0;
        }
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.voteCount = 0;
        this.redisCore = redisCore;
        this.raftLogManager = raftLogManager;
        
        // 初始化持久化文件名
        initializePersistenceFileNames();
        
        // 如果没有传入 raftLogManager，尝试创建一个
        if (this.raftLogManager == null && this.redisCore != null) {
            try {
                this.raftLogManager = new RaftLogManager(raftLogFileName, redisCore, this);
            } catch (Exception e) {
                System.err.println("初始化 RaftLogManager 失败: " + e.getMessage());
            }
        }
        
        // 启动applier线程
        if (this.redisCore != null) {
            startApplier();
        }
        
        // 读取持久化状态
        loadPersistedState();

    }

    public Raft(int selfId, int[] peerIds, RaftNetwork network) {
        this(selfId, peerIds, network, null,null);
    }

    /**
     * 初始化持久化文件名
     */
    private void initializePersistenceFileNames() {
        // 根据节点ID生成文件名
        raftStateFileName = "node" + selfId + ".log";   // Raft状态文件
        raftLogFileName = "node" + selfId + ".raof";    // Raft日志文件
        
        System.out.println("Node " + selfId + " 持久化文件名初始化:");
        System.out.println("  状态文件: " + raftStateFileName);
        System.out.println("  日志文件: " + raftLogFileName);
    }

    /**
     * 加载持久化状态
     */
    private void loadPersistedState() {
        try {
            // 加载 Raft 状态信息
            loadRaftState();
            
            // 加载 Raft 日志
            loadRaftLog();
            
            System.out.println("Node " + selfId + " 持久化状态加载完成");
            System.out.println("  当前任期: " + currentTerm);
            System.out.println("  投票对象: " + votedFor);
            System.out.println("  日志大小: " + log.size());
            
        } catch (Exception e) {
            System.err.println("Node " + selfId + " 加载持久化状态失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 加载 Raft 状态信息
     */
    private void loadRaftState() {
        if (raftStateFileName == null) {
            System.out.println("Node " + selfId + " Raft状态文件名未初始化，使用默认状态");
            return;
        }
        
        try (java.io.FileInputStream fis = new java.io.FileInputStream(raftStateFileName)) {
            // 读取当前任期 (4字节，大端序)
            int term = 0;
            term |= (fis.read() & 0xFF) << 24;
            term |= (fis.read() & 0xFF) << 16;
            term |= (fis.read() & 0xFF) << 8;
            term |= (fis.read() & 0xFF);
            
            // 读取投票对象 (4字节，大端序)
            int voted = 0;
            voted |= (fis.read() & 0xFF) << 24;
            voted |= (fis.read() & 0xFF) << 16;
            voted |= (fis.read() & 0xFF) << 8;
            voted |= (fis.read() & 0xFF);
            
            // 更新状态
            this.currentTerm = term;
            this.votedFor = voted;
            
            System.out.println("Node " + selfId + " 成功加载Raft状态: term=" + currentTerm + ", votedFor=" + votedFor);
            
        } catch (java.io.FileNotFoundException e) {
            System.out.println("Node " + selfId + " Raft状态文件不存在，使用默认状态");
        } catch (Exception e) {
            System.err.println("Node " + selfId + " 加载Raft状态失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 加载 Raft 日志
     */
    private void loadRaftLog() {
        if (raftLogManager != null) {
            try {
                raftLogManager.load();

            } catch (Exception e) {
                System.err.println("Node " + selfId + " 加载日志失败: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("Node " + selfId + " RaftLogManager 未初始化，跳过日志加载");
        }
    }




    /**
     * 重置选举超时时间
     */
    private void resetElectionTimeout() {
        lastHeartbeatTime = System.currentTimeMillis();
        if (nodeRef != null) {
            nodeRef.resetElectionTimer();
        }
    }

    public void startElection(){
        RequestVoteArg arg = new RequestVoteArg();
        synchronized (lock){
            if (state != RoleState.CANDIDATE){
                return;
            }
            this.currentTerm++; // 增加当前任期
            this.votedFor = selfId; // 给自己投票
            resetElectionTimeout();

            arg.candidateId = selfId;
            arg.term = currentTerm;
            arg.lastLogIndex = log.size() - 1;
            arg.lastLogTerm = log.get(log.size() - 1).getLogTerm();
        }
        // 异步发送投票请求并统计结果
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        AtomicInteger voteCount = new AtomicInteger(1); // 先给自己投一票

        for (Integer peerId : peerIds) {
            if (peerId == selfId) {
                continue;
            }

            CompletableFuture<Boolean> future = network.sendRequestVote(peerId, arg)
                    .thenApply(reply -> {
                        boolean granted = handleRequestVoteReply(peerId, arg.term, reply);
                        if (granted) {
                            System.out.println("Node " + selfId + " received vote from " + peerId);
                            voteCount.incrementAndGet();
                        }
                        return granted;
                    });
            futures.add(future);
        }

        // 等待所有投票请求完成或达到多数票
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        // 异步处理投票结果
        allFutures.thenRun(() -> {
            synchronized (lock) {
                int requiredVotes = (peerIds.size() / 2) + 1; // 修正：集群大小的一半加1
                int currentVotes = voteCount.get();
                System.out.println("Node " + selfId + " election result: " + currentVotes + "/" + requiredVotes + " votes, state=" + state);

                if (state == RoleState.CANDIDATE && currentVotes >= requiredVotes) {
                    becomeLeader();
                }
            }
        });

        // 或者当获得多数票时立即成为领导者
        for (CompletableFuture<Boolean> future : futures) {
            future.thenRun(() -> {
                synchronized (lock) {
                    int requiredVotes = (peerIds.size() / 2) + 1; // 修正：集群大小的一半加1
                    int currentVotes = voteCount.get();

                    if (state == RoleState.CANDIDATE && currentVotes >= requiredVotes) {
                        System.out.println("Node " + selfId + " got majority votes: " + currentVotes + "/" + requiredVotes);
                        becomeLeader();
                    }
                }
            });
        }
    }


    public synchronized boolean handleRequestVoteReply(int serverId,
                                                       int requestTerm,
                                                       RequestVoteReply reply){
        if (reply.term > currentTerm) {
            this.becomeFollower(reply.term);
            return false;
        }

        if(currentTerm != requestTerm){
            return false; // 如果当前任期不匹配，忽略回复
        }

        return reply.voteGranted;
    }

    public synchronized RequestVoteReply handleRequestVoteRequest(RequestVoteArg arg) {
        RequestVoteReply reply = new RequestVoteReply();
        //1.如果当前任期小于请求的任期，则更新当前任期
        if (arg.term < currentTerm) {
            reply.term = currentTerm;
            reply.voteGranted = false;
            return reply;
        }

        //2.如果请求任期大于当前任期，更新当前任期以及当前任期投票给的候选人状态
        if (arg.term > currentTerm) {
            currentTerm = arg.term;
            votedFor = -1; // 重置投票状态
            state = RoleState.FOLLOWER; // 转为跟随者状态

            // 重置选举超时时间，因为我们接收到了更高任期的请求
            resetElectionTimeout();

            System.out.println("Node " + selfId + " updated term to " + currentTerm + " and became FOLLOWER");
        }

        reply.term = currentTerm;

        boolean canVoted = votedFor == -1 || votedFor == arg.candidateId;
        boolean isLogUpToDate = (arg.lastLogTerm > this.getLastLogTerm()) ||
                (arg.lastLogTerm == this.getLastLogTerm() && arg.lastLogIndex >= this.getLastLogIndex());

        if (canVoted && isLogUpToDate) {
            votedFor = arg.candidateId;
            reply.voteGranted = true;

            System.out.println("Node " + selfId + " voted for " + arg.candidateId + " in term " + currentTerm);

            // 重置选举超时时间，因为我们刚刚投票给了一个候选人
            resetElectionTimeout();
        }
        else{
            reply.voteGranted = false;
            System.out.println(STR."Node \{selfId} denied vote for \{arg.candidateId} in term \{currentTerm} (canVoted=\{canVoted}, isLogUpToDate=\{isLogUpToDate})");
        }
        return reply;

    }

    public void becomeFollower(int term){
        state = RoleState.FOLLOWER;
        currentTerm = term;
        votedFor = -1; // 重置投票状态

        // 通知RaftNode状态变化
        if (nodeRef != null) {
            nodeRef.onBecomeFollower();
        }
    }


    public void becomeLeader(){
        state = RoleState.LEADER;

        for(int i = 0; i < peerIds.size(); i++){
            if (i < nextIndex.length) {
                nextIndex[i] = log.size(); // 初始化为当前日志长度（下一个要发送的索引）
            }
            if (i < matchIndex.length) {
                matchIndex[i] = 0; // 初始化每个从节点的匹配索引为0
            }
        }

        // 找到自己在peerIds中的索引
        int selfIndex = peerIds.indexOf(selfId);
        if (selfIndex >= 0 && selfIndex < matchIndex.length) {
            matchIndex[selfIndex] = getLastLogIndex(); // 自己的匹配索引为最后日志索引
        }

        System.out.println("Node " + selfId + " became LEADER for term " + currentTerm);

        // 通知RaftNode启动心跳定时器
        if (nodeRef != null) {
            nodeRef.onBecomeLeader();
        }

        // 立即发送一次心跳宣告Leader身份
        sendHeartbeats();
    }






    /**
     * 处理AppendEntries请求（心跳）
     * @param args 心跳请求参数
     * @return 心跳回复
     */
    public synchronized AppendEntriesReply handleAppendEntriesRequest(AppendEntriesArgs args) {
        AppendEntriesReply reply = new AppendEntriesReply();

        // 1. 如果leader的任期小于当前任期，拒绝请求
        if (args.term < currentTerm) {
            reply.term = currentTerm;
            reply.success = false;
            return reply;
        }

        if(args.term > currentTerm){
            System.out.println("任期更新: " + currentTerm + " -> " + args.term);
            currentTerm = args.term;
            state = RoleState.FOLLOWER; // 转为跟随者状态
            votedFor = -1; // 重置投票状态
            //持久化
            persist();
        }

        // 重置心跳和选举超时
        resetElectionTimeout();

        // 收到来自leader的有效AppendEntries，转为follower
        if (state == RoleState.CANDIDATE) {
            state = RoleState.FOLLOWER;
        }
        
        reply.term = currentTerm;

        //2.reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if(args.prevLogIndex >= log.size()){
            reply.success = false;
            reply.xLen = log.size();
            reply.xIndex = -1;
            reply.xTerm = -1; // 没有冲突的任期
            System.out.println("失败: prevLogIndex " + args.prevLogIndex + " 超出日志范围，当前日志长度为 " + log.size());
            return reply;
        }

        if(args.prevLogTerm != log.get(args.prevLogIndex).getLogTerm()){
            reply.success = false;
            int conflictTerm = log.get(args.prevLogIndex).getLogTerm();
            reply.xTerm = conflictTerm;

            reply.xIndex = args.prevLogIndex;
            while (reply.xIndex > 0 && log.get(reply.xIndex - 1).getLogTerm() == conflictTerm) {
                reply.xIndex--;
            }
            reply.xLen = log.size();

            System.out.println(STR."失败: prevLogTerm \{args.prevLogTerm} 与日志条目不匹配，当前日志长度为 \{log.size()}");
            return reply;
        }

        //3.If an existing entry conflicts with a new one (same index but different term), delete the existing entry and all that follow it
        //4.Append any new entries not already in the log
        if(!args.entries.isEmpty()){
            int insertIndex = args.prevLogIndex +1;
            System.out.println("接收到新日志条目，插入索引: " + insertIndex + ", 条目数量: " + args.entries.size());

            int conflictIndex = -1;
            for(int i=0;i<args.entries.size();i++){
                int currentIndex = insertIndex + i;
                if(currentIndex < log.size()){
                    if(log.get(currentIndex).getLogTerm() != args.entries.get(i).getLogTerm()){
                        conflictIndex = currentIndex;
                        break; // 找到冲突的索引，停止处理
                    }
                }else{
                    conflictIndex = currentIndex;
                    break;
                }
            }
            if(conflictIndex != -1){
                if(conflictIndex < log.size()){
                    List<LogEntry> subList = new ArrayList<>(log.subList(0, conflictIndex));
                    log.clear();
                    log.addAll(subList); // 保留冲突前的日志
                }

                if(lastApplied >= conflictIndex){
                    int oldLastApplied = lastApplied;
                    lastApplied = conflictIndex - 1; // 更新lastApplied为冲突索引前一个
                    System.out.println("更新 lastApplied: " + oldLastApplied + " -> " + lastApplied);
                }
                if(commitIndex >= conflictIndex){
                    int oldCommitIndex = commitIndex;
                    commitIndex = conflictIndex - 1; // 更新commitIndex为冲突索引前一个
                    System.out.println("更新 commitIndex: " + oldCommitIndex + " -> " + commitIndex);
                }

                int startAppendIndex = conflictIndex - insertIndex;
                for(int i=startAppendIndex; i<args.entries.size();i++){
                    log.add(args.entries.get(i));
                    System.out.println("追加日志条目: " + args.entries.get(i));
                }
                // 持久化日志
                persist();
            }
        }
        else{
            if(log.size() > args.prevLogIndex +1){
                System.out.println("心跳截断多余日志，清除索引 " + (args.prevLogIndex + 1) + " 之后的日志");
                List<LogEntry> subList = new ArrayList<>(log.subList(0, args.prevLogIndex + 1));
                log.clear();
                log.addAll(subList);
                // 持久化日志
                persist();

            }
        }

        //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if(args.leaderCommit > commitIndex){
            int oldCommitIndex = commitIndex;
            int lastNewEntryIndex = args.prevLogIndex + args.entries.size();
            commitIndex = Math.min(args.leaderCommit, lastNewEntryIndex);
            System.out.println("更新 commitIndex: " + oldCommitIndex + " -> " + commitIndex);
            
            // 通知applier线程有新的提交
            notifyApplier();
        }
        reply.success = true;
        reply.xLen = log.size();
        reply.xIndex = -1; // 没有冲突的索引
        reply.xTerm = -1; // 没有冲突的任期

        return reply;

    }

    /**
     * 发送心跳到所有节点
     */
    public void sendHeartbeats() {
        AppendEntriesArgs args;
        if (state != RoleState.LEADER) {
            return;
        }

        // 每次发送心跳时重置自己的选举超时时间
        resetElectionTimeout();

        for (Integer peerId : peerIds) {
            if (peerId == selfId) {
                continue;
            }

            synchronized (lock){
                if(state != RoleState.LEADER){
                    return;
                }

                int prevLogIndex = nextIndex[peerId] - 1; // 修复：prevLogIndex应该是nextIndex - 1
                List<LogEntry> entries;

                if(nextIndex[peerId] <= getLastLogIndex()){
                    int startArrayIndex = nextIndex[peerId];
                    if(startArrayIndex < log.size()) {
                        entries = new ArrayList<>(log.subList(startArrayIndex, log.size()));
                    }else{
                        entries = new ArrayList<>();
                    }
                }
                else{
                    entries = new ArrayList<>();
                }

                args = new AppendEntriesArgs();
                args.term = currentTerm;
                args.leaderId = selfId;
                args.prevLogIndex = prevLogIndex;
                args.prevLogTerm = log.get(prevLogIndex).getLogTerm();
                args.entries = entries;
                args.leaderCommit = commitIndex;
            }


            AppendEntriesArgs finalArgs = args;
            network.sendAppendEntries(peerId, args)
                    .thenAccept(reply -> {
                        synchronized (lock){
                            if (state!= RoleState.LEADER || currentTerm != finalArgs.term){
                                return;
                            }

                            if(reply.term > currentTerm){
                                System.out.println("心跳过程中发现更高任期，退位");
                                currentTerm = reply.term;
                                state = RoleState.FOLLOWER;
                                votedFor = -1;
                                resetElectionTimeout();
                                // 持久化
                                persist();
                            }
                            else if(reply.success){
                                nextIndex[peerId] = finalArgs.prevLogIndex + finalArgs.entries.size() +1;
                                matchIndex[peerId] = finalArgs.prevLogIndex + finalArgs.entries.size();

                                if(finalArgs.entries.size() >0){
                                    System.out.println("心跳中成功复制到节点 " + peerId + "，nextIndex: " + nextIndex[peerId] + ", matchIndex: " + matchIndex[peerId]);
                                    updateCommitIndex(); //检查是否可以提交新的日志条目
                                }
                            }
                            else{
                                if(reply.term < currentTerm){
                                    int oldNextIndex = nextIndex[peerId];
                                    nextIndex[peerId] = optimizeNextIndex(peerId, reply);
                                    System.out.println("心跳中节点 " + peerId + " 返回失败，更新 nextIndex: " + oldNextIndex + " -> " + nextIndex[peerId]);
                                }
                            }
                        }
                    })
                    .exceptionally(throwable -> {
                        System.err.println("Failed to send heartbeat to node " + peerId + ": " + throwable.getMessage());
                        // 网络错误时不要放弃，尝试继续发送
                        return null;
                    });
        }
    }

    public void updateCommitIndex(){
        if(state != RoleState.LEADER){
            return;
        }

        for (int n=getLastLogIndex(); n > commitIndex; n--){
            if(n<log.size() && log.get(n).getLogTerm() == currentTerm){
                int count = 1; // 自己先投一票

                for(int i : peerIds){
                    if(i != selfId && matchIndex[i] >=n){
                        count++;
                    }
                }

                if(count >= peerIds.size()/2 +1){
                    System.out.println("提交日志条目到索引 " + n + "，当前任期: " + currentTerm);
                    commitIndex = n;
                    
                    // 通知applier线程有新的提交
                    notifyApplier();
                    return;
                }else{
                    System.out.println("无法提交日志条目到索引 " + n + "，需要更多投票，当前投票数: " + count);
                }
            }
        }
    }





    public int optimizeNextIndex(int serverId, AppendEntriesReply reply){
        if(reply.xTerm == -1){
            return reply.xLen;
        }

        int conflictTerm = reply.xTerm;
        int lastIndexOfXTerm = -1;

        for(int i=log.size()-1;i>=0;i--){
            if(log.get(i).getLogTerm() == conflictTerm){
                lastIndexOfXTerm = i;
                break;
            }
        }

        if(lastIndexOfXTerm != -1){
            return lastIndexOfXTerm+1;
        }
        else{
            return reply.xIndex;
        }
    }



    public synchronized AppendResult start(RespArray command){
        AppendResult result = new AppendResult();
        if(state != RoleState.LEADER){
            result.setCurrentTerm(currentTerm);
            result.setNewLogIndex(-1);
            result.setSuccess(false);
            return result;
        }

        final int curTerm = currentTerm;
        LogEntry newEntry = new LogEntry(log.size(), curTerm, command);

        log.add(newEntry);
        // 持久化日志
        persist();

        if(state != RoleState.LEADER || currentTerm !=curTerm){
            log.removeLast();
            //持久化回滚
            persist();
            System.out.println("当前状态不是领导者或任期已变更，无法添加日志条目");
            result.setCurrentTerm(currentTerm);
            result.setNewLogIndex(-1);
            result.setSuccess(false);
            return result;
        }

        replicationLogEntries();

        result.setCurrentTerm(currentTerm);
        result.setNewLogIndex(newEntry.getLogIndex());
        result.setSuccess(true);
        return result;
    }

    /**
     * 启动applier线程
     */
    private void startApplier() {
        if (applierRunning) {
            return;
        }
        
        applierRunning = true;
        applierThread = Thread.ofVirtual().name("raft-applier-" + selfId).start(this::applierLoop);
        System.out.println("Node " + selfId + " applier线程已启动");
    }

    /**
     * 停止applier线程
     */
    public void stopApplier() {
        applierRunning = false;
        if (applierThread != null) {
            synchronized (applierLock) {
                applierLock.notifyAll(); // 唤醒可能在等待的applier线程
            }
            try {
                applierThread.join(1000); // 等待最多1秒
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 通知applier线程有新的提交
     */
    private void notifyApplier() {
        if (applierRunning) {
            synchronized (applierLock) {
                applierLock.notify();
            }
        }
    }
    
    /**
     * 通知applier线程有新的提交（公开方法，用于测试）
     */
    public void notifyApplierForTest() {
        notifyApplier();
    }
    
    /**
     * 设置commitIndex（用于测试）
     */
    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }
    
    /**
     * 获取RedisCore实例（用于测试）
     */
    public RedisCore getRedisCore() {
        return redisCore;
    }

    /**
     * applier主循环
     */
    private void applierLoop() {
        while (applierRunning) {
            try {
                // 检查是否有新的日志条目需要应用
                boolean hasWork = false;
                synchronized (lock) {
                    hasWork = lastApplied < commitIndex;
                }
                
                if (!hasWork) {
                    // 没有工作，等待通知
                    synchronized (applierLock) {
                        try {
                            applierLock.wait(100); // 最多等待100ms，避免死锁
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    continue;
                }
                
                // 应用日志条目
                applyLogEntries();
                
            } catch (Exception e) {
                System.err.println("Node " + selfId + " applier线程发生错误: " + e.getMessage());
                e.printStackTrace();
                // 发生错误时短暂休息，避免CPU占用过高
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        System.out.println("Node " + selfId + " applier线程已停止");
    }

    /**
     * 应用日志条目到状态机
     */
    private void applyLogEntries() {
        List<LogEntry> toApply = new ArrayList<>();
        
        synchronized (lock) {
            // 收集需要应用的日志条目
            while (lastApplied < commitIndex) {
                lastApplied++;
                int applyIndex = lastApplied;
                
                if (applyIndex >= log.size()) {
                    System.err.println("Node " + selfId + " 日志索引越界，lastApplied: " + lastApplied + ", log.size(): " + log.size());
                    lastApplied--; // 回滚
                    break;
                }
                
                LogEntry entry = log.get(applyIndex);
                if (entry.getCommand() != null) {
                    toApply.add(entry);
                }
            }
        }
        
        // 在锁外执行状态机操作，避免长时间持锁
        for (LogEntry entry : toApply) {
            try {
                applyLogEntry(entry);
            } catch (Exception e) {
                System.err.println("Node " + selfId + " 应用日志条目失败: " + entry + ", 错误: " + e.getMessage());
                // 可以选择回滚或者继续，这里选择继续
            }
        }
    }

    /**
     * 应用单个日志条目
     */
    private void applyLogEntry(LogEntry entry) {
        if (redisCore == null) {
            return;
        }
        
        RespArray command = entry.getCommand();
        if (command == null || command.getContent() == null || command.getContent().length == 0) {
            return;
        }
        
        try {
            // 解析命令
            final String commandName = ((BulkString) command.getContent()[0])
                    .getContent().getString().toUpperCase();
            final Resp[] content = command.getContent();
            final String[] args = new String[content.length - 1];
            
            for (int i = 1; i < content.length; i++) {
                if (content[i] instanceof BulkString) {
                    args[i - 1] = ((BulkString) content[i]).getContent().getString();
                } else {
                    System.err.println("Node " + selfId + " 日志条目参数类型不正确: " + content[i].getClass().getSimpleName());
                    return;
                }
            }
            
            // 执行命令
            boolean success = redisCore.executeCommand(commandName, args);
            if (success) {
                System.out.println(STR."Node \{selfId} 成功应用日志条目 \{entry.getLogIndex()}: \{commandName} \{String.join(" ", args)}");
            } else {
                System.err.println(STR."Node \{selfId} 应用日志条目失败 \{entry.getLogIndex()}: \{commandName} \{String.join(" ", args)}");
            }
            
        } catch (Exception e) {
            System.err.println("Node " + selfId + " 解析或执行日志条目时发生异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void replicationLogEntries(){
        synchronized (lock){
            if(state != RoleState.LEADER){
                return; // 只有领导者可以进行日志复制
            }
        }

        for(Integer peerId: peerIds){
            if( peerId == selfId){
                continue;
            }
            Thread.ofVirtual().start(() -> {
                sendAppendEntriesToPeer(peerId);
            });
        }
    }

    private void sendAppendEntriesToPeer(Integer peerId) {
        AppendEntriesArgs args;
        final int curTerm;
        synchronized (lock){
            if(state != RoleState.LEADER){
                return; // 只有领导者可以进行日志复制
            }

            int prevLogIndex = nextIndex[peerId]-1;
            List<LogEntry> entries;

            if(nextIndex[peerId] <= getLastLogIndex()){
                int startArrayIndex = nextIndex[peerId];
                if(startArrayIndex <log.size()){
                    entries = new ArrayList<>(log.subList(startArrayIndex, log.size()));
                }else{
                    entries = new ArrayList<>();
                }
            }else{
                entries = new ArrayList<>();
            }

            args = new AppendEntriesArgs().builder().term(currentTerm)
                    .leaderId(selfId)
                    .prevLogIndex(prevLogIndex)
                    .prevLogTerm(log.get(prevLogIndex).getLogTerm())
                    .entries(entries)
                    .leaderCommit(commitIndex).build();
            curTerm = currentTerm;
        }

        network.sendAppendEntries(peerId, args).thenAccept(
                reply ->{
                    synchronized (lock){
                        if(state !=RoleState.LEADER || currentTerm != curTerm){
                            return;
                        }

                        if(reply.success){
                            nextIndex[peerId] = args.prevLogIndex+ args.entries.size() + 1;
                            matchIndex[peerId] = args.prevLogIndex + args.entries.size();
                            System.out.println("节点 " + selfId + " 成功复制日志到节点 " + peerId + "，nextIndex: " + nextIndex[peerId] + ", matchIndex: " + matchIndex[peerId]);
                            updateCommitIndex();
                        }
                        else{
                            if(reply.term > curTerm){
                                System.out.println("发现更高任期，退位");
                                currentTerm = reply.term;
                                state = RoleState.FOLLOWER;
                                votedFor = -1; // 重置投票状态
                                resetElectionTimeout();
                            }
                            else{
                                int oldNextIndex = nextIndex[peerId];
                                nextIndex[peerId] = optimizeNextIndex(peerId, reply);
                                System.out.println("节点 " + selfId + " 复制日志到节点 " + peerId + " 失败，更新 nextIndex: " + oldNextIndex + " -> " + nextIndex[peerId]);
                            }
                        }
                    }
                }
        ).exceptionally(throwable -> {
            System.err.println("Failed to send AppendEntries to node " + peerId + ": " + throwable.getMessage());
            return null;
        });


    }

    public void persist(){
        writeRaftInfo();
        writeLog();
    }

    /**
     * 写入 Raft 日志到文件
     */
    public void writeLog(){
        if(raftLogManager == null){
            System.err.println("Node " + selfId + " RaftLogManager 未初始化，无法写入日志");
            return;
        }

        Thread.ofVirtual().start(()->{
            try{
                for(LogEntry entry : log){
                    raftLogManager.write(entry);
                }
                System.out.println("Node " + selfId + " 成功写入日志到文件，日志大小: " + log.size());
            }catch(Exception e){
                System.err.println("Node " + selfId + " 写入日志失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    /**
     * 增量写入单个日志条目
     */
    public void appendLogEntry(LogEntry entry) {
        if(raftLogManager == null){
            System.err.println("Node " + selfId + " RaftLogManager 未初始化，无法写入日志条目");
            return;
        }

        Thread.ofVirtual().start(()->{
            try{
                raftLogManager.write(entry);
                System.out.println("Node " + selfId + " 成功追加日志条目: " + entry.getLogIndex());
            }catch(Exception e){
                System.err.println("Node " + selfId + " 追加日志条目失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void writeRaftInfo() {
        if (raftStateFileName == null) {
            System.err.println("Node " + selfId + " Raft状态文件名未初始化");
            return;
        }
        
        Thread.ofVirtual().start(()->{
           try(FileOutputStream fos = new FileOutputStream(raftStateFileName)){
               // 写入当前任期 (4字节，大端序)
               fos.write((currentTerm >>> 24) & 0xFF);
               fos.write((currentTerm >>> 16) & 0xFF);
               fos.write((currentTerm >>> 8) & 0xFF);
               fos.write(currentTerm & 0xFF);
               
               // 写入投票对象 (4字节，大端序)
               fos.write((votedFor >>> 24) & 0xFF);
               fos.write((votedFor >>> 16) & 0xFF);
               fos.write((votedFor >>> 8) & 0xFF);
               fos.write(votedFor & 0xFF);
               
               fos.flush();
               System.out.println("Node " + selfId + " 成功写入Raft状态: term=" + currentTerm + ", votedFor=" + votedFor);
           }catch(Exception e){
               System.err.println("Node " + selfId + " 写入Raft状态失败: " + e.getMessage());
               e.printStackTrace();
           }
        });
    }
    
    /**
     * 获取最后一条日志的索引
     */
    public int getLastLogIndex() {
        return log.size() - 1; // 日志索引从0开始，但第0个是dummy entry
    }
    
    /**
     * 获取最后一条日志的任期
     */
    public int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getLogTerm();
    }
    
    /**
     * 获取指定索引的日志条目
     */
    public LogEntry getLogEntry(int index) {
        if (index <= 0 || index > log.size()) {
            return null;
        }
        return log.get(index - 1);
    }
    
    /**
     * 检查是否为Leader
     */
    public boolean isLeader() {
        return state == RoleState.LEADER;
    }

    /**
     * 获取lastApplied（用于测试）
     */
    public int getLastApplied() {
        return lastApplied;
    }
    
    /**
     * 获取commitIndex（用于测试）
     */
    public int getCommitIndex() {
        return commitIndex;
    }

    public void setLog(List<LogEntry> result) {
            this.log.clear();
            this.log.addAll(result);
            System.out.println("Node " + selfId + " 已加载日志，日志大小: " + log.size());
    }

    
    /**
     * 关闭 Raft 实例，清理资源
     */
    public void shutdown() {
        System.out.println("Node " + selfId + " 开始关闭...");
        
        // 停止 applier 线程
        stopApplier();
        
        // 最后一次持久化
        if (raftLogManager != null || raftStateFileName != null) {
            persist();
            // 等待持久化完成
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // 关闭 RaftLogManager
        if (raftLogManager != null) {
            try {
                raftLogManager.close();
            } catch (Exception e) {
                System.err.println("Node " + selfId + " 关闭 RaftLogManager 失败: " + e.getMessage());
            }
        }
        
        System.out.println("Node " + selfId + " 已关闭");
    }
}

package site.hnfy258.raft;

import site.hnfy258.core.RoleState;
import site.hnfy258.network.RaftNetwork;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Raft节点的高层抽象
 * 负责管理Raft算法实例和网络层，处理定时器等
 */
public class RaftNode {

    private final int serverId;
    private final Raft raft;
    private final RaftNetwork network;
    private final ScheduledExecutorService scheduler;

    // 定时器相关
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    // 超时时间配置 - 真实网络环境优化
    private final int MIN_ELECTION_TIMEOUT = 3000; // ms - 3秒，考虑网络延迟和GC
    private final int MAX_ELECTION_TIMEOUT = 6000; // ms - 6秒，保证稳定性
    private final int HEARTBEAT_INTERVAL = 500; // ms - 500ms，适应网络延迟

    private boolean started = false;

    public RaftNode(int serverId, int[] peerIds, RaftNetwork network) {
        this.serverId = serverId;
        this.network = network;
        this.raft = new Raft(serverId, peerIds, network);
        this.raft.setNodeRef(this); // 设置反向引用
        this.scheduler = Executors.newScheduledThreadPool(2);
    }

    /**
     * 启动Raft节点
     */
    public void start() {
        if (started) {
            return;
        }

        started = true;

        // 启动网络层
        network.start(serverId, raft);

        // 启动选举定时器
        startElectionTimer();

        System.out.println("Raft node " + serverId + " started");
    }

    /**
     * 停止Raft节点
     */
    public void stop() {
        if (!started) {
            return;
        }

        started = false;

        // 停止定时器
        stopElectionTimer();
        stopHeartbeatTimer();

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 停止网络层
        network.stop();

        System.out.println("Raft node " + serverId + " stopped");
    }

    /**
     * 手动触发选举（用于测试）
     */
    public void triggerElection() {
        synchronized (raft) {
            raft.setState(RoleState.CANDIDATE);
            raft.startElection();
        }
    }

    /**
     * 获取当前状态
     */
    public RoleState getState() {
        return raft.getState();
    }

    /**
     * 获取当前任期
     */
    public int getCurrentTerm() {
        return raft.getCurrentTerm();
    }

    /**
     * 获取Raft实例（用于测试）
     */
    public Raft getRaft() {
        return raft;
    }

    /**
     * 启动选举定时器
     */
    private void startElectionTimer() {
        stopElectionTimer();

        int timeout = generateElectionTimeout();
        electionTimer = scheduler.schedule(() -> {
            if (!started) {
                return;
            }

            synchronized (raft) {
                if (raft.getState() == RoleState.FOLLOWER || raft.getState() == RoleState.CANDIDATE) {
                    System.out.println("Node " + serverId + " election timeout, starting election");
                    raft.setState(RoleState.CANDIDATE);
                    raft.startElection();

                    // 重启选举定时器，等待下次超时或收到心跳
                    startElectionTimer();
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止选举定时器
     */
    private void stopElectionTimer() {
        if (electionTimer != null && !electionTimer.isDone()) {
            electionTimer.cancel(false);
        }
    }

    /**
     * 启动心跳定时器
     */
    private void startHeartbeatTimer() {
        stopHeartbeatTimer();

        heartbeatTimer = scheduler.scheduleAtFixedRate(() -> {
            if (!started) {
                return;
            }

            synchronized (raft) {
                if (raft.getState() == RoleState.LEADER) {
                    raft.sendHeartbeats();
                } else {
                    // 如果不再是Leader，停止心跳定时器，启动选举定时器
                    stopHeartbeatTimer();
                    startElectionTimer();
                }
            }
        }, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止心跳定时器
     */
    private void stopHeartbeatTimer() {
        if (heartbeatTimer != null && !heartbeatTimer.isDone()) {
            heartbeatTimer.cancel(false);
        }
    }

    /**
     * 生成随机选举超时时间
     */
    private int generateElectionTimeout() {
        return ThreadLocalRandom.current().nextInt(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
    }

    /**
     * 重置选举定时器（收到心跳时调用）
     */
    public void resetElectionTimer() {
        if (raft.getState() == RoleState.FOLLOWER) {
            startElectionTimer();
        }
    }

    /**
     * 成为Leader时调用
     */
    public void onBecomeLeader() {
        stopElectionTimer();
        startHeartbeatTimer();
    }

    /**
     * 成为Follower时调用
     */
    public void onBecomeFollower() {
        stopHeartbeatTimer();
        startElectionTimer();
    }
}


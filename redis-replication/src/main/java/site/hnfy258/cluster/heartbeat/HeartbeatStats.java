package site.hnfy258.cluster.heartbeat;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class HeartbeatStats {
    private volatile boolean running;
    private volatile int failedAttempts;
    private volatile long lastSuccessful;

    public HeartbeatStats() {
        this.running = false;
        this.failedAttempts = 0;
        this.lastSuccessful = 0;
    }

    public void recordHeartbeatSent(){
        this.running = true;
        this.failedAttempts = 0; // 重置失败次数
        this.lastSuccessful = System.currentTimeMillis(); // 记录成功时间
    }

    public void recordHeartbeatFailed(){
        this.failedAttempts++;
    }

    public void resetFailedCount(){
        this.failedAttempts = 0; // 重置失败次数
    }

    public long getTimeSinceLastSuccess() {
        return System.currentTimeMillis() - lastSuccessful; // 返回自上次成功以来的时间差
    }
}

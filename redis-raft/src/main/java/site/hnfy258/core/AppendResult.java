package site.hnfy258.core;

import lombok.Getter;
import lombok.Setter;

/**
 * Raft Start方法的返回结果
 */
@Getter
@Setter
public class AppendResult {
    private int newLogIndex;    // 分配给命令的日志索引
    private int currentTerm;    // 当前任期
    private boolean success;    // 是否成功接受命令
    
    public AppendResult() {
        this.newLogIndex = -1;
        this.currentTerm = 0;
        this.success = false;
    }
    
    public AppendResult(int newLogIndex, int currentTerm, boolean success) {
        this.newLogIndex = newLogIndex;
        this.currentTerm = currentTerm;
        this.success = success;
    }
    
    public static AppendResult success(int index, int term) {
        return new AppendResult(index, term, true);
    }
    
    public static AppendResult failure(int term) {
        return new AppendResult(-1, term, false);
    }
}

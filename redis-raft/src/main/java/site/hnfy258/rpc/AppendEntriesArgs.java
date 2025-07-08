package site.hnfy258.rpc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import site.hnfy258.core.LogEntry;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AppendEntriesArgs implements Serializable {
    public int term; // leader's term
    public int leaderId; // so follower can redirect clients
    public int prevLogIndex; // index of log entry immediately preceding new ones
    public int prevLogTerm; // term of prevLogIndex entry
    public List<LogEntry> entries; // log entries to store (empty for heartbeat)
    public int leaderCommit; // leader's commitIndex
}

package site.hnfy258.rpc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesReply implements Serializable {
    public int term; // currentTerm, for leader to update itself
    public boolean success; // true if follower contained entry matching prevLogIndex and prevLogTerm

    public int xTerm;
    public int xIndex;
    public int xLen;


}

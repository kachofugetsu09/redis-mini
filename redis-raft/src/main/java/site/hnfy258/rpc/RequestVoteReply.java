package site.hnfy258.rpc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteReply implements Serializable {
    public int term; // currentTerm, for candidate to update itself
    public boolean voteGranted; // true means candidate received vote
}

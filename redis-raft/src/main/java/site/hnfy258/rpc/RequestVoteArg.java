package site.hnfy258.rpc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteArg implements Serializable {
    public int term;  //candidate term
    public int candidateId; // candidateId
    public int lastLogIndex; //index of candidate's last log entry
    public int lastLogTerm; //term of candidate's last log entry
}

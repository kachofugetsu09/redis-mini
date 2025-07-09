package site.hnfy258.core;

import lombok.*;
import site.hnfy258.protocal.RespArray;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class LogEntry implements Serializable {
    private int logIndex;
    private int logTerm;
    private RespArray command;

    public int getLogTerm(int index){
        if(index <0){
            return -1;
        }
        if(index ==0){
            return 0;
        }
        return logTerm;
    }
}

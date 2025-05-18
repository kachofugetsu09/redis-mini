package site.hnfy258.datastructure;

import site.hnfy258.protocal.Resp;

import java.util.List;

public interface RedisData {
    long timeout();
    void setTimeout(long timeout);
    List<Resp> convertToResp();
}

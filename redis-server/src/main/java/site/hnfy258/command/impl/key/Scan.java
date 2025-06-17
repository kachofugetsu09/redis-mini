package site.hnfy258.command.impl.key;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.RespInteger;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.datastructure.RedisBytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class Scan implements Command {
    private final RedisContext context;
    private Resp[] array;
    private int cursor = 0;
    private String pattern;
    private int count = 10;

    public Scan(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.SCAN;
    }    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            RedisBytes cursorBytes = ((BulkString) array[1]).getContent();
            this.cursor = Integer.parseInt(cursorBytes.getString());
        }
        if (array.length >= 4) {
            RedisBytes optionBytes = ((BulkString) array[2]).getContent();
            String option = optionBytes.getString().toLowerCase();
            if ("match".equals(option)) {
                RedisBytes patternBytes = ((BulkString) array[3]).getContent();
                this.pattern = patternBytes.getString();
            } else if ("count".equals(option)) {
                RedisBytes countBytes = ((BulkString) array[3]).getContent();
                this.count = Integer.parseInt(countBytes.getString());
            }
        }        if (array.length >= 6) {
            RedisBytes optionBytes = ((BulkString) array[4]).getContent();
            String option = optionBytes.getString().toLowerCase();
            if ("count".equals(option)) {
                RedisBytes countBytes = ((BulkString) array[5]).getContent();
                this.count = Integer.parseInt(countBytes.getString());
            }
        }
    }

    @Override
    public Resp handle() {
        Set<RedisBytes> keys = context.keys();
        List<RedisBytes> result = new ArrayList<>();
        Pattern regex = pattern != null ? Pattern.compile(pattern.replace("*", ".*")) : null;
        
        int i = 0;
        int matched = 0;
        int nextCursor = 0;
        
        for (RedisBytes key : keys) {
            if (i++ < cursor) continue;
            
            if (regex == null || regex.matcher(key.getString()).matches()) {
                result.add(key);
                matched++;
                if (matched >= count) {
                    nextCursor = i;
                    break;
                }
            }
        }
        
        Resp[] response = new Resp[2];
        response[0] = new RespInteger(nextCursor);
        
        Resp[] keyArray = new Resp[result.size()];
        for (int j = 0; j < result.size(); j++) {
            keyArray[j] = new BulkString(result.get(j).getBytesUnsafe());
        }
        response[1] = new RespArray(keyArray);
        
        return new RespArray(response);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
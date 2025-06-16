package site.hnfy258.command.impl.key;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.datastructure.RedisBytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class Keys implements Command {
    private final RedisContext context;
    private Resp[] array;
    private String pattern;

    public Keys(RedisContext context) {
        this.context = context;
    }

    @Override
    public CommandType getType() {
        return CommandType.KEYS;
    }

    @Override
    public void setContext(Resp[] array) {
        this.array = array;
        if (array.length >= 2) {
            this.pattern = ((BulkString) array[1]).toString();
        }
    }

    @Override
    public Resp handle() {
        if (pattern == null) {
            return new RespArray(new Resp[0]);
        }

        Set<RedisBytes> keys = context.keys();
        List<RedisBytes> result = new ArrayList<>();
        Pattern regex = Pattern.compile(pattern.replace("*", ".*"));
        
        for (RedisBytes key : keys) {
            if (regex.matcher(key.toString()).matches()) {
                result.add(key);
            }
        }
        
        Resp[] keyArray = new Resp[result.size()];
        for (int i = 0; i < result.size(); i++) {
            keyArray[i] = new BulkString(result.get(i).getBytesUnsafe());
        }
        
        return new RespArray(keyArray);
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }
} 
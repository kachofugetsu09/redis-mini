package site.hnfy258.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.protocal.*;
import site.hnfy258.server.config.RedisServerConfig;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisMiniServerTest {

    private RedisMiniServer server;
    private RedisServerConfig config;

    @Mock
    private Channel channel;

    @Mock
    private ChannelFuture channelFuture;

    @Mock
    private EventLoopGroup bossGroup;

    @Mock
    private EventLoopGroup workerGroup;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        config = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .databaseCount(16)
                .rdbEnabled(false)  // 禁用RDB
                .build();
        server = new RedisMiniServer(config);
    }

    @Test
    void testExecuteCommand() {
        // 测试执行SET命令
        Resp[] setArray = new Resp[]{
            BulkString.SET,
            new BulkString(RedisBytes.fromString("test-key")),
            new BulkString(RedisBytes.fromString("test-value"))
        };
        RespArray setCommand = new RespArray(setArray);
        
        Resp setResponse = server.executeCommand(setCommand);
        assertTrue(setResponse instanceof SimpleString);
        assertEquals("OK", ((SimpleString) setResponse).getContent());

        // 测试执行GET命令
        Resp[] getArray = new Resp[]{
            BulkString.GET,
            new BulkString(RedisBytes.fromString("test-key"))
        };
        RespArray getCommand = new RespArray(getArray);
        
        Resp getResponse = server.executeCommand(getCommand);
        assertTrue(getResponse instanceof BulkString);
        assertEquals("test-value", ((BulkString) getResponse).getContent().getString());
    }

    @Test
    void testExecuteInvalidCommand() {
        // 测试执行无效命令
        Resp[] invalidArray = new Resp[]{
            new BulkString(RedisBytes.fromString("INVALID_COMMAND")),
            new BulkString(RedisBytes.fromString("arg1"))
        };
        RespArray invalidCommand = new RespArray(invalidArray);
        
        Resp response = server.executeCommand(invalidCommand);
        assertTrue(response instanceof Errors);
        assertEquals("ERR unknown command", ((Errors) response).getContent());
    }

    @Test
    void testExecuteEmptyCommand() {
        // 测试执行空命令
        RespArray emptyCommand = new RespArray(new Resp[]{});
        
        Resp response = server.executeCommand(emptyCommand);
        assertTrue(response instanceof Errors);
        assertEquals("ERR empty command", ((Errors) response).getContent());
    }

    @Test
    void testExecuteNonArrayCommand() {
        // 测试执行非数组类型的命令
        Resp command = new SimpleString("invalid");
        
        Resp response = server.executeCommand(command);
        assertTrue(response instanceof Errors);
        assertEquals("ERR Invalid command format", ((Errors) response).getContent());
    }

    @Test
    void testGenerateRdbSnapshot() {
        // 测试生成RDB快照
        assertThrows(UnsupportedOperationException.class, () -> {
            server.generateRdbSnapshot();
        });
    }

    @Test
    void testReplicationOffset() {
        // 测试复制偏移量
        long offset = 1000L;
        server.setReplicationOffset(offset);
        assertEquals(offset, server.getReplicationOffset());
    }
} 
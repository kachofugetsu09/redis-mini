package site.hnfy258.server.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.core.RedisCore;
import site.hnfy258.core.RedisCoreImpl;
import site.hnfy258.datastructure.RedisBytes;
import site.hnfy258.datastructure.RedisData;
import site.hnfy258.datastructure.RedisString;
import site.hnfy258.internal.Sds;
import site.hnfy258.protocal.*;
import site.hnfy258.server.config.RedisServerConfig;
import site.hnfy258.server.context.RedisContext;
import site.hnfy258.server.context.RedisContextImpl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RespCommandHandlerTest {

    @Mock
    private ChannelHandlerContext ctx;
    
    @Mock
    private Channel channel;

    private RespCommandHandler handler;
    private RedisContext redisContext;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // 创建真实的RedisContext
        RedisServerConfig config = RedisServerConfig.builder()
                .host("localhost")
                .port(6379)
                .aofEnabled(false)
                .rdbEnabled(false)
                .build();
        RedisCore redisCore = new RedisCoreImpl(16);
        redisContext = new RedisContextImpl(redisCore, "localhost", 6379, config);

        // 创建命令处理器
        handler = new RespCommandHandler(redisContext);
        
        // 设置基本的mock行为
        when(ctx.channel()).thenReturn(channel);
    }

    @Test
    void testSetCommand() throws Exception {
        // 准备SET命令
        Resp[] commandArray = new Resp[]{
            BulkString.SET,
            new BulkString(RedisBytes.fromString("test-key")),
            new BulkString(RedisBytes.fromString("test-value"))
        };
        RespArray setCommand = new RespArray(commandArray);

        // 执行命令
        Resp response = handler.executeCommand(setCommand);

        // 验证响应
        assertTrue(response instanceof SimpleString);
        assertEquals("OK", ((SimpleString) response).getContent());

        // 验证数据是否正确存储
        RedisData storedValue = redisContext.get(RedisBytes.fromString("test-key"));
        assertNotNull(storedValue);
        assertTrue(storedValue instanceof RedisString);
        assertEquals("test-value", ((RedisString) storedValue).getSds().toString());
    }

    @Test
    void testGetCommand() throws Exception {
        // 先设置一个键值对
        RedisBytes key = RedisBytes.fromString("get-test-key");
        RedisString value = new RedisString(Sds.create("get-test-value".getBytes()));
        redisContext.put(key, value);

        // 准备GET命令
        Resp[] commandArray = new Resp[]{
            BulkString.GET,
            new BulkString(key)
        };
        RespArray getCommand = new RespArray(commandArray);

        // 执行命令
        Resp response = handler.executeCommand(getCommand);

        // 验证响应
        assertTrue(response instanceof BulkString);
        assertEquals("get-test-value", ((BulkString) response).getContent().getString());
    }

    @Test
    void testInvalidCommand() throws Exception {
        // 准备无效命令
        Resp[] commandArray = new Resp[]{
            new BulkString(RedisBytes.fromString("INVALID_COMMAND")),
            new BulkString(RedisBytes.fromString("arg1"))
        };
        RespArray invalidCommand = new RespArray(commandArray);

        // 执行命令
        Resp response = handler.executeCommand(invalidCommand);

        // 验证错误响应
        assertTrue(response instanceof Errors);
        assertEquals("命令不存在", ((Errors) response).getContent());
    }

    @Test
    void testEmptyCommand() throws Exception {
        // 准备空命令
        RespArray emptyCommand = new RespArray(new Resp[]{});

        // 执行命令
        Resp response = handler.executeCommand(emptyCommand);

        // 验证错误响应
        assertTrue(response instanceof Errors);
        assertEquals("命令不能为空", ((Errors) response).getContent());
    }

    @Test
    void testUnsupportedMessageType() throws Exception {
        // 设置channel mock行为
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);
        
        // 测试非RespArray类型的消息
        SimpleString unsupportedMessage = new SimpleString("unsupported");
        
        // 执行命令处理
        handler.channelRead0(ctx, unsupportedMessage);

        // 验证错误响应被发送
        verify(ctx).writeAndFlush(any(Errors.class));
    }
} 
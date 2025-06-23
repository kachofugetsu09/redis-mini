package site.hnfy258.server.command.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import site.hnfy258.server.context.RedisContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CommandExecutorImplTest {

    @Mock
    private RedisContext redisContext;

    private CommandExecutorImpl commandExecutor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        commandExecutor = new CommandExecutorImpl(redisContext);
    }

    @Test
    void testExecuteValidCommand() {
        // 测试有效的SET命令
        String[] args = {"key", "value"};
        assertTrue(commandExecutor.executeCommand("SET", args));
    }

    @Test
    void testExecuteInvalidCommand() {
        // 测试无效的命令
        String[] args = {"arg1"};
        assertFalse(commandExecutor.executeCommand("INVALID_COMMAND", args));
    }

    @Test
    void testExecuteCommandWithNullArgs() {
        // 测试参数为null的情况
        assertFalse(commandExecutor.executeCommand("SET", null));
    }

    @Test
    void testExecuteCommandWithEmptyArgs() {
        // 测试空参数的情况
        String[] args = {};
        assertFalse(commandExecutor.executeCommand("SET", args));
    }

    @Test
    void testExecuteCommandWithNullCommandName() {
        // 测试命令名为null的情况
        String[] args = {"key", "value"};
        assertFalse(commandExecutor.executeCommand(null, args));
    }

    @Test
    void testExecuteCommandWithException() {
        // 测试执行过程中抛出异常的情况
        String[] args = {"key"};  // SET命令需要两个参数，这里只给一个，会抛出异常
        assertFalse(commandExecutor.executeCommand("SET", args));
    }
} 
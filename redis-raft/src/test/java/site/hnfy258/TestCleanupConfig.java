package site.hnfy258;

/**
 * 测试清理配置
 * 定义激进清理的各种策略和参数
 */
public class TestCleanupConfig {
    
    /**
     * 是否启用激进清理模式
     */
    public static final boolean AGGRESSIVE_CLEANUP_ENABLED = true;
    
    /**
     * 是否在每个测试方法前清理
     */
    public static final boolean CLEANUP_BEFORE_EACH_TEST = true;
    
    /**
     * 是否在每个测试方法后清理
     */
    public static final boolean CLEANUP_AFTER_EACH_TEST = true;
    
    /**
     * 是否在测试类之间进行深度清理
     */
    public static final boolean DEEP_CLEANUP_BETWEEN_CLASSES = true;
    
    /**
     * 清理后的等待时间（毫秒）
     */
    public static final int CLEANUP_WAIT_TIME_MS = 500;
    
    /**
     * 深度清理后的等待时间（毫秒）
     */
    public static final int DEEP_CLEANUP_WAIT_TIME_MS = 1500;
    
    /**
     * 是否验证清理后的状态
     */
    public static final boolean VERIFY_CLEAN_STATE = true;
    
    /**
     * 是否在清理失败时抛出异常
     */
    public static final boolean FAIL_ON_CLEANUP_ERROR = false;
    
    /**
     * 清理重试次数
     */
    public static final int CLEANUP_RETRY_COUNT = 3;
    
    /**
     * 清理重试间隔（毫秒）
     */
    public static final int CLEANUP_RETRY_INTERVAL_MS = 200;
    
    /**
     * 额外的文件扩展名需要清理
     */
    public static final String[] ADDITIONAL_FILE_EXTENSIONS = {
        ".tmp", ".temp", ".bak", ".backup", ".old"
    };
    
    /**
     * 额外的目录模式需要清理
     */
    public static final String[] ADDITIONAL_DIRECTORY_PATTERNS = {
        "tmp-*", "temp-*", "backup-*", "test-data-*"
    };
    
    /**
     * 是否启用详细的清理日志
     */
    public static final boolean VERBOSE_CLEANUP_LOGGING = true;
    
    /**
     * 获取清理配置摘要
     */
    public static String getConfigSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("Test Cleanup Configuration:\n");
        sb.append("  - Aggressive cleanup: ").append(AGGRESSIVE_CLEANUP_ENABLED).append("\n");
        sb.append("  - Before each test: ").append(CLEANUP_BEFORE_EACH_TEST).append("\n");
        sb.append("  - After each test: ").append(CLEANUP_AFTER_EACH_TEST).append("\n");
        sb.append("  - Deep cleanup between classes: ").append(DEEP_CLEANUP_BETWEEN_CLASSES).append("\n");
        sb.append("  - Verify clean state: ").append(VERIFY_CLEAN_STATE).append("\n");
        sb.append("  - Cleanup wait time: ").append(CLEANUP_WAIT_TIME_MS).append("ms\n");
        sb.append("  - Deep cleanup wait time: ").append(DEEP_CLEANUP_WAIT_TIME_MS).append("ms\n");
        sb.append("  - Retry count: ").append(CLEANUP_RETRY_COUNT).append("\n");
        return sb.toString();
    }
}

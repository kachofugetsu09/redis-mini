package site.hnfy258.aof.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * 文件操作工具类
 * 
 * <p>提供跨平台兼容的文件操作功能，包括原子性文件移动、安全重命名、
 * 备份创建等核心功能。专为 AOF 文件管理而优化，确保在 Windows 和
 * Unix/Linux 环境下的一致性行为。</p>
 * 
 * <p>主要功能：</p>
 * <ul>
 *   <li>原子性文件移动操作，带降级处理</li>
 *   <li>安全的文件重命名，避免数据丢失</li>
 *   <li>文件备份创建，支持自定义后缀</li>
 *   <li>跨平台目录缓存刷新（Windows 兼容）</li>
 * </ul>
 * 
 * <p>设计原则：</p>
 * <ul>
 *   <li>优先使用原子操作，失败时降级到安全操作</li>
 *   <li>Windows 特殊处理：不执行目录 fsync</li>
 *   <li>完善的异常处理和日志记录</li>
 *   <li>参数验证确保操作安全性</li>
 * </ul>
 * 
 * <p>线程安全性：此类中的方法不是线程安全的，需要外部调用者确保同步。</p>
 * 
 * @author hnfy258
 * @version 1.0.0
 * @since 1.0.0
 */
@Slf4j
public final class FileUtils {
    
    // ==================== 构造函数 ====================
    
    /**
     * 私有构造函数，防止实例化工具类
     * 
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    private FileUtils() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    // ==================== 公共API方法 ====================

    /**
     * 安全地重命名文件，支持原子操作和跨平台兼容性
     * 
     * <p>优先使用系统的原子移动操作（ATOMIC_MOVE），如果不支持则
     * 降级到普通移动操作。确保在各种文件系统和操作系统上的一致行为。</p>
     * 
     * <p>操作流程：</p>
     * <ol>
     *   <li>验证源文件和目标文件参数</li>
     *   <li>尝试原子性移动操作</li>
     *   <li>失败时降级到普通移动</li>
     *   <li>刷新目录缓存（非 Windows 系统）</li>
     * </ol>
     * 
     * @param source 源文件，不能为null且必须存在
     * @param target 目标文件，不能为null
     * @throws IllegalArgumentException 参数无效时抛出
     * @throws RuntimeException 文件重命名失败时抛出，包装底层IO异常
     */
    public static void safeRenameFile(final File source, final File target) {
        // 1. 参数验证
        validateFileParameters(source, target);
        
        try {
            atomicMoveWithFallback(source.toPath(), target.toPath());
            log.debug("文件重命名成功: {} -> {}", source.getAbsolutePath(), 
                     target.getAbsolutePath());
        } catch (final Exception e) {
            throw new RuntimeException(String.format(
                "Failed to rename file from %s to %s", 
                source.getAbsolutePath(), target.getAbsolutePath()), e);
        }
    }

    /**
     * 创建文件备份
     * 
     * <p>将现有文件复制到带有指定后缀的备份文件中。如果备份文件已存在，
     * 会先删除旧备份再创建新备份。支持原子性操作确保备份的一致性。</p>
     * 
     * <p>使用场景：</p>
     * <ul>
     *   <li>AOF 重写前创建备份</li>
     *   <li>关键配置文件的版本保存</li>
     *   <li>数据文件的安全副本创建</li>
     * </ul>
     * 
     * @param file 要备份的文件，不能为null
     * @param backupSuffix 备份文件后缀，不能为null或空字符串
     * @return 备份文件对象，如果源文件不存在则返回null
     * @throws IOException 备份操作失败时抛出
     * @throws IllegalArgumentException 参数无效时抛出
     */
    public static File createBackupFile(final File file, final String backupSuffix) 
            throws IOException {
        // 1. 参数验证
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (backupSuffix == null || backupSuffix.trim().isEmpty()) {
            throw new IllegalArgumentException("Backup suffix cannot be null or empty");
        }
        
        // 2. 检查文件是否存在
        if (!file.exists()) {
            log.debug("文件不存在，跳过备份: {}", file.getAbsolutePath());
            return null;
        }
        
        // 3. 创建备份文件
        final File backupFile = new File(file.getAbsolutePath() + backupSuffix);
        if (backupFile.exists()) {
            if (!backupFile.delete()) {
                log.warn("无法删除已存在的备份文件: {}", backupFile.getAbsolutePath());
            }
        }

        // 4. 原子性移动操作
        atomicMoveWithFallback(file.toPath(), backupFile.toPath());
        log.info("文件备份完成: {} -> {}", file.getAbsolutePath(), 
                backupFile.getAbsolutePath());
        return backupFile;
    }
    
    // ==================== 私有辅助方法 ====================
    
    /**
     * 验证文件操作参数
     * 
     * <p>检查文件操作所需的基本参数，确保操作的安全性和有效性。</p>
     * 
     * @param source 源文件，不能为null且必须存在
     * @param target 目标文件，不能为null
     * @throws IllegalArgumentException 当参数不满足要求时抛出
     */
    private static void validateFileParameters(final File source, final File target) {
        if (source == null) {
            throw new IllegalArgumentException("Source file cannot be null");
        }
        if (target == null) {
            throw new IllegalArgumentException("Target file cannot be null");
        }
        if (!source.exists()) {
            throw new IllegalArgumentException("Source file does not exist: " + 
                                             source.getAbsolutePath());
        }
    }    /**
     * 原子性文件移动操作，带降级处理
     * 
     * <p>优先使用操作系统提供的原子移动功能（ATOMIC_MOVE），如果不支持
     * 或失败，则降级到普通的文件移动操作。确保在各种文件系统上的兼容性。</p>
     * 
     * <p>操作特点：</p>
     * <ul>
     *   <li>原子性：要么完全成功，要么完全失败</li>
     *   <li>降级处理：不支持原子操作时自动使用普通移动</li>
     *   <li>目录刷新：移动完成后刷新目录缓存</li>
     * </ul>
     * 
     * @param source 源路径，不能为null
     * @param target 目标路径，不能为null
     * @throws IOException 移动操作失败时抛出
     */
    private static void atomicMoveWithFallback(final Path source, final Path target) 
            throws IOException {
        try {
            // 1. 尝试原子性移动
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (final Exception e) {
            log.debug("原子性移动失败，尝试普通移动: {}", e.getMessage());
            try {
                // 2. 降级为普通移动操作
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (final Exception ex) {
                throw new IOException(String.format(
                    "Both atomic and regular file move failed from %s to %s", 
                    source, target), ex);
            }
        } finally {
            // 3. 刷新目录缓存（非Windows系统）
            flushDir(target.toAbsolutePath().normalize().getParent());
        }
    }

    /**
     * 刷新目录缓存，确保文件系统元数据同步到磁盘
     * 
     * <p>在支持的操作系统上执行目录 fsync 操作，确保目录项的变更
     * 被持久化到磁盘。Windows 和 z/OS 系统不支持此操作，会被跳过。</p>
     * 
     * <p>技术说明：</p>
     * <ul>
     *   <li>Linux/Unix：执行目录 fsync 确保元数据持久化</li>
     *   <li>Windows：跳过操作，因为不支持目录 fsync</li>
     *   <li>错误处理：目录刷新失败不会影响文件操作本身</li>
     * </ul>
     * 
     * @param path 目录路径，可以为null（将被跳过）
     * @throws IOException 刷新失败时抛出（通常被忽略）
     */
    private static void flushDir(final Path path) throws IOException {
        if (path == null) {
            return;
        }
        
        final String os = System.getProperty("os.name").toLowerCase();
        // Windows和z/OS不支持目录fsync
        if (os.contains("win") || os.contains("z/os")) {
            return;
        }
        
        try (final FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
            dir.force(true);
        } catch (final Exception e) {
            log.debug("目录刷新失败: {}, 错误: {}", path, e.getMessage());
            // 目录刷新失败不应该阻止文件操作
        }
    }

}

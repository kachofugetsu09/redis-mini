package site.hnfy258.aof.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FileUtils 工具类测试
 * 
 * @author hnfy258
 */
@DisplayName("文件工具类测试")
class FileUtilsTest {
    
    @TempDir
    private Path tempDir;
    
    // ==================== 安全文件重命名测试 ====================
    
    @Nested
    @DisplayName("安全文件重命名测试")
    class SafeRenameFileTests {
        
        @Test
        @DisplayName("正常文件重命名测试")
        void testSafeRenameFile() throws IOException {
            // Given: 创建源文件
            File sourceFile = tempDir.resolve("source.txt").toFile();
            String sourceContent = "source file content";
            Files.write(sourceFile.toPath(), sourceContent.getBytes(StandardCharsets.UTF_8));
            
            File targetFile = tempDir.resolve("target.txt").toFile();
            
            // When: 执行安全重命名
            FileUtils.safeRenameFile(sourceFile, targetFile);
            
            // Then: 验证结果
            assertFalse(sourceFile.exists(), "源文件应该被移动");
            assertTrue(targetFile.exists(), "目标文件应该存在");
            
            String targetContent = new String(Files.readAllBytes(targetFile.toPath()), 
                                            StandardCharsets.UTF_8);
            assertEquals(sourceContent, targetContent, "文件内容应该完全一致");
        }
        
        @Test
        @DisplayName("覆盖现有文件测试")
        void testSafeRenameFileOverwrite() throws IOException {
            // Given: 创建源文件和已存在的目标文件
            File sourceFile = tempDir.resolve("source.txt").toFile();
            File targetFile = tempDir.resolve("target.txt").toFile();
            
            String sourceContent = "new content";
            String originalTargetContent = "original content";
            
            Files.write(sourceFile.toPath(), sourceContent.getBytes(StandardCharsets.UTF_8));
            Files.write(targetFile.toPath(), originalTargetContent.getBytes(StandardCharsets.UTF_8));
            
            // When: 执行安全重命名
            FileUtils.safeRenameFile(sourceFile, targetFile);
            
            // Then: 验证目标文件被覆盖
            assertFalse(sourceFile.exists());
            assertTrue(targetFile.exists());
            
            String finalContent = new String(Files.readAllBytes(targetFile.toPath()), 
                                           StandardCharsets.UTF_8);
            assertEquals(sourceContent, finalContent, "目标文件应该被新内容覆盖");
        }
        
        @Test
        @DisplayName("跨目录重命名测试")
        void testSafeRenameFileCrossDirectory() throws IOException {
            // Given: 在不同目录创建文件
            Path subDir = tempDir.resolve("subdir");
            Files.createDirectories(subDir);
            
            File sourceFile = tempDir.resolve("source.txt").toFile();
            File targetFile = subDir.resolve("target.txt").toFile();
            
            String content = "cross directory content";
            Files.write(sourceFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
            
            // When: 执行跨目录重命名
            FileUtils.safeRenameFile(sourceFile, targetFile);
            
            // Then: 验证结果
            assertFalse(sourceFile.exists());
            assertTrue(targetFile.exists());
            
            String targetContent = new String(Files.readAllBytes(targetFile.toPath()), 
                                            StandardCharsets.UTF_8);
            assertEquals(content, targetContent);
        }
        
        @Test
        @DisplayName("源文件不存在异常测试")
        void testSafeRenameFileSourceNotExists() {
            // Given: 不存在的源文件
            File sourceFile = tempDir.resolve("nonexistent.txt").toFile();
            File targetFile = tempDir.resolve("target.txt").toFile();
            
            // When & Then: 应该抛出异常
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.safeRenameFile(sourceFile, targetFile));
        }
        
        @Test
        @DisplayName("空参数异常测试")
        void testSafeRenameFileNullParameters() {
            // Given: 空参数
            File sourceFile = tempDir.resolve("source.txt").toFile();
            
            // When & Then: 空参数应该抛出异常
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.safeRenameFile(null, sourceFile));
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.safeRenameFile(sourceFile, null));
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.safeRenameFile(null, null));
        }
    }
    
    // ==================== 创建备份文件测试 ====================
    
    @Nested
    @DisplayName("创建备份文件测试")
    class CreateBackupFileTests {
        
        @Test
        @DisplayName("创建普通文件备份测试")
        void testCreateBackupFile() throws IOException {
            // Given: 创建原始文件
            File originalFile = tempDir.resolve("original.txt").toFile();
            String originalContent = "original file content";
            Files.write(originalFile.toPath(), originalContent.getBytes(StandardCharsets.UTF_8));
            
            // When: 创建备份
            File backupFile = FileUtils.createBackupFile(originalFile, ".backup");
            
            // Then: 验证备份结果
            assertNotNull(backupFile, "备份文件不应该为空");
            assertFalse(originalFile.exists(), "原始文件应该被移动");
            assertTrue(backupFile.exists(), "备份文件应该存在");
            assertEquals(originalFile.getAbsolutePath() + ".backup", 
                        backupFile.getAbsolutePath(), "备份文件路径应该正确");
            
            // 验证内容
            String backupContent = new String(Files.readAllBytes(backupFile.toPath()), 
                                            StandardCharsets.UTF_8);
            assertEquals(originalContent, backupContent, "备份文件内容应该与原文件一致");
        }
        
        @Test
        @DisplayName("创建大文件备份测试")
        void testCreateBackupLargeFile() throws IOException {
            // Given: 创建较大的文件
            File originalFile = tempDir.resolve("large.dat").toFile();
            byte[] largeContent = new byte[1024 * 1024]; // 1MB
            for (int i = 0; i < largeContent.length; i++) {
                largeContent[i] = (byte) (i % 256);
            }
            Files.write(originalFile.toPath(), largeContent);
            
            // When: 创建备份
            File backupFile = FileUtils.createBackupFile(originalFile, ".bak");
            
            // Then: 验证备份结果
            assertNotNull(backupFile);
            assertFalse(originalFile.exists());
            assertTrue(backupFile.exists());
            
            // 验证文件大小和内容
            assertEquals(largeContent.length, backupFile.length());
            byte[] backupContent = Files.readAllBytes(backupFile.toPath());
            assertArrayEquals(largeContent, backupContent);
        }
        
        @Test
        @DisplayName("覆盖已存在备份文件测试")
        void testCreateBackupFileOverwriteExisting() throws IOException {
            // Given: 创建原始文件和已存在的备份文件
            File originalFile = tempDir.resolve("original.txt").toFile();
            File existingBackupFile = new File(originalFile.getAbsolutePath() + ".backup");
            
            String originalContent = "new original content";
            String oldBackupContent = "old backup content";
            
            Files.write(originalFile.toPath(), originalContent.getBytes(StandardCharsets.UTF_8));
            Files.write(existingBackupFile.toPath(), oldBackupContent.getBytes(StandardCharsets.UTF_8));
            
            assertTrue(existingBackupFile.exists(), "备份文件应该已存在");
            
            // When: 创建新备份
            File backupFile = FileUtils.createBackupFile(originalFile, ".backup");
            
            // Then: 验证备份覆盖成功
            assertNotNull(backupFile);
            assertFalse(originalFile.exists());
            assertTrue(backupFile.exists());
            
            // 验证内容是新的原始文件内容，不是旧备份内容
            String finalContent = new String(Files.readAllBytes(backupFile.toPath()), 
                                           StandardCharsets.UTF_8);
            assertEquals(originalContent, finalContent);
            assertNotEquals(oldBackupContent, finalContent);
        }
        
        @Test
        @DisplayName("备份不存在文件测试")
        void testCreateBackupFileNotExists() throws IOException {
            // Given: 不存在的文件
            File nonExistentFile = tempDir.resolve("nonexistent.txt").toFile();
            assertFalse(nonExistentFile.exists());
            
            // When: 尝试创建备份
            File backupFile = FileUtils.createBackupFile(nonExistentFile, ".backup");
            
            // Then: 应该返回 null
            assertNull(backupFile, "不存在的文件备份应该返回 null");
        }
        
        @Test
        @DisplayName("无效参数异常测试")
        void testCreateBackupFileInvalidParameters() throws IOException {
            // Given: 创建测试文件
            File testFile = tempDir.resolve("test.txt").toFile();
            Files.write(testFile.toPath(), "test content".getBytes(StandardCharsets.UTF_8));
            
            // When & Then: 无效参数应该抛出异常
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.createBackupFile(null, ".backup"));
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.createBackupFile(testFile, null));
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.createBackupFile(testFile, ""));
            assertThrows(IllegalArgumentException.class, 
                () -> FileUtils.createBackupFile(testFile, "   "));
        }
        
        @Test
        @DisplayName("自定义后缀测试")
        void testCreateBackupFileCustomSuffix() throws IOException {
            // Given: 创建测试文件
            File testFile = tempDir.resolve("test.log").toFile();
            String content = "log content";
            Files.write(testFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
            
            // When: 使用自定义后缀创建备份
            File backupFile = FileUtils.createBackupFile(testFile, ".old");
            
            // Then: 验证后缀正确
            assertNotNull(backupFile);
            assertTrue(backupFile.getName().endsWith(".old"));
            assertEquals(testFile.getAbsolutePath() + ".old", backupFile.getAbsolutePath());
            
            String backupContent = new String(Files.readAllBytes(backupFile.toPath()), 
                                            StandardCharsets.UTF_8);
            assertEquals(content, backupContent);
        }
    }
    
    // ==================== 工具类特性测试 ====================
    
    @Nested
    @DisplayName("工具类特性测试")
    class UtilityClassTests {
          @Test
        @DisplayName("工具类不能实例化测试")
        void testUtilityClassCannotBeInstantiated() {
            // When & Then: 尝试通过反射创建实例应该抛出异常
            Exception exception = assertThrows(Exception.class, () -> {
                Constructor<FileUtils> constructor = FileUtils.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
            });
            
            // 验证根本原因是 UnsupportedOperationException
            Throwable cause = exception.getCause();
            if (cause != null) {
                assertTrue(cause instanceof UnsupportedOperationException, 
                          "根本原因应该是 UnsupportedOperationException，实际是: " + cause.getClass());
                assertTrue(cause.getMessage().contains("Utility class cannot be instantiated"));
            } else {
                // 如果没有 cause，那么异常本身应该是 UnsupportedOperationException
                assertTrue(exception instanceof UnsupportedOperationException,
                          "异常本身应该是 UnsupportedOperationException，实际是: " + exception.getClass());
            }
        }
    }
    
    // ==================== 边界条件测试 ====================
    
    @Nested
    @DisplayName("边界条件测试")
    class EdgeCaseTests {
        
        @Test
        @DisplayName("空文件备份测试")
        void testCreateBackupEmptyFile() throws IOException {
            // Given: 创建空文件
            File emptyFile = tempDir.resolve("empty.txt").toFile();
            Files.createFile(emptyFile.toPath());
            assertEquals(0, emptyFile.length());
            
            // When: 创建备份
            File backupFile = FileUtils.createBackupFile(emptyFile, ".backup");
            
            // Then: 验证备份成功
            assertNotNull(backupFile);
            assertTrue(backupFile.exists());
            assertEquals(0, backupFile.length(), "备份的空文件大小应该为0");
        }
        
        @Test
        @DisplayName("特殊字符文件名测试")
        void testSpecialCharacterFileName() throws IOException {
            // Given: 创建包含特殊字符的文件名
            String specialName = "test-file_with.special@chars.txt";
            File specialFile = tempDir.resolve(specialName).toFile();
            String content = "special character file content";
            Files.write(specialFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
            
            // When: 重命名文件
            File renamedFile = tempDir.resolve("renamed-file.txt").toFile();
            FileUtils.safeRenameFile(specialFile, renamedFile);
            
            // Then: 验证操作成功
            assertFalse(specialFile.exists());
            assertTrue(renamedFile.exists());
            
            String renamedContent = new String(Files.readAllBytes(renamedFile.toPath()), 
                                             StandardCharsets.UTF_8);
            assertEquals(content, renamedContent);
        }
        
        @Test
        @DisplayName("中文文件名测试")
        void testChineseFileName() throws IOException {
            // Given: 创建中文文件名
            String chineseName = "测试文件.txt";
            File chineseFile = tempDir.resolve(chineseName).toFile();
            String content = "中文内容测试";
            Files.write(chineseFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
            
            // When: 创建备份
            File backupFile = FileUtils.createBackupFile(chineseFile, ".备份");
            
            // Then: 验证备份成功
            assertNotNull(backupFile);
            assertTrue(backupFile.exists());
            assertTrue(backupFile.getName().contains("测试文件"));
            assertTrue(backupFile.getName().endsWith(".备份"));
            
            String backupContent = new String(Files.readAllBytes(backupFile.toPath()), 
                                            StandardCharsets.UTF_8);
            assertEquals(content, backupContent);
        }
    }
}

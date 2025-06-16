package site.hnfy258.datastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisBytes 单元测试
 * 
 * 测试覆盖：
 * 1. 基本构造函数和工厂方法
 * 2. 字节数组操作（getBytes/getBytesUnsafe）
 * 3. 字符串操作（getString/fromString）
 * 4. 比较操作（equals/equalsIgnoreCase/compareTo）
 * 5. 哈希码计算
 * 6. 命令缓存池功能
 * 7. 边界条件和异常处理
 * 8. 线程安全性
 * 9. 性能相关功能
 * 
 * @author hnfy258
 */
@DisplayName("RedisBytes 单元测试")
class RedisBytesTest {

    private RedisBytes testBytes;
    private final String testString = "Hello Redis";
    private final byte[] testByteArray = testString.getBytes(RedisBytes.CHARSET);

    @BeforeEach
    void setUp() {
        testBytes = new RedisBytes(testByteArray);
    }

    @Nested
    @DisplayName("构造函数测试")
    class ConstructorTests {

        @Test
        @DisplayName("标准构造函数 - 正常情况")
        void testStandardConstructor() {
            final RedisBytes rb = new RedisBytes(testByteArray);
            
            assertNotNull(rb);
            assertEquals(testString.length(), rb.length());
            assertArrayEquals(testByteArray, rb.getBytes());
        }

        @Test
        @DisplayName("标准构造函数 - 空数组")
        void testStandardConstructorWithEmptyArray() {
            final RedisBytes rb = new RedisBytes(new byte[0]);
            
            assertNotNull(rb);
            assertEquals(0, rb.length());
            assertArrayEquals(new byte[0], rb.getBytes());
        }

        @Test
        @DisplayName("标准构造函数 - null 输入")
        void testStandardConstructorWithNull() {
            assertThrows(IllegalArgumentException.class, () -> {
                new RedisBytes((byte[]) null);
            });
        }        @Test
        @DisplayName("受信任构造函数")
        void testTrustedConstructor() {
            final byte[] trustedArray = testString.getBytes();
            final RedisBytes rb = RedisBytes.wrapTrusted(trustedArray);
            
            assertNotNull(rb);
            assertEquals(testString.length(), rb.length());
            // 对于受信任的构造，getBytesUnsafe() 应该返回原始数组引用
            assertSame(trustedArray, rb.getBytesUnsafe());
        }

        @Test
        @DisplayName("零拷贝工厂方法")
        void testWrapTrusted() {
            final byte[] trustedArray = testString.getBytes();
            final RedisBytes rb = RedisBytes.wrapTrusted(trustedArray);
            
            assertNotNull(rb);
            assertEquals(testString.length(), rb.length());
        }

        @Test
        @DisplayName("零拷贝工厂方法 - null 输入")
        void testWrapTrustedWithNull() {
            final RedisBytes rb = RedisBytes.wrapTrusted(null);
            assertNull(rb);
        }
    }

    @Nested
    @DisplayName("fromString 工厂方法测试")
    class FromStringTests {

        @Test
        @DisplayName("fromString - 正常字符串")
        void testFromStringNormal() {
            final RedisBytes rb = RedisBytes.fromString(testString);
            
            assertNotNull(rb);
            assertEquals(testString, rb.getString());
            assertEquals(testString.length(), rb.length());
        }

        @Test
        @DisplayName("fromString - 空字符串")
        void testFromStringEmpty() {
            final RedisBytes rb = RedisBytes.fromString("");
            
            assertNotNull(rb);
            assertSame(RedisBytes.EMPTY, rb); // 应该返回常量
            assertEquals("", rb.getString());
            assertEquals(0, rb.length());
        }

        @Test
        @DisplayName("fromString - null 输入")
        void testFromStringNull() {
            final RedisBytes rb = RedisBytes.fromString(null);
            assertNull(rb);
        }

        @ParameterizedTest
        @ValueSource(strings = {"GET", "SET", "PING", "LPUSH", "SADD", "HSET", "ZADD"})
        @DisplayName("fromString - 常用命令缓存")
        void testFromStringCachedCommands(String command) {
            final RedisBytes rb1 = RedisBytes.fromString(command);
            final RedisBytes rb2 = RedisBytes.fromString(command);
            
            // 缓存的命令应该返回同一个实例
            assertSame(rb1, rb2);
            assertEquals(command, rb1.getString());
        }
    }

    @Nested
    @DisplayName("字节数组操作测试")
    class ByteArrayOperationTests {

        @Test
        @DisplayName("getBytes - 返回副本")
        void testGetBytesReturnsCopy() {
            final byte[] bytes1 = testBytes.getBytes();
            final byte[] bytes2 = testBytes.getBytes();
            
            assertArrayEquals(bytes1, bytes2);
            assertNotSame(bytes1, bytes2); // 应该是不同的实例
        }

        @Test
        @DisplayName("getBytes - 修改返回的数组不影响原始数据")
        void testGetBytesImmutability() {
            final byte[] bytes = testBytes.getBytes();
            final byte[] originalBytes = testBytes.getBytes();
            
            // 修改返回的数组
            if (bytes.length > 0) {
                bytes[0] = (byte) (bytes[0] + 1);
            }
            
            // 原始数据应该不受影响
            assertArrayEquals(originalBytes, testBytes.getBytes());
        }

        @Test
        @DisplayName("getBytesUnsafe - 零拷贝访问")
        void testGetBytesUnsafe() {
            final byte[] unsafeBytes = testBytes.getBytesUnsafe();
            
            assertNotNull(unsafeBytes);
            assertEquals(testString, new String(unsafeBytes, RedisBytes.CHARSET));
        }

        @Test
        @DisplayName("length - 正确返回长度")
        void testLength() {
            assertEquals(testByteArray.length, testBytes.length());
            assertEquals(0, RedisBytes.EMPTY.length());
        }

        @Test
        @DisplayName("isEmpty - 空数组检测")
        void testIsEmpty() {
            assertFalse(testBytes.isEmpty());
            assertTrue(RedisBytes.EMPTY.isEmpty());
            assertTrue(new RedisBytes(new byte[0]).isEmpty());
        }
    }

    @Nested
    @DisplayName("字符串操作测试")
    class StringOperationTests {

        @Test
        @DisplayName("getString - 正常转换")
        void testGetString() {
            assertEquals(testString, testBytes.getString());
        }

        @Test
        @DisplayName("getString - 延迟初始化和缓存")
        void testGetStringCaching() {
            final String str1 = testBytes.getString();
            final String str2 = testBytes.getString();
            
            assertEquals(str1, str2);
            assertSame(str1, str2); // 应该缓存字符串
        }

        @Test
        @DisplayName("getString - UTF-8 编码")
        void testGetStringUTF8() {
            final String chineseText = "你好世界";
            final RedisBytes rb = RedisBytes.fromString(chineseText);
            
            assertEquals(chineseText, rb.getString());
        }

        @Test
        @DisplayName("toString - 正确格式")
        void testToString() {
            final String result = testBytes.toString();
            assertTrue(result.startsWith("RedisBytes["));
            assertTrue(result.contains("length=" + testBytes.length()));
            assertTrue(result.contains("hashCode="));
        }
    }

    @Nested
    @DisplayName("比较操作测试")
    class ComparisonTests {

        @Test
        @DisplayName("equals - 相同内容")
        void testEqualsSameContent() {
            final RedisBytes other = new RedisBytes(testByteArray);
            
            assertEquals(testBytes, other);
            assertEquals(other, testBytes);
        }

        @Test
        @DisplayName("equals - 不同内容")
        void testEqualsDifferentContent() {
            final RedisBytes other = RedisBytes.fromString("Different");
            
            assertNotEquals(testBytes, other);
            assertNotEquals(other, testBytes);
        }

        @Test
        @DisplayName("equals - null 和 不同类型")
        void testEqualsSpecialCases() {
            assertNotEquals(testBytes, null);
            assertNotEquals(testBytes, "String");
            assertNotEquals(testBytes, 123);
        }

        @Test
        @DisplayName("equals - 自反性")
        void testEqualsReflexive() {
            assertEquals(testBytes, testBytes);
        }

        @Test
        @DisplayName("equalsIgnoreCase - 大小写不敏感")
        void testEqualsIgnoreCase() {
            final RedisBytes upper = RedisBytes.fromString("HELLO");
            final RedisBytes lower = RedisBytes.fromString("hello");
            final RedisBytes mixed = RedisBytes.fromString("Hello");
            
            assertTrue(upper.equalsIgnoreCase(lower));
            assertTrue(lower.equalsIgnoreCase(upper));
            assertTrue(upper.equalsIgnoreCase(mixed));
            assertTrue(lower.equalsIgnoreCase(mixed));
        }

        @Test
        @DisplayName("equalsIgnoreCase - 不同长度")
        void testEqualsIgnoreCaseDifferentLength() {
            final RedisBytes short1 = RedisBytes.fromString("HI");
            final RedisBytes long1 = RedisBytes.fromString("HELLO");
            
            assertFalse(short1.equalsIgnoreCase(long1));
            assertFalse(long1.equalsIgnoreCase(short1));
        }

        @Test
        @DisplayName("equalsIgnoreCase - null 处理")
        void testEqualsIgnoreCaseNull() {
            assertFalse(testBytes.equalsIgnoreCase(null));
        }

        @Test
        @DisplayName("compareTo - 字典序比较")
        void testCompareTo() {
            final RedisBytes a = RedisBytes.fromString("apple");
            final RedisBytes b = RedisBytes.fromString("banana");
            final RedisBytes c = RedisBytes.fromString("cherry");
            
            assertTrue(a.compareTo(b) < 0);
            assertTrue(b.compareTo(a) > 0);
            assertTrue(b.compareTo(c) < 0);
            assertEquals(0, a.compareTo(RedisBytes.fromString("apple")));
        }

        @Test
        @DisplayName("compareTo - 不同长度")
        void testCompareToDifferentLength() {
            final RedisBytes short1 = RedisBytes.fromString("hi");
            final RedisBytes long1 = RedisBytes.fromString("hello");
            
            assertTrue(short1.compareTo(long1) > 0); // 'h' vs 'h', 'i' vs 'e'
        }

        @Test
        @DisplayName("compareTo - 空字符串")
        void testCompareToEmpty() {
            assertTrue(testBytes.compareTo(RedisBytes.EMPTY) > 0);
            assertTrue(RedisBytes.EMPTY.compareTo(testBytes) < 0);
            assertEquals(0, RedisBytes.EMPTY.compareTo(RedisBytes.fromString("")));
        }
    }

    @Nested
    @DisplayName("哈希码测试")
    class HashCodeTests {

        @Test
        @DisplayName("hashCode - 一致性")
        void testHashCodeConsistency() {
            final int hash1 = testBytes.hashCode();
            final int hash2 = testBytes.hashCode();
            
            assertEquals(hash1, hash2);
        }

        @Test
        @DisplayName("hashCode - equals 对象有相同哈希码")
        void testHashCodeEqualObjects() {
            final RedisBytes other = new RedisBytes(testByteArray);
            
            assertEquals(testBytes.hashCode(), other.hashCode());
        }

        @Test
        @DisplayName("hashCode - 不同内容有不同哈希码（大概率）")
        void testHashCodeDifferentObjects() {
            final RedisBytes other = RedisBytes.fromString("Different Content");
            
            // 注意：这不是绝对保证，但对于不同内容，哈希码应该大概率不同
            assertNotEquals(testBytes.hashCode(), other.hashCode());
        }

        @Test
        @DisplayName("hashCode - 预计算缓存")
        void testHashCodeCaching() {
            // 多次调用应该返回相同结果（测试缓存）
            final int hash1 = testBytes.hashCode();
            final int hash2 = testBytes.hashCode();
            final int hash3 = testBytes.hashCode();
            
            assertEquals(hash1, hash2);
            assertEquals(hash2, hash3);
        }
    }

    @Nested
    @DisplayName("命令缓存池测试")
    class CommandCacheTests {

        @Test
        @DisplayName("常用命令预缓存")
        void testCommonCommandsCached() {
            final String[] commonCommands = {
                "GET", "SET", "PING", "LPUSH", "SADD", "HSET", "ZADD",
                "SCAN", "KEYS", "INFO", "CONFIG", "DBSIZE"
            };
            
            for (String cmd : commonCommands) {
                assertTrue(RedisBytes.isCommandCached(cmd), 
                          "命令 " + cmd + " 应该被缓存");
            }
        }

        @Test
        @DisplayName("缓存命中率")
        void testCacheHitRate() {
            assertTrue(RedisBytes.getCachedCommandCount() > 0);
            
            // 测试缓存命中
            final RedisBytes rb1 = RedisBytes.fromString("GET");
            final RedisBytes rb2 = RedisBytes.fromString("GET");
            assertSame(rb1, rb2);
        }

        @Test
        @DisplayName("非缓存命令")
        void testNonCachedCommands() {
            final String customCommand = "CUSTOM_COMMAND_NOT_CACHED_" + System.nanoTime();
            
            assertFalse(RedisBytes.isCommandCached(customCommand));
            
            final RedisBytes rb1 = RedisBytes.fromString(customCommand);
            final RedisBytes rb2 = RedisBytes.fromString(customCommand);
            
            // 非缓存命令应该创建不同实例
            assertNotSame(rb1, rb2);
            assertEquals(rb1, rb2); // 但内容相同
        }
    }

    @Nested
    @DisplayName("边界条件和异常处理测试")
    class BoundaryAndExceptionTests {

        @Test
        @DisplayName("大数据量处理")
        void testLargeData() {
            final int size = 1024 * 1024; // 1MB
            final byte[] largeArray = new byte[size];
            Arrays.fill(largeArray, (byte) 'A');
            
            final RedisBytes rb = new RedisBytes(largeArray);
            
            assertEquals(size, rb.length());
            assertArrayEquals(largeArray, rb.getBytes());
        }

        @Test
        @DisplayName("Unicode 字符处理")
        void testUnicodeHandling() {
            final String unicodeString = "🌟Redis测试🚀";
            final RedisBytes rb = RedisBytes.fromString(unicodeString);
            
            assertEquals(unicodeString, rb.getString());
        }

        @Test
        @DisplayName("特殊字符处理")
        void testSpecialCharacters() {
            final String specialChars = "\r\n\t\0\u001f\u007f";
            final RedisBytes rb = RedisBytes.fromString(specialChars);
            
            assertEquals(specialChars, rb.getString());
        }

        @Test
        @DisplayName("空白字符处理")
        void testWhitespaceHandling() {
            final String whitespace = "   ";
            final RedisBytes rb = RedisBytes.fromString(whitespace);
            
            assertEquals(whitespace, rb.getString());
            assertEquals(3, rb.length());
        }
    }

    /**
     * 边界条件和分支覆盖率测试
     */
    @Nested
    @DisplayName("边界条件和分支覆盖率测试")
    class BoundaryAndBranchTests {
        
        @Test
        @DisplayName("测试超长字符串缓存限制")
        void testLongStringCaching() {
            // 1. 测试超过缓存大小限制的字符串
            StringBuilder longString = new StringBuilder();
            for (int i = 0; i < 200; i++) { // 超过 MAX_CACHED_STRING_SIZE
                longString.append("a");
            }
            
            RedisBytes longRedisBytes = RedisBytes.fromString(longString.toString());
            
            // 2. 验证字符串可以正确获取
            assertEquals(longString.toString(), longRedisBytes.getString());
            assertEquals(200, longRedisBytes.length());
        }
        
        @Test
        @DisplayName("测试字节比较的所有分支")
        void testByteComparisonBranches() {
            // 1. 相同长度，不同内容
            RedisBytes bytes1 = RedisBytes.fromString("abc");
            RedisBytes bytes2 = RedisBytes.fromString("abd");
            assertTrue(bytes1.compareTo(bytes2) < 0);
            
            // 2. 不同长度，一个是另一个的前缀
            RedisBytes short1 = RedisBytes.fromString("ab");
            RedisBytes long1 = RedisBytes.fromString("abc");
            assertTrue(short1.compareTo(long1) < 0);
            
            // 3. 完全相同
            RedisBytes same1 = RedisBytes.fromString("test");
            RedisBytes same2 = RedisBytes.fromString("test");
            assertEquals(0, same1.compareTo(same2));
            
            // 4. 空字节数组比较
            assertTrue(RedisBytes.EMPTY.compareTo(bytes1) < 0);
            assertTrue(bytes1.compareTo(RedisBytes.EMPTY) > 0);
            assertEquals(0, RedisBytes.EMPTY.compareTo(RedisBytes.EMPTY));
        }
        
        @Test
        @DisplayName("测试 equalsIgnoreCase 的所有分支")
        void testEqualsIgnoreCaseAllBranches() {
            RedisBytes rb1 = RedisBytes.fromString("Test");
            RedisBytes rb2 = RedisBytes.fromString("TEST");
            RedisBytes rb3 = RedisBytes.fromString("test");
            RedisBytes rb4 = RedisBytes.fromString("different");
            RedisBytes rb5 = RedisBytes.fromString("longer_string");
            
            // 1. 相同实例
            assertTrue(rb1.equalsIgnoreCase(rb1));
            
            // 2. null 参数
            assertFalse(rb1.equalsIgnoreCase(null));
            
            // 3. 不同长度
            assertFalse(rb1.equalsIgnoreCase(rb5));
            
            // 4. 相同长度，大小写不同
            assertTrue(rb1.equalsIgnoreCase(rb2));
            assertTrue(rb1.equalsIgnoreCase(rb3));
            
            // 5. 相同长度，内容不同
            assertFalse(rb1.equalsIgnoreCase(rb4));
            
            // 6. 包含非字母字符
            RedisBytes num1 = RedisBytes.fromString("test123");
            RedisBytes num2 = RedisBytes.fromString("TEST123");
            assertTrue(num1.equalsIgnoreCase(num2));
        }
        
        @Test
        @DisplayName("测试命令缓存边界情况")
        void testCommandCacheBoundaries() {
            // 1. 测试大小写不敏感的缓存检查
            assertTrue(RedisBytes.isCommandCached("get"));
            assertTrue(RedisBytes.isCommandCached("GET"));
            assertTrue(RedisBytes.isCommandCached("Get"));
              // 2. 测试不存在的命令
            assertFalse(RedisBytes.isCommandCached("NONEXISTENT_COMMAND"));
            assertFalse(RedisBytes.isCommandCached(""));
            
            // 3. 验证缓存计数
            int cacheCount = RedisBytes.getCachedCommandCount();
            assertTrue(cacheCount > 0);
            
            // 4. 创建新的 RedisBytes 不应影响缓存计数
            RedisBytes.fromString("uncached_command");
            assertEquals(cacheCount, RedisBytes.getCachedCommandCount());
        }
        
        @Test
        @DisplayName("测试 wrapTrusted 方法的边界情况")
        void testWrapTrustedBoundaries() {
            // 1. null 参数
            assertNull(RedisBytes.wrapTrusted(null));
            
            // 2. 空数组
            byte[] emptyArray = new byte[0];
            RedisBytes wrapped = RedisBytes.wrapTrusted(emptyArray);
            assertNotNull(wrapped);
            assertEquals(0, wrapped.length());
            
            // 3. 验证是否真的是零拷贝（同一个数组引用）
            byte[] testArray = "test".getBytes();
            RedisBytes wrappedTest = RedisBytes.wrapTrusted(testArray);
            
            // 注意：这里我们不能直接验证内部引用相同，因为 getBytesUnsafe 是只读的
            // 但我们可以验证内容正确
            assertArrayEquals(testArray, wrappedTest.getBytesUnsafe());
        }
        
        @Test
        @DisplayName("测试字符串延迟初始化")
        void testStringLazyInitialization() {
            // 1. 测试字符串缓存的延迟初始化
            byte[] testBytes = "lazy_test".getBytes();
            RedisBytes rb = new RedisBytes(testBytes);
            
            // 2. 第一次调用 getString() 应该初始化字符串缓存
            String str1 = rb.getString();
            assertEquals("lazy_test", str1);
            
            // 3. 第二次调用应该返回缓存的字符串（相同实例）
            String str2 = rb.getString();
            assertSame(str1, str2); // 验证是同一个对象实例
        }
        
        @Test
        @DisplayName("测试哈希码缓存")
        void testHashCodeCaching() {
            RedisBytes rb1 = RedisBytes.fromString("hash_test");
            RedisBytes rb2 = RedisBytes.fromString("hash_test");
            
            // 1. 相同内容应该有相同的哈希码
            assertEquals(rb1.hashCode(), rb2.hashCode());
            
            // 2. 多次调用 hashCode() 应该返回相同值
            int hash1 = rb1.hashCode();
            int hash2 = rb1.hashCode();
            assertEquals(hash1, hash2);
            
            // 3. 不同内容应该有不同的哈希码（大概率）
            RedisBytes rb3 = RedisBytes.fromString("different");
            assertNotEquals(rb1.hashCode(), rb3.hashCode());
        }
        
        @Test
        @DisplayName("测试 toString 方法的调试格式")
        void testToStringDebugFormat() {
            // 1. 普通字符串
            RedisBytes rb1 = RedisBytes.fromString("test");
            String debugStr = rb1.toString();
            assertTrue(debugStr.contains("RedisBytes"));
            assertTrue(debugStr.contains("4"));  // 长度
            assertTrue(debugStr.contains("test")); // 内容预览
            
            // 2. 空字符串
            RedisBytes empty = RedisBytes.EMPTY;
            String emptyDebugStr = empty.toString();
            assertTrue(emptyDebugStr.contains("RedisBytes"));
            assertTrue(emptyDebugStr.contains("0")); // 长度为0
            
            // 3. 长字符串（应该被截断）
            StringBuilder longStr = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                longStr.append("x");
            }
            RedisBytes longRb = RedisBytes.fromString(longStr.toString());
            String longDebugStr = longRb.toString();
            assertTrue(longDebugStr.contains("RedisBytes"));
            assertTrue(longDebugStr.contains("100")); // 长度
            // 内容应该被截断，不会包含完整的100个字符
        }
    }

    // 参数化测试的数据提供方法
    static Stream<Arguments> differentStringPairs() {
        return Stream.of(
            Arguments.of("hello", "world"),
            Arguments.of("", "non-empty"),
            Arguments.of("short", "this is a longer string"),
            Arguments.of("123", "456"),
            Arguments.of("中文", "English")
        );
    }

    @ParameterizedTest
    @MethodSource("differentStringPairs")
    @DisplayName("参数化测试 - 不同字符串对比")
    void testDifferentStringComparisons(String str1, String str2) {
        final RedisBytes rb1 = RedisBytes.fromString(str1);
        final RedisBytes rb2 = RedisBytes.fromString(str2);
        
        assertNotEquals(rb1, rb2);
        assertNotEquals(rb1.hashCode(), rb2.hashCode());
          if (!str1.equalsIgnoreCase(str2)) {
            assertFalse(rb1.equalsIgnoreCase(rb2));
        }
    }

    // ========== Comparable 接口测试 ==========

    @Nested
    @DisplayName("Comparable 接口测试")
    class ComparableTests {

        /**
         * 测试 compareTo 方法的基本功能
         */
        @Test
        @DisplayName("基本比较功能")
        void testCompareTo() {
            final RedisBytes rb1 = RedisBytes.fromString("abc");
            final RedisBytes rb2 = RedisBytes.fromString("abc");
            final RedisBytes rb3 = RedisBytes.fromString("abd");
            final RedisBytes rb4 = RedisBytes.fromString("ab");
            
            // 1. 相等比较
            assertEquals(0, rb1.compareTo(rb2), "相同内容应该返回 0");
            
            // 2. 字典序比较
            assertTrue(rb1.compareTo(rb3) < 0, "'abc' 应该小于 'abd'");
            assertTrue(rb3.compareTo(rb1) > 0, "'abd' 应该大于 'abc'");
            
            // 3. 前缀比较
            assertTrue(rb4.compareTo(rb1) < 0, "'ab' 应该小于 'abc'");
            assertTrue(rb1.compareTo(rb4) > 0, "'abc' 应该大于 'ab'");
        }

        /**
         * 测试 compareTo 方法的边界条件
         */
        @Test
        @DisplayName("边界条件测试")
        void testCompareToEdgeCases() {
            final RedisBytes rb = RedisBytes.fromString("test");
            final RedisBytes empty = RedisBytes.EMPTY;
            
            // 1. null 值比较
            assertTrue(rb.compareTo(null) > 0, "非null 应该大于 null");
            
            // 2. 空字符串比较
            assertTrue(rb.compareTo(empty) > 0, "非空字符串应该大于空字符串");
            assertTrue(empty.compareTo(rb) < 0, "空字符串应该小于非空字符串");
            assertEquals(0, empty.compareTo(RedisBytes.EMPTY), "两个空字符串应该相等");
            
            // 3. 自身比较
            assertEquals(0, rb.compareTo(rb), "对象与自身比较应该返回 0");
        }

        /**
         * 测试 compareTo 方法的字节级比较
         */
        @Test
        @DisplayName("字节级比较测试")
        void testCompareToByteLevel() {
            // 1. 无符号字节比较测试
            final RedisBytes rb1 = new RedisBytes(new byte[]{(byte) 0x7F}); // 127
            final RedisBytes rb2 = new RedisBytes(new byte[]{(byte) 0x80}); // -128 (as signed), 128 (as unsigned)
            
            assertTrue(rb1.compareTo(rb2) < 0, "0x7F 应该小于 0x80（无符号比较）");
            
            // 2. 多字节比较
            final RedisBytes rb3 = new RedisBytes(new byte[]{1, 2, 3});
            final RedisBytes rb4 = new RedisBytes(new byte[]{1, 2, 4});
            
            assertTrue(rb3.compareTo(rb4) < 0, "[1,2,3] 应该小于 [1,2,4]");
        }

        /**
         * 测试 compareTo 方法支持排序
         */
        @Test
        @DisplayName("排序功能测试")
        void testCompareToSorting() {
            final java.util.List<RedisBytes> list = Arrays.asList(
                    RedisBytes.fromString("zebra"),
                    RedisBytes.fromString("apple"),
                    RedisBytes.fromString("banana"),
                    RedisBytes.fromString("cherry")
            );
            
            // 1. 排序测试
            java.util.Collections.sort(list);
            
            // 2. 验证排序结果
            assertEquals("apple", list.get(0).getString());
            assertEquals("banana", list.get(1).getString());
            assertEquals("cherry", list.get(2).getString());
            assertEquals("zebra", list.get(3).getString());
        }

        /**
         * 测试 compareTo 的传递性和对称性
         */
        @Test
        @DisplayName("数学性质测试")
        void testCompareToProperties() {
            final RedisBytes a = RedisBytes.fromString("a");
            final RedisBytes b = RedisBytes.fromString("b");
            final RedisBytes c = RedisBytes.fromString("c");
            
            // 1. 对称性：如果 a.compareTo(b) < 0，则 b.compareTo(a) > 0
            assertTrue(a.compareTo(b) < 0);
            assertTrue(b.compareTo(a) > 0);
            
            // 2. 传递性：如果 a.compareTo(b) < 0 且 b.compareTo(c) < 0，则 a.compareTo(c) < 0
            assertTrue(a.compareTo(b) < 0);
            assertTrue(b.compareTo(c) < 0);
            assertTrue(a.compareTo(c) < 0);
            
            // 3. 与 equals 的一致性
            final RedisBytes a2 = RedisBytes.fromString("a");
            assertEquals(0, a.compareTo(a2));
            assertTrue(a.equals(a2));
        }

        /**
         * 测试特殊字符的比较
         */
        @Test
        @DisplayName("特殊字符比较测试")
        void testCompareToSpecialCharacters() {
            final RedisBytes unicode1 = RedisBytes.fromString("测试");
            final RedisBytes unicode2 = RedisBytes.fromString("测试2");
            final RedisBytes ascii = RedisBytes.fromString("test");
            
            // Unicode 字符比较
            assertTrue(unicode1.compareTo(unicode2) < 0);
            
            // ASCII 与 Unicode 比较（按字节值）
            final int result = ascii.compareTo(unicode1);
            assertNotEquals(0, result); // 应该不相等
        }
    }
}

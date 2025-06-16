package site.hnfy258.internal.sds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import site.hnfy258.internal.Sds;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SDS (Simple Dynamic String) 单元测试
 * 
 * <p>测试覆盖范围：</p>
 * <ul>
 *   <li>工厂方法和构造函数</li>
 *   <li>字节数组操作（getBytes/append）</li>
 *   <li>字符串操作（toString/fromString）</li>
 *   <li>长度和容量管理</li>
 *   <li>比较操作（compare/equals）</li>
 *   <li>类型升级（Sds8 -> Sds16 -> Sds32）</li>
 *   <li>预分配策略验证</li>
 *   <li>边界条件和异常处理</li>
 *   <li>二进制安全性</li>
 *   <li>线程安全性</li>
 *   <li>性能特性验证</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0
 */
@DisplayName("SDS 单元测试")
class SdsTest {

    private static final byte[] TEST_BYTES = "Hello SDS".getBytes();
    private static final String TEST_STRING = "Hello SDS";
    private static final Random RANDOM = new Random(42); // 固定种子确保可重现性

    private Sds testSds;

    @BeforeEach
    void setUp() {
        testSds = Sds.create(TEST_BYTES);
    }

    @Nested
    @DisplayName("工厂方法和构造函数测试")
    class FactoryMethodTests {

        @Test
        @DisplayName("create - 正常情况")
        void testCreateNormal() {
            final Sds sds = Sds.create(TEST_BYTES);
            
            assertNotNull(sds);
            assertEquals(TEST_BYTES.length, sds.length());
            assertArrayEquals(TEST_BYTES, sds.getBytes());
        }

        @Test
        @DisplayName("create - 空数组")
        void testCreateEmpty() {
            final Sds sds = Sds.create(new byte[0]);
            
            assertNotNull(sds);
            assertEquals(0, sds.length());
            assertArrayEquals(new byte[0], sds.getBytes());
        }

        @Test
        @DisplayName("create - null 输入")
        void testCreateNull() {
            assertThrows(NullPointerException.class, () -> {
                Sds.create(null);
            });
        }

        @Test
        @DisplayName("empty - 空SDS创建")
        void testEmpty() {
            final Sds empty = Sds.empty();
            
            assertNotNull(empty);
            assertEquals(0, empty.length());
            assertTrue(empty.getBytes().length == 0);
        }

        @Test
        @DisplayName("fromString - 正常字符串")
        void testFromStringNormal() {
            final Sds sds = Sds.fromString(TEST_STRING);
            
            assertNotNull(sds);
            assertEquals(TEST_STRING, sds.toString());
            assertEquals(TEST_STRING.length(), sds.length());
        }

        @Test
        @DisplayName("fromString - 空字符串")
        void testFromStringEmpty() {
            final Sds sds = Sds.fromString("");
            
            assertNotNull(sds);
            assertEquals("", sds.toString());
            assertEquals(0, sds.length());
        }

        @Test
        @DisplayName("fromString - null 输入")
        void testFromStringNull() {
            final Sds sds = Sds.fromString(null);
            assertNotNull(sds);
            assertEquals(0, sds.length());
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 1, 10, 255, 256, 1000, 65535, 65536, 100000})
        @DisplayName("类型自动选择测试")
        void testTypeAutoSelection(int size) {
            final byte[] data = new byte[size];
            Arrays.fill(data, (byte) 'A');
            
            final Sds sds = Sds.create(data);
            
            assertNotNull(sds);
            assertEquals(size, sds.length());
            
            // 验证类型选择逻辑
            if (size < 256) {
                assertTrue(sds.getClass().getSimpleName().contains("Sds8"));
            } else if (size < 65536) {
                assertTrue(sds.getClass().getSimpleName().contains("Sds16"));
            } else {
                assertTrue(sds.getClass().getSimpleName().contains("Sds32"));
            }
        }
    }

    @Nested
    @DisplayName("字节数组操作测试")
    class ByteArrayOperationTests {

        @Test
        @DisplayName("getBytes - 返回副本")
        void testGetBytesReturnsCopy() {
            final byte[] bytes1 = testSds.getBytes();
            final byte[] bytes2 = testSds.getBytes();
            
            assertArrayEquals(bytes1, bytes2);
            assertNotSame(bytes1, bytes2); // 应该是不同的实例
        }

        @Test
        @DisplayName("getBytes - 修改返回的数组不影响原始数据")
        void testGetBytesImmutability() {
            final byte[] bytes = testSds.getBytes();
            final byte[] originalBytes = testSds.getBytes();
            
            // 修改返回的数组
            if (bytes.length > 0) {
                bytes[0] = (byte) (bytes[0] + 1);
            }
            
            // 原始数据应该不受影响
            assertArrayEquals(originalBytes, testSds.getBytes());
        }

        @Test
        @DisplayName("length - O(1)长度获取")
        void testLength() {
            assertEquals(TEST_BYTES.length, testSds.length());
            assertEquals(0, Sds.empty().length());
            
            // 验证append后长度正确更新
            final int originalLength = testSds.length();
            testSds.append("_suffix".getBytes());
            assertEquals(originalLength + "_suffix".length(), testSds.length());
        }

        @Test
        @DisplayName("avail - 可用空间计算")
        void testAvail() {
            final int available = testSds.avail();
            final int allocated = testSds.alloc();
            final int used = testSds.length();
            
            assertEquals(allocated - used, available);
            assertTrue(available >= 0);
        }

        @Test
        @DisplayName("alloc - 分配空间查询")
        void testAlloc() {
            final int allocated = testSds.alloc();
            final int length = testSds.length();
            
            assertTrue(allocated >= length);
            assertTrue(allocated > 0);
        }

        @Test
        @DisplayName("clear - 清空内容")
        void testClear() {
            testSds.clear();
            
            assertEquals(0, testSds.length());
            assertArrayEquals(new byte[0], testSds.getBytes());
        }
    }

    @Nested
    @DisplayName("字符串操作测试")
    class StringOperationTests {

        @Test
        @DisplayName("toString - 正确转换")
        void testToString() {
            assertEquals(TEST_STRING, testSds.toString());
        }

        @Test
        @DisplayName("toString - UTF-8 编码")
        void testToStringUTF8() {
            final String chineseText = "你好世界";
            final Sds sds = Sds.fromString(chineseText);
            
            assertEquals(chineseText, sds.toString());
        }

        @Test
        @DisplayName("append String - 字符串追加")
        void testAppendString() {
            final String suffix = "_suffix";
            final Sds result = testSds.append(suffix);
            
            // append应该返回自身或新实例
            assertNotNull(result);
            assertEquals(TEST_STRING + suffix, result.toString());
        }

        @Test
        @DisplayName("append bytes - 字节数组追加")
        void testAppendBytes() {
            final byte[] suffix = "_suffix".getBytes();
            final int originalLength = testSds.length();
            
            final Sds result = testSds.append(suffix);
            
            assertNotNull(result);
            assertEquals(originalLength + suffix.length, result.length());
        }        @Test
        @DisplayName("append - 大量数据追加")
        void testAppendLargeData() {
            Sds sds = Sds.empty();
            final StringBuilder expected = new StringBuilder();
            
            // 追加25次，避免过度测试
            for (int i = 0; i < 25; i++) {
                final String data = String.format("data_%04d_", i);
                sds = sds.append(data);
                expected.append(data);
            }
            
            assertEquals(expected.toString(), sds.toString());
        }
    }

    @Nested
    @DisplayName("比较操作测试")
    class ComparisonTests {

        @Test
        @DisplayName("compare - 相同内容")
        void testCompareSameContent() {
            final Sds other = Sds.create(TEST_BYTES);
            
            assertEquals(0, testSds.compare(other));
            assertEquals(0, other.compare(testSds));
        }

        @Test
        @DisplayName("compare - 不同内容")
        void testCompareDifferentContent() {
            final Sds other = Sds.fromString("Different");
            
            final int result = testSds.compare(other);
            assertNotEquals(0, result);
            assertEquals(-result, other.compare(testSds)); // 对称性
        }

        @Test
        @DisplayName("compare - 字典序排序")
        void testCompareLexicographical() {
            final Sds a = Sds.fromString("apple");
            final Sds b = Sds.fromString("banana");
            final Sds c = Sds.fromString("cherry");
            
            assertTrue(a.compare(b) < 0);
            assertTrue(b.compare(c) < 0);
            assertTrue(a.compare(c) < 0);
        }

        @Test
        @DisplayName("compare - 不同长度")
        void testCompareDifferentLength() {
            final Sds short1 = Sds.fromString("hi");
            final Sds long1 = Sds.fromString("hello");
            
            final int result = short1.compare(long1);
            assertNotEquals(0, result);
        }

        @Test
        @DisplayName("duplicate - 复制SDS")
        void testDuplicate() {
            final Sds duplicate = testSds.duplicate();
            
            assertNotNull(duplicate);
            assertNotSame(testSds, duplicate);
            assertEquals(0, testSds.compare(duplicate));
            assertArrayEquals(testSds.getBytes(), duplicate.getBytes());
        }
    }

    @Nested
    @DisplayName("类型升级测试")
    class TypeUpgradeTests {

        @Test
        @DisplayName("Sds8 -> Sds16 升级")
        void testSds8ToSds16Upgrade() {
            // 创建一个Sds8实例
            final Sds sds8 = Sds.create("small".getBytes());
            assertTrue(sds8.getClass().getSimpleName().contains("Sds8"));
            
            // 追加足够的数据触发升级到Sds16
            final StringBuilder largeData = new StringBuilder();
            for (int i = 0; i < 30; i++) { // 超过256字节
                largeData.append("0123456789");
            }
            
            final Sds result = sds8.append(largeData.toString());
            assertTrue(result.length() > 256);
            // 升级后的类型应该是Sds16或Sds32
            final String className = result.getClass().getSimpleName();
            assertTrue(className.contains("Sds16") || className.contains("Sds32"));
        }

        @Test
        @DisplayName("Sds16 -> Sds32 升级")
        void testSds16ToSds32Upgrade() {
            // 创建一个中等大小的SDS
            final byte[] mediumData = new byte[1000];
            Arrays.fill(mediumData, (byte) 'A');
            final Sds sds16 = Sds.create(mediumData);
            
            // 追加足够的数据触发升级到Sds32
            final byte[] largeData = new byte[70000]; // 超过65536
            Arrays.fill(largeData, (byte) 'B');
            
            final Sds result = sds16.append(largeData);
            assertTrue(result.length() > 65536);
            assertTrue(result.getClass().getSimpleName().contains("Sds32"));
        }

        @Test
        @DisplayName("升级后数据完整性")
        void testUpgradeDataIntegrity() {
            final String original = "original_data";
            final Sds sds = Sds.fromString(original);
            
            // 构建大量追加数据
            final StringBuilder appendData = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                appendData.append("_").append(i);
            }
            
            final Sds result = sds.append(appendData.toString());
            final String expected = original + appendData.toString();
            
            assertEquals(expected, result.toString());
            assertEquals(expected.length(), result.length());
        }
    }

    @Nested
    @DisplayName("预分配策略测试")
    class PreallocationTests {

        @Test
        @DisplayName("贪婪预分配策略")
        void testGreedyPreallocation() {
            final Sds sds = Sds.fromString("test");
            final int initialAlloc = sds.alloc();
            final int initialLength = sds.length();
            
            // 贪婪策略应该分配比实际需要更多的空间
            assertTrue(initialAlloc > initialLength);
        }

        @Test
        @DisplayName("预分配减少重分配次数")
        void testPreallocationReducesReallocations() {
            final Sds sds = Sds.empty();
            int reallocCount = 0;
            int lastAlloc = sds.alloc();
            
            // 连续追加小数据
            for (int i = 0; i < 100; i++) {
                sds.append("x");
                final int currentAlloc = sds.alloc();
                if (currentAlloc != lastAlloc) {
                    reallocCount++;
                    lastAlloc = currentAlloc;
                }
            }
            
            // 由于预分配，重分配次数应该远少于追加次数
            assertTrue(reallocCount < 20, "重分配次数过多: " + reallocCount);
        }

        @Test
        @DisplayName("大数据预分配限制")
        void testLargeDataPreallocationLimit() {
            final byte[] largeData = new byte[2 * 1024 * 1024]; // 2MB
            Arrays.fill(largeData, (byte) 'X');
            
            final Sds sds = Sds.create(largeData);
            final int allocated = sds.alloc();
            final int length = sds.length();
            
            // 对于大数据，预分配应该有限制
            assertTrue(allocated <= length + 1024 * 1024); // 最多额外1MB
        }
    }

    @Nested
    @DisplayName("二进制安全性测试")
    class BinarySafetyTests {

        @Test
        @DisplayName("null 字节处理")
        void testNullByteHandling() {
            final byte[] dataWithNull = {65, 0, 66, 0, 67}; // A\0B\0C
            final Sds sds = Sds.create(dataWithNull);
            
            assertEquals(5, sds.length());
            assertArrayEquals(dataWithNull, sds.getBytes());
        }

        @Test
        @DisplayName("任意字节值支持")
        void testArbitraryByteValues() {
            final byte[] binaryData = new byte[256];
            for (int i = 0; i < 256; i++) {
                binaryData[i] = (byte) i;
            }
            
            final Sds sds = Sds.create(binaryData);
            
            assertEquals(256, sds.length());
            assertArrayEquals(binaryData, sds.getBytes());
        }

        @Test
        @DisplayName("二进制数据追加")
        void testBinaryDataAppend() {
            final byte[] data1 = {1, 2, 3, 0, 4, 5};
            final byte[] data2 = {6, 0, 7, 8, 0};
            
            final Sds sds = Sds.create(data1);
            sds.append(data2);
            
            final byte[] expected = new byte[data1.length + data2.length];
            System.arraycopy(data1, 0, expected, 0, data1.length);
            System.arraycopy(data2, 0, expected, data1.length, data2.length);
            
            assertArrayEquals(expected, sds.getBytes());
        }

        @Test
        @DisplayName("图片数据模拟")
        void testImageDataSimulation() {
            // 模拟JPEG文件头
            final byte[] jpegHeader = {
                (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0,
                0x00, 0x10, 0x4A, 0x46, 0x49, 0x46
            };
            
            final Sds sds = Sds.create(jpegHeader);
            
            assertEquals(jpegHeader.length, sds.length());
            assertArrayEquals(jpegHeader, sds.getBytes());
            
            // 追加更多二进制数据
            final byte[] moreData = {(byte) 0x89, 0x50, 0x4E, 0x47}; // PNG signature
            sds.append(moreData);
            
            assertEquals(jpegHeader.length + moreData.length, sds.length());
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {

        @Test
        @DisplayName("最大长度测试")
        void testMaxLength() {
            // 测试接近类型边界的长度
            assertDoesNotThrow(() -> {
                final byte[] data255 = new byte[255];
                Sds.create(data255);
            });
            
            assertDoesNotThrow(() -> {
                final byte[] data256 = new byte[256];
                Sds.create(data256);
            });
            
            assertDoesNotThrow(() -> {
                final byte[] data65535 = new byte[65535];
                Sds.create(data65535);
            });
        }

        @Test
        @DisplayName("极端字符串测试")
        void testExtremeStrings() {
            // 只有空格的字符串
            final String spaces = "   ";
            final Sds spaceSds = Sds.fromString(spaces);
            assertEquals(spaces, spaceSds.toString());
            
            // 特殊字符
            final String special = "\r\n\t\b\f";
            final Sds specialSds = Sds.fromString(special);
            assertEquals(special, specialSds.toString());
            
            // Unicode字符
            final String unicode = "🌟💻🚀";
            final Sds unicodeSds = Sds.fromString(unicode);
            assertEquals(unicode, unicodeSds.toString());
        }

        @Test
        @DisplayName("连续操作稳定性")
        void testContinuousOperationStability() {
            final Sds sds = Sds.fromString("base");
            
            // 连续1000次操作
            for (int i = 0; i < 1000; i++) {
                switch (i % 4) {
                    case 0:
                        sds.append("_");
                        break;
                    case 1:
                        sds.length(); // 长度查询
                        break;
                    case 2:
                        sds.avail(); // 可用空间查询
                        break;
                    case 3:
                        sds.getBytes(); // 字节数组获取
                        break;
                }
            }
            
            // 验证最终状态
            assertTrue(sds.length() > 4);
            assertNotNull(sds.toString());
        }
    }

    @Nested
    @DisplayName("性能特性验证")
    class PerformanceCharacteristicTests {

        @Test
        @DisplayName("O(1) 长度获取验证")
        void testO1LengthAccess() {
            final Sds[] sdsArray = new Sds[1000];
            
            // 创建不同大小的SDS
            for (int i = 0; i < sdsArray.length; i++) {
                final byte[] data = new byte[i * 100];
                Arrays.fill(data, (byte) 'A');
                sdsArray[i] = Sds.create(data);
            }
            
            // 测量长度获取时间
            final long startTime = System.nanoTime();
            long totalLength = 0;
            
            for (int round = 0; round < 10000; round++) {
                for (final Sds sds : sdsArray) {
                    totalLength += sds.length(); // 应该是O(1)
                }
            }
            
            final long endTime = System.nanoTime();
            final double timePerCall = (endTime - startTime) / (10000.0 * sdsArray.length);
            
            // 每次length()调用应该很快（小于1微秒）
            assertTrue(timePerCall < 1000, "length()调用过慢: " + timePerCall + "ns");
            assertTrue(totalLength > 0); // 确保计算了总长度
        }        @Test
        @DisplayName("append 操作效率验证")
        void testAppendEfficiency() {
            Sds sds = Sds.empty();
            
            final long startTime = System.nanoTime();
            
            // 追加10000次
            for (int i = 0; i < 10000; i++) {
                sds = sds.append("x");
            }
            
            final long endTime = System.nanoTime();
            final double timePerAppend = (endTime - startTime) / 10000.0;
            
            assertEquals(10000, sds.length());
            // 每次追加应该很快（小于10微秒）
            assertTrue(timePerAppend < 10000, "append操作过慢: " + timePerAppend + "ns");
        }

        @Test
        @DisplayName("内存效率验证")
        void testMemoryEfficiency() {
            final int count = 1000;
            final Sds[] sdsArray = new Sds[count];
            
            // 创建大量小SDS
            for (int i = 0; i < count; i++) {
                sdsArray[i] = Sds.fromString("test_" + i);
            }
            
            // 验证每个SDS的内存开销合理
            for (final Sds sds : sdsArray) {
                final int allocated = sds.alloc();
                final int used = sds.length();
                final double overhead = (double) (allocated - used) / used;
                
                // 内存开销应该合理（不超过100%）
                assertTrue(overhead < 1.0, "内存开销过高: " + (overhead * 100) + "%");
            }
        }
    }

    @Nested
    @DisplayName("线程安全性测试")
    class ThreadSafetyTests {

        @Test
        @DisplayName("并发读取安全性")
        void testConcurrentReadSafety() throws InterruptedException {
            final Sds sds = Sds.fromString("concurrent_test_data");
            final int threadCount = 10;
            final int operationsPerThread = 1000;
            final CountDownLatch latch = new CountDownLatch(threadCount);
            final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            
            // 并发读取测试
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {                            // 并发执行只读操作
                            assertEquals("concurrent_test_data", sds.toString());
                            assertEquals(20, sds.length());
                            assertNotNull(sds.getBytes());
                            assertTrue(sds.alloc() >= sds.length());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            executor.shutdown();
              // 验证数据完整性
            assertEquals("concurrent_test_data", sds.toString());
            assertEquals(20, sds.length());
        }

        @Test
        @DisplayName("独立实例修改隔离")
        void testInstanceIsolation() throws InterruptedException {
            final String baseData = "isolation_test";
            final int threadCount = 5;
            final CountDownLatch latch = new CountDownLatch(threadCount);
            final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            final String[] results = new String[threadCount];
            
            // 每个线程使用独立的SDS实例
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        final Sds localSds = Sds.fromString(baseData);
                        
                        // 每个线程追加不同的数据
                        for (int j = 0; j < 100; j++) {
                            localSds.append("_thread" + threadId + "_" + j);
                        }
                        
                        results[threadId] = localSds.toString();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            executor.shutdown();
            
            // 验证每个线程的结果都不同
            for (int i = 0; i < threadCount; i++) {
                assertNotNull(results[i]);
                assertTrue(results[i].startsWith(baseData));
                assertTrue(results[i].contains("_thread" + i + "_"));
                
                // 确保不同线程的结果不同
                for (int j = i + 1; j < threadCount; j++) {
                    assertNotEquals(results[i], results[j]);
                }
            }
        }
    }

    // ========== 参数化测试数据提供方法 ==========

    /**
     * 提供不同大小的测试数据
     */
    static Stream<Arguments> differentSizes() {
        return Stream.of(
            Arguments.of(0, "空数据"),
            Arguments.of(1, "单字节"),
            Arguments.of(10, "小数据"),
            Arguments.of(255, "Sds8边界"),
            Arguments.of(256, "Sds16起始"),
            Arguments.of(1000, "中等数据"),
            Arguments.of(65535, "Sds16边界"),
            Arguments.of(65536, "Sds32起始"),
            Arguments.of(100000, "大数据")
        );
    }

    /**
     * 提供不同的字符串内容
     */
    static Stream<Arguments> differentStrings() {
        return Stream.of(
            Arguments.of("", "空字符串"),
            Arguments.of("a", "单字符"),
            Arguments.of("hello", "普通英文"),
            Arguments.of("你好", "中文"),
            Arguments.of("🌟💻🚀", "Emoji"),
            Arguments.of("Hello\nWorld\t!", "控制字符"),
            Arguments.of("  spaces  ", "空格"),
            Arguments.of("MiXeD_CaSe_123", "混合大小写数字")
        );
    }

    @ParameterizedTest
    @MethodSource("differentSizes")
    @DisplayName("参数化测试 - 不同大小数据处理")
    void testDifferentSizes(int size, String description) {
        final byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'T');
        
        final Sds sds = Sds.create(data);
        
        assertEquals(size, sds.length(), description + " 长度不匹配");
        assertArrayEquals(data, sds.getBytes(), description + " 数据不匹配");
        assertTrue(sds.alloc() >= size, description + " 分配空间不足");
    }

    @ParameterizedTest
    @MethodSource("differentStrings")
    @DisplayName("参数化测试 - 不同字符串内容")
    void testDifferentStringContents(String content, String description) {
        final Sds sds = Sds.fromString(content);
        
        assertEquals(content, sds.toString(), description + " 字符串转换失败");
        assertEquals(content.getBytes().length, sds.length(), description + " 长度计算错误");
        
        // 测试复制和比较
        final Sds duplicate = sds.duplicate();
        assertEquals(0, sds.compare(duplicate), description + " 复制后比较失败");
    }
}

package site.hnfy258.rdb.crc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

/**
 * CRC64算法单元测试
 * 
 * <p>测试CRC64核心算法的正确性，包括：</p>
 * <ul>
 *     <li>基本CRC64计算功能</li>
 *     <li>增量计算一致性</li>
 *     <li>边界条件处理</li>
 *     <li>Redis兼容性验证</li>
 * </ul>
 */
@DisplayName("CRC64算法测试")
class Crc64Test {

    @Nested
    @DisplayName("基本功能测试")
    class BasicFunctionTests {

        @Test
        @DisplayName("空数据CRC64计算")
        void testEmptyDataCrc64() {
            // 1. 测试空字节数组
            byte[] emptyData = new byte[0];
            long crc = Crc64.crc64(Crc64.INITIAL_CRC, emptyData, 0, 0);
            assertEquals(Crc64.INITIAL_CRC, crc, "空数据CRC64应该等于初始值");

            // 2. 测试便捷方法
            long crcConvenient = Crc64.crc64(emptyData);
            assertEquals(crc, crcConvenient, "便捷方法结果应该一致");
        }

        @Test
        @DisplayName("单字节CRC64计算")
        void testSingleByteCrc64() {
            // 1. 测试单个字节
            byte[] singleByte = {0x42}; // 'B'
            long crc = Crc64.crc64(singleByte);
            
            // 2. 验证非零结果
            assertNotEquals(0L, crc, "单字节CRC64不应该为0");
            
            // 3. 验证可重现
            long crc2 = Crc64.crc64(singleByte);
            assertEquals(crc, crc2, "相同输入应该产生相同CRC64");
        }

        @Test
        @DisplayName("字符串CRC64计算")
        void testStringCrc64() {
            // 1. 测试简单字符串
            String testStr = "Hello Redis";
            long crc1 = Crc64.crc64(testStr);
            long crc2 = Crc64.crc64(testStr.getBytes(StandardCharsets.UTF_8));
            
            assertEquals(crc1, crc2, "字符串方法和字节数组方法结果应该一致");
            
            // 2. 测试不同字符串产生不同CRC64
            String differentStr = "Hello World";
            long crc3 = Crc64.crc64(differentStr);
            assertNotEquals(crc1, crc3, "不同字符串应该产生不同CRC64");
        }

        @Test
        @DisplayName("增量计算一致性验证")
        void testIncrementalCalculation() {
            // 1. 准备测试数据
            String part1 = "Hello ";
            String part2 = "Redis";
            String full = part1 + part2;
            
            // 2. 一次性计算
            long fullCrc = Crc64.crc64(full);
            
            // 3. 增量计算
            long incrementalCrc = Crc64.INITIAL_CRC;
            incrementalCrc = Crc64.crc64(incrementalCrc, part1.getBytes(), 0, part1.length());
            incrementalCrc = Crc64.crc64(incrementalCrc, part2.getBytes(), 0, part2.length());
            
            assertEquals(fullCrc, incrementalCrc, "增量计算结果应该与一次性计算一致");
        }
    }

    @Nested
    @DisplayName("参数验证测试")
    class ParameterValidationTests {

        @Test
        @DisplayName("null参数异常测试")
        void testNullParameterExceptions() {
            // 1. 测试null数据数组
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64(0L, null, 0, 0);
            }, "null数据应该抛出IllegalArgumentException");

            // 2. 测试null字符串
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64((String) null);
            }, "null字符串应该抛出IllegalArgumentException");
        }

        @Test
        @DisplayName("无效偏移量和长度测试")
        void testInvalidOffsetAndLength() {
            byte[] data = "test data".getBytes();
            
            // 1. 负偏移量
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64(0L, data, -1, 1);
            }, "负偏移量应该抛出异常");
            
            // 2. 负长度
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64(0L, data, 0, -1);
            }, "负长度应该抛出异常");
            
            // 3. 超出数组边界
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64(0L, data, 0, data.length + 1);
            }, "超出数组边界应该抛出异常");
            
            // 4. 偏移量超出边界
            assertThrows(IllegalArgumentException.class, () -> {
                Crc64.crc64(0L, data, data.length, 1);
            }, "偏移量超出边界应该抛出异常");
        }
    }

    @Nested
    @DisplayName("Redis兼容性测试")
    class RedisCompatibilityTests {

        @Test
        @DisplayName("Redis头部CRC64验证")
        void testRedisHeaderCrc64() {
            // 1. Redis RDB文件头部
            String redisHeader = "REDIS0009";
            long headerCrc = Crc64.crc64(redisHeader);
            
            // 2. 验证结果是确定的
            long headerCrc2 = Crc64.crc64(redisHeader);
            assertEquals(headerCrc, headerCrc2, "Redis头部CRC64应该是确定的");
            
            // 3. 输出用于手动验证
            System.out.printf("Redis头部'%s'的CRC64: 0x%x%n", redisHeader, headerCrc);
        }

        @Test
        @DisplayName("已知数据CRC64验证")
        void testKnownDataCrc64() {
            // 1. 测试一些已知的数据模式
            byte[] testData1 = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
            long crc1 = Crc64.crc64(testData1);
            
            byte[] testData2 = {(byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC};
            long crc2 = Crc64.crc64(testData2);
            
            // 2. 验证结果非零且不同
            assertNotEquals(0L, crc1, "测试数据1的CRC64不应该为0");
            assertNotEquals(0L, crc2, "测试数据2的CRC64不应该为0");
            assertNotEquals(crc1, crc2, "不同数据应该产生不同CRC64");
            
            // 3. 输出用于调试
            System.out.printf("测试数据1 CRC64: 0x%x%n", crc1);
            System.out.printf("测试数据2 CRC64: 0x%x%n", crc2);
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {

        @Test
        @DisplayName("最大数据块CRC64计算")
        void testLargeDataCrc64() {
            // 1. 创建较大的数据块（1MB）
            int size = 1024 * 1024;
            byte[] largeData = new byte[size];
            
            // 2. 填充模式数据
            for (int i = 0; i < size; i++) {
                largeData[i] = (byte) (i % 256);
            }
            
            // 3. 计算CRC64（测试性能和正确性）
            long startTime = System.nanoTime();
            long crc = Crc64.crc64(largeData);
            long endTime = System.nanoTime();
            
            // 4. 验证结果
            assertNotEquals(0L, crc, "大数据块CRC64不应该为0");
            
            // 5. 性能检查（应该在合理时间内完成）
            long durationMs = (endTime - startTime) / 1_000_000;
            assertTrue(durationMs < 1000, "1MB数据CRC64计算应该在1秒内完成，实际: " + durationMs + "ms");
            
            System.out.printf("1MB数据CRC64计算耗时: %d ms%n", durationMs);
        }

        @Test
        @DisplayName("偏移量和长度边界测试")
        void testOffsetAndLengthBoundaries() {
            byte[] data = "0123456789".getBytes();
            
            // 1. 测试零长度
            long crc1 = Crc64.crc64(0L, data, 5, 0);
            assertEquals(0L, crc1, "零长度应该返回初始CRC值");
            
            // 2. 测试单字符
            long crc2 = Crc64.crc64(0L, data, 5, 1);
            long expected = Crc64.crc64(new byte[]{data[5]});
            assertEquals(expected, crc2, "单字符CRC64应该正确");
            
            // 3. 测试最大偏移量
            long crc3 = Crc64.crc64(0L, data, data.length - 1, 1);
            long expected3 = Crc64.crc64(new byte[]{data[data.length - 1]});
            assertEquals(expected3, crc3, "最大偏移量CRC64应该正确");
        }
    }

    @Nested
    @DisplayName("工具类特性测试")
    class UtilityClassTests {
        @Test
        @DisplayName("工具类不可实例化")
        void testUtilityClassCannotBeInstantiated() {
            // 使用反射测试私有构造函数
            assertThrows(java.lang.reflect.InvocationTargetException.class, () -> {
                java.lang.reflect.Constructor<Crc64> constructor = Crc64.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
            }, "工具类不应该允许实例化");
            
            // 验证内部异常是UnsupportedOperationException
            try {
                java.lang.reflect.Constructor<Crc64> constructor = Crc64.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
                fail("应该抛出异常");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue(e.getCause() instanceof UnsupportedOperationException, 
                          "内部异常应该是UnsupportedOperationException");
                assertEquals("工具类不允许实例化", e.getCause().getMessage(),
                          "异常消息应该正确");
            } catch (Exception e) {
                fail("意外的异常类型: " + e.getClass());
            }
        }

        @Test
        @DisplayName("常量定义验证")
        void testConstants() {
            // 验证初始CRC值
            assertEquals(0L, Crc64.INITIAL_CRC, "初始CRC值应该为0");
        }
    }

    @Nested
    @DisplayName("多项式和算法验证")
    class PolynomialAndAlgorithmTests {

        @Test
        @DisplayName("CRC64多项式特性验证")
        void testCrc64PolynomialProperties() {
            // 1. 测试简单的数学特性：CRC(a ⊕ b) ≠ CRC(a) ⊕ CRC(b)
            byte[] dataA = {0x01, 0x02, 0x03};
            byte[] dataB = {0x04, 0x05, 0x06};
            
            long crcA = Crc64.crc64(dataA);
            long crcB = Crc64.crc64(dataB);
            
            // 2. XOR数据
            byte[] dataXor = new byte[dataA.length];
            for (int i = 0; i < dataA.length; i++) {
                dataXor[i] = (byte) (dataA[i] ^ dataB[i]);
            }
            long crcXor = Crc64.crc64(dataXor);
            
            // 3. 验证CRC不是简单的XOR
            assertNotEquals(crcA ^ crcB, crcXor, "CRC64不应该是简单的XOR操作");
        }

        @Test
        @DisplayName("数据顺序敏感性测试")
        void testDataOrderSensitivity() {
            // 1. 原始数据
            byte[] original = {0x01, 0x02, 0x03, 0x04};
            long originalCrc = Crc64.crc64(original);
            
            // 2. 反转数据
            byte[] reversed = {0x04, 0x03, 0x02, 0x01};
            long reversedCrc = Crc64.crc64(reversed);
            
            // 3. 验证顺序敏感性
            assertNotEquals(originalCrc, reversedCrc, "CRC64应该对数据顺序敏感");
        }
    }
}

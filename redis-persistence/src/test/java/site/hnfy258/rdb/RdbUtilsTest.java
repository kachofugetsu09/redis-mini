package site.hnfy258.rdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import site.hnfy258.core.RedisCore;
import site.hnfy258.datastructure.*;
import site.hnfy258.internal.Sds;
import site.hnfy258.rdb.crc.Crc64OutputStream;
import site.hnfy258.rdb.crc.Crc64InputStream;

import java.io.*;

/**
 * RdbUtils单元测试
 * 
 * <p>测试RDB工具类的功能，包括：</p>
 * <ul>
 *     <li>RDB头部写入和验证</li>
 *     <li>RDB尾部写入</li>
 *     <li>CRC64校验</li>
 *     <li>数据类型读写</li>
 * </ul>
 */
@DisplayName("RdbUtils测试")
class RdbUtilsTest {

    @Mock
    private RedisCore mockRedisCore;
    
    private AutoCloseable mockCloser;

    @BeforeEach
    void setUp() {
        mockCloser = MockitoAnnotations.openMocks(this);
    }

    @Nested
    @DisplayName("RDB头部测试")
    class HeaderTests {

        @Test
        @DisplayName("RDB头部写入测试")
        void testWriteRdbHeader() throws IOException {
            // 1. 准备输出流
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // 2. 写入头部
            RdbUtils.writeRdbHeader(dos);
            dos.flush();
            
            // 3. 验证结果
            byte[] result = baos.toByteArray();
            String header = new String(result);
            assertEquals("REDIS0009", header, "RDB头部应该是REDIS0009");
            assertEquals(9, result.length, "RDB头部长度应该是9字节");
        }

        @Test
        @DisplayName("RDB头部验证测试（DataInputStream）")
        void testCheckRdbHeaderWithDataInputStream() throws IOException {
            // 1. 准备正确的头部数据
            ByteArrayInputStream bais = new ByteArrayInputStream("REDIS0009".getBytes());
            DataInputStream dis = new DataInputStream(bais);
            
            // 2. 验证头部
            boolean result = RdbUtils.checkRdbHeader(dis);
            
            // 3. 验证结果
            assertTrue(result, "正确的RDB头部验证应该成功");
        }

        @Test
        @DisplayName("RDB头部验证测试（CRC64InputStream）")
        void testCheckRdbHeaderWithCrc64InputStream() throws IOException {
            // 1. 准备正确的头部数据
            ByteArrayInputStream bais = new ByteArrayInputStream("REDIS0009".getBytes());
            Crc64InputStream cis = new Crc64InputStream(bais);
            
            // 2. 验证头部
            boolean result = RdbUtils.checkRdbHeader(cis);
            
            // 3. 验证结果
            assertTrue(result, "正确的RDB头部验证应该成功");
            
            // 4. 验证CRC64已更新
            assertNotEquals(0L, cis.getCrc64(), "读取头部后CRC64应该更新");
        }

        @Test
        @DisplayName("无效RDB头部验证测试")
        void testCheckInvalidRdbHeader() throws IOException {
            // 1. 准备错误的头部数据
            ByteArrayInputStream bais = new ByteArrayInputStream("INVALID12".getBytes());
            DataInputStream dis = new DataInputStream(bais);
            
            // 2. 验证头部
            boolean result = RdbUtils.checkRdbHeader(dis);
            
            // 3. 验证结果
            assertFalse(result, "错误的RDB头部验证应该失败");
        }

        @Test
        @DisplayName("不完整RDB头部验证测试")
        void testCheckIncompleteRdbHeader() throws IOException {
            // 1. 准备不完整的头部数据
            ByteArrayInputStream bais = new ByteArrayInputStream("REDIS".getBytes()); // 只有6字节
            DataInputStream dis = new DataInputStream(bais);
            
            // 2. 验证头部
            boolean result = RdbUtils.checkRdbHeader(dis);
            
            // 3. 验证结果
            assertFalse(result, "不完整的RDB头部验证应该失败");
        }
    }

    @Nested
    @DisplayName("RDB尾部测试")
    class FooterTests {        @Test
        @DisplayName("RDB尾部写入测试")
        void testWriteRdbFooter() throws IOException {
            // 1. 准备CRC64输出流
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Crc64OutputStream cos = new Crc64OutputStream(baos);
            
            // 2. 先写入一些数据以产生非零CRC64
            DataOutputStream dos = cos.getDataOutputStream();
            dos.writeBytes("test data");
            dos.flush();
            
            // 3. 写入尾部（包含EOF标记和校验和）
            RdbUtils.writeRdbFooter(cos);
            cos.flush();
            
            // 4. 验证结果
            byte[] result = baos.toByteArray();
            
            // 验证EOF标记
            assertEquals((byte)0xFF, result[9], "应该包含EOF标记"); // 第9个字节（"test data"后）
            
            // 5. 手动计算期望的CRC64（数据 + EOF标记）
            byte[] dataWithEof = new byte[10]; // "test data" + EOF
            System.arraycopy("test data".getBytes(), 0, dataWithEof, 0, 9);
            dataWithEof[9] = (byte) 0xFF;
            
            long expectedCrcWithEof = site.hnfy258.rdb.crc.Crc64.crc64(dataWithEof);
            
            // 验证CRC64包含了EOF标记但不包含校验和
            assertEquals(expectedCrcWithEof, cos.getCrc64(), "CRC64应该包含EOF标记但不包含校验和");
            
            // 验证总长度（数据 + EOF + CRC64）
            assertEquals(9 + 1 + 8, result.length, "总长度应该是数据+EOF+CRC64");
            
            // 验证校验和是否正确写入（读取文件末尾的8字节）
            long readChecksum = 0L;
            for (int i = 0; i < 8; i++) {
                readChecksum |= ((long) (result[10 + i] & 0xFF)) << (i * 8);
            }
            assertEquals(expectedCrcWithEof, readChecksum, "文件中的校验和应该与计算的CRC64一致");
        }
    }

    @Nested
    @DisplayName("CRC64校验测试")
    class Crc64VerificationTests {

        @Test
        @DisplayName("CRC64校验成功测试")
        void testVerifyCrc64ChecksumSuccess() throws IOException {
            // 1. 准备带有正确CRC64的数据
            ByteArrayOutputStream dataStream = createDataWithCorrectCrc64();
            
            // 2. 创建输入流进行验证
            ByteArrayInputStream bais = new ByteArrayInputStream(dataStream.toByteArray());
            Crc64InputStream cis = new Crc64InputStream(bais);
            
            // 3. 读取数据（除了最后8字节的CRC64）
            byte[] buffer = new byte[dataStream.size() - 8];
            cis.read(buffer);
            
            // 4. 验证CRC64
            boolean result = RdbUtils.verifyCrc64Checksum(cis);
            
            // 5. 验证结果
            assertTrue(result, "正确的CRC64校验应该成功");
        }

        @Test
        @DisplayName("CRC64校验失败测试")
        void testVerifyCrc64ChecksumFailure() throws IOException {
            // 1. 准备带有错误CRC64的数据
            ByteArrayOutputStream dataStream = createDataWithIncorrectCrc64();
            
            // 2. 创建输入流进行验证
            ByteArrayInputStream bais = new ByteArrayInputStream(dataStream.toByteArray());
            Crc64InputStream cis = new Crc64InputStream(bais);
            
            // 3. 读取数据（除了最后8字节的CRC64）
            byte[] buffer = new byte[dataStream.size() - 8];
            cis.read(buffer);
            
            // 4. 验证CRC64
            boolean result = RdbUtils.verifyCrc64Checksum(cis);
            
            // 5. 验证结果
            assertFalse(result, "错误的CRC64校验应该失败");
        }        private ByteArrayOutputStream createDataWithCorrectCrc64() throws IOException {
            ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
            try (Crc64OutputStream cos = new Crc64OutputStream(dataStream)) {
                // 写入测试数据
                DataOutputStream dos = cos.getDataOutputStream();
                dos.writeBytes("test data for crc64 verification");
                dos.flush();
                
                // 写入正确的CRC64
                cos.writeCrc64Checksum();
                cos.flush();
            }
            
            return dataStream;
        }

        private ByteArrayOutputStream createDataWithIncorrectCrc64() throws IOException {
            ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
            
            // 写入测试数据
            dataStream.write("test data for crc64 verification".getBytes());
            
            // 写入错误的CRC64（全是0xFF）
            for (int i = 0; i < 8; i++) {
                dataStream.write(0xFF);
            }
            
            return dataStream;
        }
    }

    @Nested
    @DisplayName("数据类型读写测试")
    class DataTypeReadWriteTests {

        @Test
        @DisplayName("字符串保存测试")
        void testSaveString() throws IOException {
            // 1. 准备测试数据
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            RedisBytes key = new RedisBytes("test-key".getBytes());
            RedisString value = new RedisString(Sds.create("test-value".getBytes()));
            
            // 2. 保存字符串
            RdbUtils.saveString(dos, key, value);
            dos.flush();
            
            // 3. 验证结果
            byte[] result = baos.toByteArray();
            assertTrue(result.length > 0, "保存的数据应该不为空");
            assertEquals(RdbConstants.STRING_TYPE, result[0], "第一个字节应该是字符串类型标识");
        }

        @Test
        @DisplayName("字符串加载测试")
        void testLoadString() throws IOException {
            // 1. 准备包含字符串的数据流
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // 写入键长度和内容
            String keyStr = "test-key";
            dos.writeByte(keyStr.length());
            dos.writeBytes(keyStr);
            
            // 写入值长度和内容
            String valueStr = "test-value";
            dos.writeByte(valueStr.length());
            dos.writeBytes(valueStr);
            dos.flush();
            
            // 2. 创建输入流
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream dis = new DataInputStream(bais);
            
            // 3. 设置Mock期望
            doNothing().when(mockRedisCore).selectDB(anyInt());
            doNothing().when(mockRedisCore).put(any(RedisBytes.class), any(RedisString.class));
            
            // 4. 加载字符串
            assertDoesNotThrow(() -> {
                RdbUtils.loadString(dis, mockRedisCore, 0);
            }, "字符串加载不应该抛出异常");
            
            // 5. 验证Mock调用
            verify(mockRedisCore).selectDB(0);
            verify(mockRedisCore).put(any(RedisBytes.class), any(RedisString.class));
        }

        @Test
        @DisplayName("列表保存测试")
        void testSaveList() throws IOException {
            // 1. 准备测试数据
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            RedisBytes key = new RedisBytes("list-key".getBytes());
            RedisList value = new RedisList();
            value.lpush(new RedisBytes("item1".getBytes()));
            value.lpush(new RedisBytes("item2".getBytes()));
            
            // 2. 保存列表
            RdbUtils.saveList(dos, key, value);
            dos.flush();
            
            // 3. 验证结果
            byte[] result = baos.toByteArray();
            assertTrue(result.length > 0, "保存的数据应该不为空");
            assertEquals(RdbConstants.LIST_TYPE, result[0], "第一个字节应该是列表类型标识");
        }

        @Test
        @DisplayName("集合保存测试")
        void testSaveSet() throws IOException {
            // 1. 准备测试数据
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            RedisBytes key = new RedisBytes("set-key".getBytes());
            RedisSet value = new RedisSet();
            value.add(java.util.Arrays.asList(
                new RedisBytes("member1".getBytes()),
                new RedisBytes("member2".getBytes())
            ));
            
            // 2. 保存集合
            RdbUtils.saveSet(dos, key, value);
            dos.flush();
            
            // 3. 验证结果
            byte[] result = baos.toByteArray();
            assertTrue(result.length > 0, "保存的数据应该不为空");
            assertEquals(RdbConstants.SET_TYPE, result[0], "第一个字节应该是集合类型标识");
        }
    }

    @Nested
    @DisplayName("数据库选择测试")
    class DatabaseSelectionTests {

        @Test
        @DisplayName("写入数据库选择测试")
        void testWriteSelectDB() throws IOException {
            // 1. 准备输出流
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // 2. 写入数据库选择
            int dbId = 5;
            RdbUtils.writeSelectDB(dos, dbId);
            dos.flush();
            
            // 3. 验证结果
            byte[] result = baos.toByteArray();
            assertTrue(result.length >= 2, "数据库选择至少包含2字节");
            assertEquals(RdbConstants.RDB_OPCODE_SELECTDB, result[0], "第一个字节应该是数据库选择操作码");
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class BoundaryConditionTests {

        @Test
        @DisplayName("空流处理测试")
        void testEmptyStreamHandling() throws IOException {
            // 1. 准备空输入流
            ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
            DataInputStream dis = new DataInputStream(bais);
            
            // 2. 尝试验证头部
            boolean result = RdbUtils.checkRdbHeader(dis);
            
            // 3. 验证结果
            assertFalse(result, "空流头部验证应该失败");
        }

        @Test
        @DisplayName("大数据库ID写入测试")
        void testLargeDatabaseId() throws IOException {
            // 1. 准备输出流
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // 2. 写入大数据库ID
            int largeDbId = 1000000;
            assertDoesNotThrow(() -> {
                RdbUtils.writeSelectDB(dos, largeDbId);
            }, "大数据库ID写入不应该抛出异常");
        }
    }
}

package site.hnfy258.rdb.crc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

/**
 * CRC64流包装器单元测试
 * 
 * <p>测试CRC64输入输出流包装器的功能，包括：</p>
 * <ul>
 *     <li>输出流CRC64实时计算</li>
 *     <li>输入流CRC64校验</li>
 *     <li>流操作透明性</li>
 *     <li>校验和读写正确性</li>
 * </ul>
 */
@DisplayName("CRC64流包装器测试")
class Crc64StreamTest {

    @Nested
    @DisplayName("Crc64OutputStream测试")
    class Crc64OutputStreamTests {

        @Test
        @DisplayName("基本写入和CRC64计算")
        void testBasicWriteAndCrc64Calculation() throws IOException {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 Crc64OutputStream crcStream = new Crc64OutputStream(byteStream)) {
                
                // 1. 初始CRC64应该为0
                assertEquals(0L, crcStream.getCrc64(), "初始CRC64应该为0");
                
                // 2. 写入测试数据
                String testData = "Hello Redis";
                byte[] testBytes = testData.getBytes(StandardCharsets.UTF_8);
                crcStream.write(testBytes);
                
                // 3. 验证CRC64已更新
                long expectedCrc = Crc64.crc64(testBytes);
                assertEquals(expectedCrc, crcStream.getCrc64(), "CRC64应该正确计算");
                
                // 4. 验证数据正确写入
                crcStream.flush();
                assertArrayEquals(testBytes, byteStream.toByteArray(), "数据应该正确写入底层流");
            }
        }

        @Test
        @DisplayName("DataOutputStream包装写入测试")
        void testDataOutputStreamWrapper() throws IOException {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 Crc64OutputStream crcStream = new Crc64OutputStream(byteStream)) {
                
                DataOutputStream dos = crcStream.getDataOutputStream();
                
                // 1. 通过DataOutputStream写入不同类型数据
                dos.writeBytes("REDIS0009");
                dos.writeByte(0xFF);
                dos.writeInt(12345);
                dos.writeLong(67890L);
                
                dos.flush();
                
                // 2. 验证CRC64正确计算
                assertNotEquals(0L, crcStream.getCrc64(), "CRC64应该已更新");
                
                // 3. 手动计算期望的CRC64
                ByteArrayOutputStream expected = new ByteArrayOutputStream();
                DataOutputStream expectedDos = new DataOutputStream(expected);
                expectedDos.writeBytes("REDIS0009");
                expectedDos.writeByte(0xFF);
                expectedDos.writeInt(12345);
                expectedDos.writeLong(67890L);
                expectedDos.flush();
                
                long expectedCrc = Crc64.crc64(expected.toByteArray());
                assertEquals(expectedCrc, crcStream.getCrc64(), "DataOutputStream包装应该正确计算CRC64");
            }
        }

        @Test
        @DisplayName("增量写入CRC64计算")
        void testIncrementalWriteCrc64() throws IOException {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 Crc64OutputStream crcStream = new Crc64OutputStream(byteStream)) {
                
                // 1. 分批写入数据
                String part1 = "Hello ";
                String part2 = "Redis";
                String full = part1 + part2;
                
                crcStream.write(part1.getBytes());
                long intermediateCrc = crcStream.getCrc64();
                
                crcStream.write(part2.getBytes());
                long finalCrc = crcStream.getCrc64();
                
                // 2. 验证最终CRC64
                long expectedCrc = Crc64.crc64(full.getBytes());
                assertEquals(expectedCrc, finalCrc, "增量写入最终CRC64应该正确");
                
                // 3. 验证中间CRC64
                long expectedIntermediateCrc = Crc64.crc64(part1.getBytes());
                assertEquals(expectedIntermediateCrc, intermediateCrc, "中间CRC64应该正确");
            }
        }

        @Test
        @DisplayName("CRC64校验和写入测试")
        void testCrc64ChecksumWrite() throws IOException {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 Crc64OutputStream crcStream = new Crc64OutputStream(byteStream)) {
                
                // 1. 写入测试数据
                String testData = "Test Data";
                crcStream.write(testData.getBytes());
                
                long calculatedCrc = crcStream.getCrc64();
                
                // 2. 写入CRC64校验和
                crcStream.writeCrc64Checksum();
                crcStream.flush();
                
                // 3. 验证CRC64未因校验和写入而改变
                assertEquals(calculatedCrc, crcStream.getCrc64(), "写入校验和不应该改变CRC64值");
                
                // 4. 验证写入的数据长度（数据 + 8字节校验和）
                byte[] result = byteStream.toByteArray();
                assertEquals(testData.length() + 8, result.length, "应该包含数据和8字节校验和");
                
                // 5. 验证校验和以小端序存储
                ByteArrayInputStream bis = new ByteArrayInputStream(result);
                bis.skip(testData.length()); // 跳过测试数据
                
                long readChecksum = 0L;
                for (int i = 0; i < 8; i++) {
                    int b = bis.read();
                    readChecksum |= ((long) (b & 0xFF)) << (i * 8);
                }
                
                assertEquals(calculatedCrc, readChecksum, "读取的校验和应该与计算的CRC64一致");
            }
        }
    }

    @Nested
    @DisplayName("Crc64InputStream测试")
    class Crc64InputStreamTests {

        @Test
        @DisplayName("基本读取和CRC64计算")
        void testBasicReadAndCrc64Calculation() throws IOException {
            // 1. 准备测试数据
            String testData = "Hello Redis";
            byte[] testBytes = testData.getBytes(StandardCharsets.UTF_8);
            
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(testBytes);
                 Crc64InputStream crcStream = new Crc64InputStream(byteStream)) {
                
                // 2. 初始CRC64应该为0
                assertEquals(0L, crcStream.getCrc64(), "初始CRC64应该为0");
                
                // 3. 读取所有数据
                byte[] buffer = new byte[testBytes.length];
                int bytesRead = crcStream.read(buffer);
                
                // 4. 验证读取正确
                assertEquals(testBytes.length, bytesRead, "应该读取所有字节");
                assertArrayEquals(testBytes, buffer, "读取的数据应该正确");
                
                // 5. 验证CRC64计算
                long expectedCrc = Crc64.crc64(testBytes);
                assertEquals(expectedCrc, crcStream.getCrc64(), "CRC64应该正确计算");
            }
        }

        @Test
        @DisplayName("DataInputStream包装读取测试")
        void testDataInputStreamWrapper() throws IOException {
            // 1. 准备复合数据
            ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(dataStream);
            dos.writeBytes("REDIS0009");
            dos.writeByte(0xFF);
            dos.writeInt(12345);
            dos.writeLong(67890L);
            dos.flush();
            
            byte[] testData = dataStream.toByteArray();
            
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(testData);
                 Crc64InputStream crcStream = new Crc64InputStream(byteStream)) {
                
                DataInputStream dis = crcStream.getDataInputStream();
                
                // 2. 通过DataInputStream读取数据
                byte[] header = new byte[9];
                dis.readFully(header);
                assertEquals("REDIS0009", new String(header), "头部应该正确读取");
                
                int byteValue = dis.readByte() & 0xFF;
                assertEquals(0xFF, byteValue, "字节值应该正确读取");
                
                int intValue = dis.readInt();
                assertEquals(12345, intValue, "整数值应该正确读取");
                
                long longValue = dis.readLong();
                assertEquals(67890L, longValue, "长整数值应该正确读取");
                
                // 3. 验证CRC64
                long expectedCrc = Crc64.crc64(testData);
                assertEquals(expectedCrc, crcStream.getCrc64(), "DataInputStream包装应该正确计算CRC64");
            }
        }

        @Test
        @DisplayName("CRC64校验和读取测试")
        void testCrc64ChecksumRead() throws IOException {
            // 1. 准备带校验和的数据
            String testData = "Test Data";
            byte[] testBytes = testData.getBytes();
            long expectedCrc = Crc64.crc64(testBytes);
            
            ByteArrayOutputStream fullDataStream = new ByteArrayOutputStream();
            fullDataStream.write(testBytes);
            
            // 写入校验和（小端序）
            for (int i = 0; i < 8; i++) {
                fullDataStream.write((int) (expectedCrc >>> (i * 8)) & 0xFF);
            }
            
            byte[] fullData = fullDataStream.toByteArray();
            
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(fullData);
                 Crc64InputStream crcStream = new Crc64InputStream(byteStream)) {
                
                // 2. 读取数据部分
                byte[] buffer = new byte[testBytes.length];
                int bytesRead = crcStream.read(buffer);
                
                assertEquals(testBytes.length, bytesRead, "应该读取所有数据字节");
                assertArrayEquals(testBytes, buffer, "读取的数据应该正确");
                
                // 3. 验证计算的CRC64
                assertEquals(expectedCrc, crcStream.getCrc64(), "计算的CRC64应该正确");
                
                // 4. 读取并验证校验和
                long readChecksum = crcStream.readCrc64Checksum();
                assertEquals(expectedCrc, readChecksum, "读取的校验和应该与期望一致");
            }
        }

        @Test
        @DisplayName("校验和读取异常测试")
        void testChecksumReadException() throws IOException {
            // 1. 准备不完整的数据（少于8字节校验和）
            byte[] incompleteData = {0x01, 0x02, 0x03}; // 只有3字节
            
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(incompleteData);
                 Crc64InputStream crcStream = new Crc64InputStream(byteStream)) {
                
                // 2. 尝试读取校验和应该抛出异常
                assertThrows(IOException.class, () -> {
                    crcStream.readCrc64Checksum();
                }, "读取不完整校验和应该抛出IOException");
            }
        }
    }

    @Nested
    @DisplayName("流集成测试")
    class StreamIntegrationTests {

        @TempDir
        Path tempDir;

        @Test
        @DisplayName("写入和读取完整流程测试")
        void testCompleteWriteReadCycle(@TempDir Path tempDir) throws IOException {
            Path testFile = tempDir.resolve("test.rdb");
            String testData = "Complete Test Data for RDB";
            
            // 1. 写入阶段
            long writtenCrc;
            try (FileOutputStream fos = new FileOutputStream(testFile.toFile());
                 Crc64OutputStream crcOut = new Crc64OutputStream(fos)) {
                
                DataOutputStream dos = crcOut.getDataOutputStream();
                dos.writeBytes("REDIS0009");
                dos.writeBytes(testData);
                dos.writeByte(0xFF); // EOF
                
                writtenCrc = crcOut.getCrc64();
                crcOut.writeCrc64Checksum();
            }
            
            // 2. 读取阶段
            try (FileInputStream fis = new FileInputStream(testFile.toFile());
                 Crc64InputStream crcIn = new Crc64InputStream(fis)) {
                
                DataInputStream dis = crcIn.getDataInputStream();
                
                // 读取头部
                byte[] header = new byte[9];
                dis.readFully(header);
                assertEquals("REDIS0009", new String(header), "头部应该正确");
                
                // 读取数据
                byte[] dataBuffer = new byte[testData.length()];
                dis.readFully(dataBuffer);
                assertEquals(testData, new String(dataBuffer), "数据应该正确");
                
                // 读取EOF
                int eof = dis.readByte() & 0xFF;
                assertEquals(0xFF, eof, "EOF应该正确");
                
                // 验证计算的CRC64
                assertEquals(writtenCrc, crcIn.getCrc64(), "读取计算的CRC64应该与写入时一致");
                
                // 读取并验证校验和
                long readChecksum = crcIn.readCrc64Checksum();
                assertEquals(writtenCrc, readChecksum, "读取的校验和应该与写入的一致");
            }
        }

        @Test
        @DisplayName("数据损坏检测测试")
        void testDataCorruptionDetection(@TempDir Path tempDir) throws IOException {
            Path testFile = tempDir.resolve("corrupt.rdb");
            String testData = "Original Data";
            
            // 1. 写入正常数据
            try (FileOutputStream fos = new FileOutputStream(testFile.toFile());
                 Crc64OutputStream crcOut = new Crc64OutputStream(fos)) {
                
                crcOut.write(testData.getBytes());
                crcOut.writeCrc64Checksum();
            }
            
            // 2. 人为损坏数据
            try (RandomAccessFile raf = new RandomAccessFile(testFile.toFile(), "rw")) {
                raf.seek(5); // 跳到数据中间
                raf.write(0x99); // 修改一个字节
            }
            
            // 3. 尝试读取损坏的数据
            try (FileInputStream fis = new FileInputStream(testFile.toFile());
                 Crc64InputStream crcIn = new Crc64InputStream(fis)) {
                
                // 读取数据
                byte[] buffer = new byte[testData.length()];
                crcIn.read(buffer);
                
                // 读取校验和
                long fileChecksum = crcIn.readCrc64Checksum();
                long calculatedChecksum = crcIn.getCrc64();
                
                // 4. 验证校验和不匹配（检测到数据损坏）
                assertNotEquals(fileChecksum, calculatedChecksum, "应该检测到数据损坏");
            }
        }
    }
}

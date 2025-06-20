package site.hnfy258.aof.writer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Redis 持久化写入器接口
 * 
 * <p>定义了 Redis 持久化系统的基本写入操作。
 * 提供统一的接口规范，支持 AOF 和 RDB 两种持久化方式。
 * 
 * <p>核心功能：
 * <ul>
 *     <li>数据写入 - 将数据写入持久化文件</li>
 *     <li>刷盘操作 - 确保数据真正写入磁盘</li>
 *     <li>资源管理 - 正确关闭和释放资源</li>
 *     <li>后台重写 - 支持文件重写和压缩</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
public interface Writer {
    /**
     * 写入数据到持久化文件
     * 
     * @param buffer 待写入的数据缓冲区
     * @return 实际写入的字节数
     * @throws IOException 写入过程中发生IO错误
     */
    int write(ByteBuffer buffer) throws IOException;

    /**
     * 将缓冲区数据刷新到磁盘
     * 
     * @throws IOException 刷盘过程中发生IO错误
     */
    void flush() throws IOException;

    /**
     * 关闭写入器并释放相关资源
     * 
     * @throws IOException 关闭过程中发生IO错误
     */
    void close() throws IOException;

    /**
     * 在后台执行文件重写操作
     * 
     * @return 重写操作是否成功启动
     * @throws IOException 重写过程中发生IO错误
     */
    boolean bgrewrite() throws IOException;
}

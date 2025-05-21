package site.hnfy258.aof.writer;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AofBatchWriter {
    private final Writer writer;

    //异步刷盘
    private final ScheduledExecutorService flushScheduler;
    private final AtomicBoolean forceFlush = new AtomicBoolean(false);
    private final int flushInterval;

    //队列
    private static final int DEFAULT_QUEUQ_SIZE = 1000;
    private final BlockingQueue<ByteBuf> writeQueue;
    private final Thread writeThread;
    private final AtomicBoolean running = new AtomicBoolean(true);


    //背压阈值
    private static final int DEFAULT_BACKPRESSURE_THRESHOLD = 6*1024*1024;
    private final AtomicInteger pendingBytes = new AtomicInteger(0);

    //批处理参数
    public static final int MIN_BATCH_SIZE = 16;
    public static final int MAX_BATCH_SIZE = 50;

    public static final int  MIN_BATCH_TIMEOUT_MS = 2;
    public static final int  MAX_BATCH_TIMEOUT_MS = 10;

    //大命令的参数
    private static final int LARGE_COMMAND_THRESHOLD = 512*1024;


    private AtomicLong batchCount = new AtomicLong(0);
    private AtomicLong totalBatchedCommands = new AtomicLong(0);

    public AofBatchWriter(Writer fileWriter, int  flushInterval){
        this.writer = fileWriter;
        this.flushInterval = flushInterval;



        this.writeQueue = new LinkedBlockingDeque<>(DEFAULT_QUEUQ_SIZE);
        this.writeThread = new Thread(this::processWriteQueue);
        this.writeThread.setName("AOF-Writer-Thread");
        this.writeThread.setDaemon(true);
        this.writeThread.start();

        this.flushScheduler = new ScheduledThreadPoolExecutor(1, r->{
            Thread thread = new Thread(r);
            thread.setName("AOF-Flush-Thread");
            thread.setDaemon(true);
            return thread;
        });

        if(flushInterval > 0){
            this.flushScheduler.scheduleAtFixedRate(()->{
                try{
                    if(forceFlush.compareAndSet(true, false)){
                        writer.flush();
                    }
                }catch(Exception e){
                    log.error("Failed to flush AOF file", e);
                }
            },flushInterval, flushInterval, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    public void processWriteQueue(){
        ByteBuf[] batch = new ByteBuf[MAX_BATCH_SIZE];
        int batchSize = 0;

        while(running.get() || !writeQueue.isEmpty()){
            try{
                int currentBatchSize = calculateBatchSize(batchSize);
                long timeout = calculateTimeout(currentBatchSize);

                long deadline = System.currentTimeMillis() + timeout;

                while(batchSize < currentBatchSize && System.currentTimeMillis() < deadline){
                    ByteBuf item = writeQueue.poll(Math.max(1,  deadline - System.currentTimeMillis()),
                            TimeUnit.MILLISECONDS);

                    if(item != null){
                        batch[batchSize++] = item;
                    }else if(batchSize == 0){
                        Thread.yield();
                    }else{
                        break;
                    }
                }

                if(batchSize > 0){
                    writeBatch(batch, batchSize);
                    for(int i = 0; i < batchSize; i++){
                        pendingBytes.addAndGet(-batch[i].readableBytes());
                        batch[i].release();
                        batch[i] = null;
                    }
                    batchSize = 0;
                }
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch
            (Exception e){
                log.error("Failed to process write queue", e);
                for(int i = 0; i < batchSize; i++){
                    batch[i].release();
                    batch[i] = null;
                }
                batchSize = 0;
            }
        }
    }

    private void writeBatch(ByteBuf[] batch, int batchSize) {
        if(batchSize <= 0) return;
        try{
            int totalBytes = 0;
            for(int i = 0; i < batchSize; i++){
                totalBytes+=batch[i].readableBytes();
            }

            ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

            for(int i = 0; i < batchSize; i++){
                buffer.put(batch[i].nioBuffer());
            }
            buffer.flip();
            writer.write(buffer);
            batchCount.incrementAndGet();
            totalBatchedCommands.addAndGet(batchSize);
        }catch(Exception e){
            log.error("Failed to write batch to AOF file", e);
        }
    }



    public void write(ByteBuf byteBuf) throws IOException{
        int byteSize = byteBuf.readableBytes();
        if(byteSize > LARGE_COMMAND_THRESHOLD){
            try{
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);
                flush();
            }finally {
                byteBuf.release();
                return;
            }
        }

        pendingBytes.addAndGet(byteSize);

        if(pendingBytes.get() > DEFAULT_BACKPRESSURE_THRESHOLD ||
        writeQueue.size() >DEFAULT_QUEUQ_SIZE *0.75){
            applyBackpressure();
        }

        try{
            boolean success = writeQueue.offer(byteBuf, 3, TimeUnit.SECONDS);
            if(!success){
                ByteBuffer byteBuffer = byteBuf.nioBuffer();
                writer.write(byteBuffer);
                byteBuf.release();
            }
        }catch(Exception e){
            byteBuf.release();
            Thread.currentThread().interrupt();
        }

    }

    public void flush() throws Exception{
        int retryCount = 0;
        int maxRetries = 3;
        while(!writeQueue.isEmpty() && retryCount < maxRetries){
            try{
                Thread.sleep(10);
                writer.flush();
                retryCount++;
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                break;
            }catch(IOException e){
                log.error("Failed to flush AOF file", e);
                retryCount++;
                if(retryCount >= maxRetries){
                    throw e;
                }
            }
            if(!writeQueue.isEmpty()){
                throw new IOException("刷盘超时，队列中还有"+writeQueue.size()+"个数据未写入");
            }
        }
    }

    public void close() throws Exception {
        if (flushScheduler != null) {
            try {
                flushScheduler.shutdown();
                try {
                    flushScheduler.shutdownNow();
                    boolean terminated = flushScheduler.awaitTermination(3, TimeUnit.SECONDS);
                    if (!terminated) {
                        log.warn("刷盘调度超时");
                    }
                } catch (InterruptedException e) {
                    flushScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                    log.warn("刷盘调度被中断");
                }
            } catch (Exception e) {
                log.error("关闭刷盘调度器时发生错误", e);
            }
            try {
                flush();
            } catch (Exception e) {
                log.error("关闭刷盘调度器时发生错误", e);
            }
            running.set(false);

            if (writeThread != null && writeThread.isAlive()) {
                writeThread.interrupt();
                try {
                    writeThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void applyBackpressure() {
        int currentBytes = pendingBytes.get();
        int queueSize = writeQueue.size();

        double pressureLevel = Math.max((double) currentBytes / DEFAULT_BACKPRESSURE_THRESHOLD, (double) queueSize / DEFAULT_QUEUQ_SIZE * 0.75);

        if (pressureLevel >= 1) {
            try {
                long waitTime = Math.min(50, (long) (pressureLevel * 20));

                if (pressureLevel >= 1.5) {
                    log.warn("AOF写入压力过大，当前队列大小为{}，当前待写入字节数为{}", queueSize, currentBytes);
                }

                if (pressureLevel >= 2) {
                    try {
                        forceFlush.set(true);
                    } catch (Exception e) {
                        log.error("强制刷盘失败", e);
                    }
                }

                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private long calculateTimeout(int currentBatchSize) {
        int queueSize = writeQueue.size();
        if(queueSize > DEFAULT_QUEUQ_SIZE / 2){
            return MIN_BATCH_TIMEOUT_MS;
        }
        return MAX_BATCH_TIMEOUT_MS;
    }

    private int calculateBatchSize(int batchSize) {
        int queueSize = writeQueue.size();
        int result = Math.min(MAX_BATCH_SIZE,Math.min(MIN_BATCH_SIZE, MIN_BATCH_SIZE + queueSize / 20));
        return result;
    }


    public int getBatchCount() {
        return (int) batchCount.get();
    }
}

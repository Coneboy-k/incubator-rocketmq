/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对应一个持久化文件
 *
 * RMQ的文件存储中，最终对应内存和文件映射的类是MapedFile这个类，
 * 而这个类完成内存和文件的映射主要是通过Java NIO包下 FileChannel 所提供的map方法来完成，
 *
 */
public class MappedFile extends ReferenceResource {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     *  pagecount 一般都是4k的倍数所以这么定义
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 映射虚拟内存总字节数
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 映射文件总数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 当前写入位置，下次开始写入的开始位置
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * ADD BY ChenYang
     * 当前commit位置
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 当前flush位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 文件大小
     */
    protected int fileSize;

    /**
     * fileChannel
     * {@link #file}的channel = new RandomAccessFile(this.file, "rw").getChannel()
     */
    protected FileChannel fileChannel;

    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 写入缓冲
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * writeBuffer缓存池
     */
    protected TransientStorePool transientStorePool = null;
    /**
     * 文件名
     */
    private String fileName;
    /**
     * 文件开始的offset。
     * 目前文件名即offset
     */
    private long fileFromOffset;
    /**
     * 文件
     */
    private File file;

    /**
     * 映射的内存对象
     */
    private MappedByteBuffer mappedByteBuffer;
    /**
     * 最后插入数据时间。即{@link #mappedByteBuffer}变更时间
     */
    private volatile long storeTimestamp = 0;
    /**
     * 是否最先创建在队列，   可以理解为填充前置空白占位
     * {@link MappedFileQueue#getLastMappedFile(long, boolean)}
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
        log.info("MappedFile init");
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 构造方法
     */
    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 确保文件目录已存在
     *
     * @param dirName 目录名
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     *  1.初始化方法
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     *  2. 初始化fileChannel、mappedByteBuffer  核心启动地方
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException 文件不存在 or io异常
     */
    private void init(final String fileName, final int fileSize) throws IOException {

        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 确保文件ok，没有文件夹就创建
        ensureDirOK(this.file.getParent());

        try {
            // nio 开启映射
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 映射文件的大小，也是这个文件最大的大小
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 更新映射大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // 映射文件搞起
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            // 如果没准备直接retrun
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 3. 写消息到内存映射区。
     * 实际是插入映射文件buffer
     *
     * @param msg 消息
     * @param cb 逻辑
     * @return 附加消息结果
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {

        assert msg != null;
        assert cb != null;

        // 当前已经写了多少
        int currentPos = this.wrotePosition.get();

        // 如果还可以写
        if (currentPos < this.fileSize) {

            // 获取写的数据， 如果 writeBuffer 不为空就用writeBuffer  否则用映射的mmap
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 设置byte 当前的下标
            byteBuffer.position(currentPos);
            // 回调 commlog 去写入数据
            AppendMessageResult result =
                cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        log.error("MappedFile.appendMessage return null, wrotePosition: " + currentPos + " fileSize: "
            + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     *
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 写入文件， 目前用到的是 consumequeue 写入的
     */
    public boolean appendMessage(final byte[] data) {

        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * flush
     *
     * @param flushLeastPages flush最小页数
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 尚未写入磁盘的数据强制写到磁盘上。出于性能方面的考虑，操作系统会将数据缓存在内存中，
                        // 所以无法保证写入到FileChannel里的数据一定会即时写到磁盘上。要保证这一点，需要调用force()方法。
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force(); // 对缓冲区进行强制写入
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 保证数据已经写到磁盘文件
     *
     * commit
     * 当{@link #writeBuffer}为null时，直接返回{@link #wrotePosition}
     *
     * @param commitLeastPages commit最小页数
     * @return 当前commit位置
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel. 写到文件尾时，回收writeBuffer。
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * commit实现，将writeBuffer写入fileChannel。
     *
     * @param commitLeastPages commit最小页数。用不上该参数
     */
    protected void commit0(final int commitLeastPages) {

        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                // 设置需要写入的byteBuffer
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                // 写入fileChannel
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                // 设置position
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够 flush 。满足如下条件任意条件：
     * 1. 映射文件已经写满
     * 2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     * 3. flushLeastPages = 0 && 有新写入部分
     *
     *  通过上面的实现可以看出，如果通过方法isFull判断文件已经写满了是可以强制写文件的，
     *  然而就是如果没满而且给commit方法的参数大于0，则会先按OS_PAGE_SIZE(内存页大小，这里是4K每页)大小
     *  通过“write / OS_PAGE_SIZE”求出已经写入了多少页，同理，然后依然通过内存页大小通过“flush / OS_PAGE_SIZE”求出已经提交了多少页
     *  ，然后相减得出已经写入了多少页了，这个时候比较commit的参数flushLeastPages，如果大于等于这个值则也可以强制写入文件，
     *  当然如果给commit的参数给了一个0，则比较写入的大小是否大于提交的大小“write > flush”，也就是只要一写入数据到映射内存就可以强制写到文件中。
     *
     * @param flushLeastPages flush最小分页
     * @return 是否能够写入
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 是否能够commit。满足如下条件任意条件：
     * 1. 映射文件已经写满
     * 2. commitLeastPages > 0 && 未commit部分超过commitLeastPages
     * 3. commitLeastPages = 0 && 有新写入部分
     *
     * @param commitLeastPages commit最小分页
     * @return 是否能够写入
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 根据 pos 获取 指定size 映射Buffer
     *
     * @see #getReadPosition()
     * @param pos 当前 Buffer 的 pos
     * @param size 长度
     * @return 映射Buffer
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 根据 pos 获取 映射Buffer，用途是保存数据到commentqueue 里面，进行逻辑保存
     *
     * @see #getReadPosition()
     * @param pos 当前 Buffer 的 pos
     * @return 映射Buffer
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmaping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 写入磁盘
     * armMappedFile接受2个参数，一个是刷盘的类型，有同步:SYNC_FLUSH和异步:ASYNC_FLUSH，
     * 当然强制写回磁盘的策略和之前说过的MapedFile的commit是一致的，
     * 按照 (写入字节 / OS_PAGE_SIZE) - (force后提交字节 / OS_PAGE_SIZE) = 已经写入的页，
     * 最后比较传入的pages参数“(i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages”来决定
     * 是否mappedByteBuffer.force()，在这个循环里不断向映射的内存区域写入字节数据，循环结束后，
     * 在将还没写到磁盘的数据在来一遍force来达到最后flush效果，最后这个mlock是个黑科技了
     *
     * @param type 一个是刷盘的类型，有同步:SYNC_FLUSH和异步:ASYNC_FLUSH，
     * @param pages 按照 (写入字节 / OS_PAGE_SIZE) - (force后提交字节 / OS_PAGE_SIZE) = 已经写入的页，
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc 停止新生代gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        // 黑魔法，直接锁定swap数据，强制一次写入数据
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 这个方法是一个Native级别的调用，调用了标准C库的方法，
     * 首先看mlock方法，mlock方法在标准C中的实现是将锁住指定的内存区域避免被操作系统调到swap空间中，
     * 而madvise方法则要配合着mmap来说了，一般来说通过mmap建立起的内存文件在刚开始并没有将文件内容映射进来，
     * 而是只建立一个映射关系，而当你读相对应区域的时候，它第一次还是会去读磁盘，而我们前面说了，读写基本上都只是和
     * Page Cache打交道，那么当读相对应页没有拿到数据的时候，系统将会产生一个缺页异常，然后去读磁盘中的内容，
     * 最后写回Page Cache然后再次读取Page Cache然后返回，而madvise的作用是一次性先将一段数据读入到映射内存区域，
     * 这样就减少了缺页异常的产生
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}

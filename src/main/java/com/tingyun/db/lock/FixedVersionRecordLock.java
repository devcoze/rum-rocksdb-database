package com.tingyun.db.lock;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

/**
 * 固定版本号记录锁
 * Record Structure:
 *  Meta Data (2 bytes)
 *  - Current Version (short) : 2 bytes - 当前版本号
 *  Records (10 bytes each)
 *  - Version (short) : 2 bytes - 版本号
 *  - Value (long) : 8 bytes - 关联值
 */
@Slf4j
public final class FixedVersionRecordLock {

    private static final String VERSION = "_VERSION";

    // 元数据大小 (字节), short(2)
    private static final int META_SIZE = 2;
    // 每条记录的大小 (字节), short(2) + long(8) = 10字节
    private static final int RECORD_SIZE = 10;
    // 默认记录数
    private static final int DEFAULT_RECORDS = 64;
    // 最大记录数, 受限于short类型
    private static final int MAX_RECORDS = Short.MAX_VALUE;

    /**
     * 能够存储的版本号数量
     */
    private final int recordCount;
    /** 文件通道 */
    private final FileChannel channel;
    /** 内存映射缓冲区 */
    private final MappedByteBuffer buffer;

    public FixedVersionRecordLock(Path path) throws IOException {
        this(path, DEFAULT_RECORDS);
    }

    /**
     * 构造函数
     * @param path 文件路径
     * @param records 版本号数量, 取值范围 [1, 32767], 超过范围则取默认值64
     * @throws IOException 如果IO异常
     */
    public FixedVersionRecordLock(Path path, int records) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        if (!path.endsWith(VERSION)) {
            path = path.resolve(VERSION);
        }
        if (records <= 0) {
            records = DEFAULT_RECORDS;
        } else if (records > MAX_RECORDS) {
            records = MAX_RECORDS;
        }
        this.recordCount = records;
        // 1. 打开文件通道
        this.channel = FileChannel.open(path, READ, WRITE, CREATE);
        // 2. 计算文件大小
        long expectedSize = META_SIZE + (long) recordCount * RECORD_SIZE;
        long currentSize = channel.size();
        // 3. 设置文件大小
        if (currentSize < expectedSize) {
            long fillSize = expectedSize - currentSize;
            channel.position(currentSize);
            channel.write(ByteBuffer.allocate((int) fillSize));
            channel.force(true);
        }
        // 建立内存映射
        long mapSize = Math.max(currentSize, expectedSize);
        buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
    }

    /**
     * 获取最大版本号, 即最后存储的版本号
     * @return 最大版本号
     */
    public short maxVersion() {
        buffer.position(0);
        return buffer.getShort();
    }

    /**
     * 尝试锁定元数据区域
     * @return 文件锁， 如果返回null表示锁定失败
     * @throws IOException 如果IO异常
     */
    public FileLock tryLockMeta() throws IOException {
        return channel.tryLock(0, META_SIZE, false);
    }

    public void updateMeta(short version) {
        if (version <= 0 || version >= recordCount) {
            throw new IllegalArgumentException("Version out of range: " + version);
        }
        buffer.position(0);
        buffer.putShort(version);
        buffer.force();
    }

    /**
     * 尝试锁定指定版本号的记录, Version [1, recordCount-1]
     * @param version 版本号
     * @return true表示锁定成功, false表示锁定失败
     * @throws IOException 如果IO异常
     */
    public FileLock tryLockRecord(short version) throws IOException {
        if (version <= 0 || version >= recordCount) {
            throw new IllegalArgumentException("Version out of range: " + version);
        }
        int position = META_SIZE + (version - 1) * RECORD_SIZE;
        return channel.lock(position, RECORD_SIZE, false);
    }

    /**
     * 释放锁
     *
     * @param lock 文件锁
     * @throws IOException 如果IO异常
     */
    public void releaseLock(FileLock lock) throws IOException {
        if (lock != null && lock.isValid()) {
            lock.release();
        }
    }

    /**
     * 存储记录, Version [1, recordCount-1]
     * @param version 版本号
     * @param value 值
     */
    public void put(short version, long value) throws IOException {
        Preconditions.checkArgument(version <= 0 || version >= recordCount, "Version out of range: " + version);
        FileLock fileLock = tryLockRecord(version);
        if (fileLock == null) {
            throw new IOException("Failed to acquire lock for version: " + version);
        }
        try {
            int position = META_SIZE + (version - 1) * RECORD_SIZE;
            buffer.position(position);
            buffer.putShort(version);
            buffer.putLong(value);
            buffer.force();
        } finally {
            releaseLock(fileLock);
        }
    }

    /**
     * 获取记录值, Version [1, recordCount-1]
     * @param version 版本号
     * @return 值, 如果版本号不存在则返回null
     */
    public Long value(short version) {
        Preconditions.checkArgument(version <= 0 || version >= recordCount, "Version out of range: " + version);
        int position = META_SIZE + (version - 1) * RECORD_SIZE;
        try (FileLock fileLock = channel.lock(position, RECORD_SIZE, true)) {
            buffer.position(position);
            short ver = buffer.getShort();
            if (ver != version) {
                return null;
            }
            return buffer.getLong();
        } catch (IOException e) {
            log.error("Failed to get record for version: {}", version, e);
            return null;
        }
    }


    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

}

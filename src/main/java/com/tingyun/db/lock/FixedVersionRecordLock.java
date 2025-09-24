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
 *  Metadata (4 bytes)
 *  - Current Version (int) : 4 bytes - 当前版本号
 *  Records (10 bytes each)
 *  - Version (int) : 4 bytes - 版本号
 *  - Value (long) : 8 bytes - 关联值
 */
@Slf4j
public final class FixedVersionRecordLock implements AutoCloseable {

    // 版本文件管理文件名
    private static final String VERSION = "_VERSION";

    // 版本号大小 (字节), int(4)
    private static final int VERSION_SIZE = 4;
    // 记录值大小 (字节), long(8)
    private static final int VALUE_SIZE = 8;

    // 元数据大小 (字节), int(4) = 4字节
    private static final int META_SIZE = VERSION_SIZE;
    private static final int META_POSITION = 0;

    // 每条记录的大小 (字节), int(4) + long(8) = 10字节
    private static final int RECORD_SIZE = VERSION_SIZE + VALUE_SIZE;
    private static final int RECORD_OFFSET = VERSION_SIZE + META_POSITION;

    // 默认记录数
    private static final int DEFAULT_RECORDS = 64;
    // 最大记录数, 最大版本数
    private static final int MAX_RECORDS = 1024;

    /**
     * 能够存储的版本号数量
     */
    private final int recordCount;
    /** 文件通道 */
    private final FileChannel channel;
    private final Path versionRecordPath;
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
        this.versionRecordPath = path;
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
            int write = channel.write(ByteBuffer.allocate((int) fillSize));
            log.debug("Initialized version record file: {}, size: {}, filled: {}",
                    path.toAbsolutePath(), expectedSize, write);
            channel.force(true);
        }
        // 建立内存映射
        long mapSize = Math.max(currentSize, expectedSize);
        buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
    }

    /**
     * 获取最新版本号
     * @return 最新版本号
     */
    public int latest() {
        buffer.position(META_POSITION);
        return buffer.getInt();
    }

    /**
     * CAS 更新版本号：只有当前版本等于 expectedVersion 时才更新
     *
     * @param expectedVersion 期望的当前版本号
     * @param newVersion      新版本号, 版本只能向上增加
     * @return true=更新成功，false=版本不匹配或被其他进程占用
     * @throws IllegalArgumentException 如果版本号不在有效范围内
     */
    public boolean compareAndSetMeta(int expectedVersion, int newVersion) {
        // newVersion 必须在有效范围内
        checkVersion(newVersion);

        // newVersion 必须大于 expectedVersion, 避免回退
        Preconditions.checkArgument(newVersion > expectedVersion, "newVersion should be greater than expectedVersion");
        try (FileLock lock = tryLockMeta()) {
            if (lock == null) {
                return false; // 被其他进程锁住
            }
            int current = buffer.getInt(META_POSITION);
            if (current != expectedVersion) {
                return false; // 版本不一致，不更新
            }
            buffer.putInt(META_POSITION, newVersion);
            buffer.force();
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to update version", e);
        }
    }

    /**
     * 尝试锁定元数据区域
     * @return 文件锁， 如果返回null表示锁定失败
     */
    public FileLock tryLockMeta() {
        try {
            return channel.tryLock(META_POSITION, META_SIZE, false);
        } catch (Exception e) {
            log.error("Failed to lock record, recordVersionPath: {}", versionRecordPath, e);
            return null;
        }
    }

    /**
     * CAS 更新指定版本号的记录值, Version [1, recordCount-1]
     * @param version 版本号
     * @param expectedValue 期望值
     * @param newValue 新值
     * @return true=更新成功，false=版本不匹配或被其他进程占用
     * @throws IllegalArgumentException 如果版本号不在有效范围内
     */
    public boolean compareAndSetRecordValue(int version, long expectedValue, long newValue) {

        // 检查版本号是否在有效范围内
        checkVersion(version);

        // 计算记录位置
        int position = RECORD_OFFSET + (version - 1) * RECORD_SIZE;
        try (FileLock lock = tryLockRecord(version)) {
            if (lock == null) {
                return false; // 被其他进程锁住
            }
            // 移动到记录位置，读取版本号和值
            buffer.position(position);
            int ver = buffer.getInt();
            long currentValue = buffer.getLong();
            // 初始化版本号, 只有在第一次写入时才会更新版本号
            if (ver != version) {
                buffer.position(position);
                buffer.putInt(version);
            }
            if (currentValue != expectedValue) {
                return false; // 版本不一致或值不匹配，不更新
            }
            // 更新值, 移动到值的位置
            buffer.position(position + VERSION_SIZE);
            buffer.putLong(newValue);
            buffer.force();
            return true;
        } catch (Exception e) {
            log.error("Failed to update record for version: {}, expectedValue: {}, newValue: {}",
                    version, expectedValue, newValue, e);
        }
        return false;
    }

    /**
     * 尝试锁定指定版本号的记录, Version [1, recordCount]
     * @param version 版本号
     * @return true表示锁定成功, false表示锁定失败
     * @throws IllegalArgumentException 如果版本号不在有效范围内
     */
    public FileLock tryLockRecord(int version) {
        // 检查版本号是否在有效范围内
        checkVersion(version);
        int position = RECORD_OFFSET + (version - 1) * RECORD_SIZE;
        try {
            return channel.tryLock(position, RECORD_SIZE, false);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取记录值, Version [1, recordCount]
     * @param version 版本号
     * @return 值, 如果版本号不存在则返回null
     * @throws IllegalArgumentException 如果版本号不在有效范围内
     */
    public Long recordValue(int version) {
        // 检查版本号是否在有效范围内
        checkVersion(version);
        // 计算记录位置, 直接定位， meta(8) + (version-1)*record(12) + version(4)
        int position = RECORD_OFFSET + (version - 1) * RECORD_SIZE + VERSION_SIZE;
        buffer.position(position); // 移动到值的位置
        return buffer.getLong();
    }

    /**
     * 检查版本号是否在有效范围内
     * @param version 版本号
     */
    private void checkVersion(int version) {
        if (version < 1 || version > recordCount) {
            throw new IllegalArgumentException(
                    String.format("Version out of range: %d, expected: [1, %d]", version, recordCount)
            );
        }
    }

    /**
     * 关闭资源
     * @throws IOException 如果IO异常
     */
    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

}

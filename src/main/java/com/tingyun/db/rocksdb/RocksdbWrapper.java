package com.tingyun.db.rocksdb;

import com.tingyun.db.MultiDBManagerConfig;
import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import com.tingyun.db.rocksdb.writer.RocksdbOnceWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * RocksDB包装类，支持多版本管理，但数据只能写入一次，写入后不可修改
 * 每次写入数据会创建一个新的版本，旧版本的数据不可修改
 * @param <K> key类型
 * @param <V> value类型
 */
@Slf4j
public class RocksdbWrapper<K, V> extends ReadonlyRocksDBWrapper<K, V> {

    /**
     * 写入数据时，临时版本名称的模版
     */
    private static final String _temp_version_prefix = "_temp_v%d_%d";
    /**
     * 清理过期版本的时间，单位小时，默认24小时
     */
    private final int versionClearTime; // 小时

    static {
        RocksDB.loadLibrary();
    }

    public RocksdbWrapper(MultiDBManagerConfig config, String dbName, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        super(config.getDataDir(), dbName, kRocksdbSerde, vRocksdbSerde);
        int dbVersionExpire = config.getDbVersionExpire();
        int versionClearTime = config.getDbVersionCleanTime();
        if (versionClearTime <= dbVersionExpire) {
            this.versionClearTime = dbVersionExpire * 5;
        } else {
            this.versionClearTime = versionClearTime;
        }
    }


    /**
     * 构造函数
     * @param dataDir 数据存储目录
     * @param dbName 数据库名称
     * @param dbVersionCount 版本数据库的数量，超过该数量未被访问的版本数据库将被关闭
     * @param dbVersionExpire 版本数据库的过期时间，单位分钟，超过该时间未被访问的版本数据库将被关闭
     * @param dbVersionClearTime 版本数据库的清理时间，单位小时，默认24小时
     * @param kRocksdbSerde key的序列化工具
     * @param vRocksdbSerde value的序列化工具
     * @throws IOException 如果IO异常
     */
    public RocksdbWrapper(String dataDir, String dbName, int dbVersionCount,
                          int dbVersionExpire, int dbVersionClearTime, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        super(dataDir, dbName, dbVersionCount, dbVersionExpire, kRocksdbSerde, vRocksdbSerde);
        this.versionClearTime = dbVersionClearTime;
    }

    /**
     * 写入数据，数据只能写入一次，写入后不可修改
     * 数据写入时会创建一个新的版本，所以想要数据数据必须一次性写入完成
     * @param data 数据
     */
    public void writeOnce(Map<K, V> data) throws IOException, RocksDBException {

        // 1 数据库的版本号
        int latest = fixedVersionRecordLock.latest();
        int nextV = latest + 1;

        // 2 为了防止竞争，把数据写入临时版本中，临时版本是随机生成的
        String tempVersionPath = String.format(_temp_version_prefix, nextV, Instant.now().toEpochMilli());
        Path tempVerPath = dbPath.resolve(tempVersionPath);
        if (!Files.exists(tempVerPath)) {
            Files.createDirectories(tempVerPath);
        }

        // 3 写入完成后必须先关闭
        try (RocksDB newRocksDB = RocksDB.open(tempVerPath.toString())) {
            for (Map.Entry<K, V> entry : data.entrySet()) {
                byte[] kBytes = kRocksdbSerde.serializer(entry.getKey());
                byte[] vBytes = vRocksdbSerde.serializer(entry.getValue());
                newRocksDB.put(kBytes, vBytes);
            }
        }

        // 4 写入完成后，尝试更新数据库版本的原数据，如果更新成功，重新命名，如果失败直接删除
        finalizeVersion(tempVerPath, latest, nextV);

    }

    /**
     * 写入数据，数据只能写入一次，写入后不可修改
     * @param rocksdbOnceWriter 写入数据的代理, 该代理负责将数据写入RocksDB实例, 返回true表示写入成功, false表示写入失败
     * @throws IOException IO异常
     * @throws RocksDBException RocksDB异常
     */
    public void writeOnceWithWriter(RocksdbOnceWriter<K, V> rocksdbOnceWriter) throws IOException, RocksDBException {

        // 1 数据库的版本号
        int latest = fixedVersionRecordLock.latest();
        int nextV = latest + 1;

        // 2 为了防止竞争，把数据写入临时版本中，临时版本是随机生成的
        Path tempVerPath = prepareTempVersionPath(nextV);

        // 3 写入数据, 失败则删除临时目录
        boolean writeSuccess;
        try (RocksDB newRocksDB = RocksDB.open(tempVerPath.toString())) {
            writeSuccess = rocksdbOnceWriter.write(newRocksDB, kRocksdbSerde, vRocksdbSerde);
        }

        // 4 成功写入后，尝试更新数据库版本的原数据，如果更新成功，重新命名，如果失败直接删除
        if (writeSuccess) {
            finalizeVersion(tempVerPath, latest, nextV);
        } else {
            FileUtils.deleteDirectory(tempVerPath.toFile());
            log.trace("Writer aborted, temp version deleted: {}", tempVerPath);
        }

    }


    /**
     * 准备临时版本目录
     * @param nextV 下一个版本号
     * @return 临时版本目录路径
     */
    private Path prepareTempVersionPath(int nextV) throws IOException {
        String tempVersionName = String.format(_temp_version_prefix, nextV, Instant.now().toEpochMilli());
        Path tempVerPath = dbPath.resolve(tempVersionName);
        Files.createDirectories(tempVerPath);
        return tempVerPath;
    }

    /**
     * 写入完成后，尝试更新元信息并重命名版本目录
     * @param tempVerPath 临时版本目录
     * @param latest 当前最新版本号
     * @param nextV 下一个版本号
     */
    private void finalizeVersion(Path tempVerPath, int latest, int nextV) throws IOException {
        Path nextVersionPath = dbPath.resolve(String.valueOf(nextV));
        if (fixedVersionRecordLock.compareAndSetMeta(latest, nextV)) {
            Files.move(tempVerPath, nextVersionPath);
            log.info("Promoted RocksDB version {} -> {}", latest, nextV);
        } else {
            FileUtils.deleteDirectory(tempVerPath.toFile());
            log.warn("CAS failed, discard temp version: {}", tempVerPath);
        }
    }

    /**
     * 清理过期版本，至少保留一个版本
     */
    public void clear() {
        int latest = fixedVersionRecordLock.latest();
        // 当前时间戳
        long now = Instant.now().toEpochMilli();
        long expireTime = Duration.ofMinutes(this.versionClearTime).toMillis();
        // 至少保留一个版本，即清理范围 [1, latest-1] && (now - recordValue(lastOpenTime)) > expireTime
        for (int v = 1; v < latest; v++) {
            // 记录的是最后一次打开的时间戳
            long lastOpenTime = fixedVersionRecordLock.recordValue(v);
            if (lastOpenTime < 0 || now - lastOpenTime <= expireTime) {
                // 已经被清理，或者未过期
                continue;
            }
            if (fixedVersionRecordLock.compareAndSetRecordValue(v, lastOpenTime, CLEAR_STATE)) {
                try {
                    Path verPath = dbPath.resolve(String.valueOf(v));
                    FileUtils.deleteDirectory(verPath.toFile());
                    log.info("Successfully cleared expired version {}/{}", dbName, v);
                } catch (Exception e) {
                    log.error("Failed to clear expired version {}/{}", dbName, v, e);
                    // 清理失败，恢复状态
                    fixedVersionRecordLock.compareAndSetRecordValue(v, CLEAR_STATE, lastOpenTime);
                }
            }
        }
    }

}

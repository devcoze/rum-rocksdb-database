package com.tingyun.db.rocksdb;

import com.google.common.base.Preconditions;
import com.tingyun.db.lock.FixedVersionRecordLock;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

/**
 * RocksDB包装类
 * @param <K> key类型
 * @param <V> value类型
 */
public class RocksdbWrapper<K, V> {

    private RocksDB db;
    /**
     * dataDir，数据存储目录
     */
    private final String dataDir;
    /**
     * 数据库名称
     */
    private final String dbName;
    /**
     * 数据库的存储路径
     */
    private final Path dbPath;
    /**
     * 版本记录工具
     */
    private final FixedVersionRecordLock fixedVersionRecordLock;

    private final Serde<K> keySerializer;
    private final Serde<V> valueSerializer;

    static {
        RocksDB.loadLibrary();
    }

    public RocksdbWrapper(String dataDir, String dbName, Serde<K> kSerde, Serde<V> vSerde) throws IOException {
        Preconditions.checkNotNull(dataDir, "dataDir can not be null");
        this.dataDir = dataDir;
        Preconditions.checkNotNull(dbName, "dbName can not be null");
        this.dbName = dbName;
        Path path = Path.of(dataDir);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Path dbPath = path.resolve(dbName);
        if (!Files.exists(dbPath)) {
            Files.createDirectories(dbPath);
        }
        this.dbPath = dbPath;
        fixedVersionRecordLock = new FixedVersionRecordLock(dbPath);
        this.keySerializer = kSerde;
        this.valueSerializer = vSerde;
    }

    public String name() {
        return dbName;
    }

    public int version() {
        return fixedVersionRecordLock.maxVersion();
    }

    /**
     * 打开最新版本的数据库
     * @return 是否成功打开
     */
    public boolean open() {
        int i = fixedVersionRecordLock.maxVersion();
        if (i == 0) {
            return false;
        }
        Path verDBPath = dbPath.resolve(i+"");
        if (!Files.exists(verDBPath)) {
            return false;
        }
        try {
            db = RocksDB.open(verDBPath.toString());
            fixedVersionRecordLock.put(i, System.currentTimeMillis());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static final String _temp_version_prefix = "_temp_v%d_%d";

    public void write(Map<K, V> data) throws IOException, RocksDBException {

        // 数据库的版本号
        int expectedVer = fixedVersionRecordLock.maxVersion();
        int next = expectedVer + 1;

        // 把数据写入临时版本中
        String tempVersionPath = String.format(_temp_version_prefix, next, Instant.now().toEpochMilli());
        Path tempVerPath = dbPath.resolve(tempVersionPath);
        if (!Files.exists(tempVerPath)) {
            Files.createDirectories(tempVerPath);
        }
        RocksDB newRocksDB = RocksDB.open(tempVerPath.toString());
        for (Map.Entry<K, V> entry : data.entrySet()) {
            byte[] kBytes = keySerializer.serializer(entry.getKey());
            byte[] vBytes = valueSerializer.serializer(entry.getValue());
            newRocksDB.put(kBytes, vBytes);
        }

        // 写入完成后直接关闭
        newRocksDB.close();

        Path nextVersionPath = dbPath.resolve(String.valueOf(next));
        // 安全更新版本原数据，如果更新成功，重新命名，如果失败直接删除
        if (fixedVersionRecordLock.compareAndSetVersion(expectedVer, next)) {
            Files.move(tempVerPath, dbPath.resolve(nextVersionPath));
        } else {
            Files.deleteIfExists(tempVerPath);
        }

    }

    public V search(K key) throws RocksDBException {
        byte[] kBytes = keySerializer.serializer(key);
        byte[] v = db.get(kBytes);
        return valueSerializer.deserializer(v);
    }

    public void finishWrite() {

    }

    public void close() {

    }

}

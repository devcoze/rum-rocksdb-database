package com.tingyun.db.rocksdb;

import com.google.common.base.Preconditions;
import com.tingyun.db.lock.FixedVersionRecordLock;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * RocksDB包装类
 * @param <K> key类型
 * @param <V> value类型
 */
public class RocksdbWrapper<K, V> {

    private RocksDB db;
    private final String dataDir;
    private final String dbName;
    private final FixedVersionRecordLock lock;
    private final Path dbPath;

    static {
        RocksDB.loadLibrary();
    }

    public RocksdbWrapper(String dataDir, String dbName) throws IOException {
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
        lock = new FixedVersionRecordLock(dbPath);
    }

    public String name() {
        return dbName;
    }

    /**
     * 打开最新版本的数据库
     * @return 是否成功打开
     */
    public boolean open() {
        short i = lock.maxVersion();
        if (i == 0) {
            return false;
        }
        Path verDBPath = dbPath.resolve(i+"");
        if (!Files.exists(verDBPath)) {
            return false;
        }
        try {
            db = RocksDB.open(verDBPath.toString());
            lock.put(i, System.currentTimeMillis());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void write(Map<K, V> data) throws IOException, RocksDBException {
        FileLock fileLock = lock.tryLockMeta();
        if (fileLock == null) {
            throw new IOException("Failed to acquire meta lock");
        }
        short next = (short) (lock.maxVersion() + 1);
        Path verDBPath = dbPath.resolve(next+"");
        if (!Files.exists(verDBPath)) {
            Files.createDirectories(verDBPath);
        }
        RocksDB open = RocksDB.open(verDBPath.toString());
        for (Map.Entry<K, V> entry : data.entrySet()) {
            open.put(entry.getKey().toString().getBytes(), entry.getValue().toString().getBytes());
        }
        open.close();

        lock.put(next, System.currentTimeMillis());
        lock.updateMeta(next);
        lock.releaseLock(fileLock);
    }

    public V search(K key) throws RocksDBException {
        byte[] bytes = db.get(key.toString().getBytes());
        if (bytes == null) {
            return null;
        }
        String s = new String(bytes);
        return (V) s;
    }

    public void finishWrite() {

    }

    public void close() {

    }

}

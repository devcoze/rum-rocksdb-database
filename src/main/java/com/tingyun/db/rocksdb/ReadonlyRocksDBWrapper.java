package com.tingyun.db.rocksdb;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.tingyun.db.lock.FixedVersionRecordLock;
import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 只读的RocksDB包装类，支持多版本管理
 * @param <K> key类型
 * @param <V> value类型
 */
@Slf4j
public class ReadonlyRocksDBWrapper<K, V> {

    // 清理状态，-1表示正在被清理
    protected static final long CLEAR_STATE = -1L;
    // 默认的版本DB数量，超过该数量未被访问的版本DB将被关闭
    protected static final int DEFAULT_VERSION_DB_COUNT = 10;
    // 默认的版本过期时间，超过该时间未被访问则关闭，单位分钟
    protected static final int DEFAULT_VERSION_EXPIRE_TIME = 30; // 分钟
    // 默认的清理过期版本的时间，单位分钟，默认24小时
    protected static final int DEFAULT_VERSION_CLEAR_TIME = 24 * 60;

    /**
     * 数据库名称
     */
    protected final String dbName;
    /**
     * 数据库的存储路径
     */
    protected final Path dbPath;
    /**
     * 版本记录工具
     */
    protected final FixedVersionRecordLock fixedVersionRecordLock;
    /**
     * key的序列化
     */
    protected final RocksdbSerde<K> kRocksdbSerde;
    /**
     * value的序列化
     */
    protected final RocksdbSerde<V> vRocksdbSerde;
    /**
     * RocksDB缓存，key为版本号，value为RocksDB实例
     */
    protected final LoadingCache<Integer, Optional<RocksDB>> rocksDBCache;

    static {
        RocksDB.loadLibrary();
    }

    public ReadonlyRocksDBWrapper(String dataDir, String dbName, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        this(dataDir, dbName, DEFAULT_VERSION_DB_COUNT, DEFAULT_VERSION_EXPIRE_TIME, kRocksdbSerde, vRocksdbSerde);
    }

    /**
     * 构造函数
     * @param dataDir 数据存储目录
     * @param dbName 数据库名称
     * @param versionDbCount 版本数据库的数量，超过该数量未被访问的版本数据库将被关闭
     * @param versionExpireTime 版本数据库的过期时间，单位分钟，超过该时间未被访问的版本数据库将被关闭
     * @param kRocksdbSerde key的序列化工具
     * @param vRocksdbSerde value的序列化工具
     * @throws IOException 如果IO异常
     */
    public ReadonlyRocksDBWrapper(String dataDir, String dbName, int versionDbCount,
                          int versionExpireTime, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        Preconditions.checkNotNull(dataDir, "dataDir can not be null");
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
        int versionDBCount = versionDbCount > 0 ? versionDbCount : DEFAULT_VERSION_DB_COUNT;
        int versionExpireTime1 = versionExpireTime > 0 ? versionExpireTime : DEFAULT_VERSION_EXPIRE_TIME;
        Preconditions.checkNotNull(kRocksdbSerde, "kSerde can not be null");
        this.kRocksdbSerde = kRocksdbSerde;
        Preconditions.checkNotNull(vRocksdbSerde, "vSerde can not be null");
        this.vRocksdbSerde = vRocksdbSerde;
        this.rocksDBCache = CacheBuilder.newBuilder()
                // 设置缓存的最大容量
                .maximumSize(versionDBCount)
                // 设置缓存的过期时间, 指定时间内未被访问则过期，单位分钟
                .expireAfterAccess(Duration.ofMinutes(versionExpireTime1))
                // 设置缓存的移除监听器，关闭RocksDB实例
                .removalListener((RemovalListener<Integer, Optional<RocksDB>>)  listener -> listener.getValue().ifPresent(RocksDB::close))
                .build(new CacheLoader<>() {
                    @Override
                    public @NonNull Optional<RocksDB> load(@NonNull Integer key) {
                        return openVersion(key);
                    }
                });
    }

    /**
     * 数据库名称
     * @return 数据库名称
     */
    public String name() {
        return dbName;
    }

    /**
     * 当前数据库版本号
     * @return 版本号
     */
    public int version() {
        return fixedVersionRecordLock.latest();
    }

    /**
     * 打开最新版本的数据库
     * @return 是否成功打开
     */
    private Optional<RocksDB> openVersion(int latest) {
        // latest: 0 标识该数据当前为空
        if (latest <= 0) {
            return Optional.empty();
        }
        // latest 对应的版本不存在，无法打开
        Path verDBPath = dbPath.resolve(String.valueOf(latest));
        if (!Files.exists(verDBPath)) {
            log.warn("RocksDB version path not exists: {}", verDBPath);
            return Optional.empty();
        }
        try {
            // recordValue存储的是最后一次打开的时间戳
            long recordValue = fixedVersionRecordLock.recordValue(latest);
            if (recordValue <= CLEAR_STATE) {
                // 该版本正在被清理，无法打开
                log.warn("RocksDB version {} is being cleared, path: {}", latest, verDBPath);
                return Optional.empty();
            }
            RocksDB db = RocksDB.openReadOnly(verDBPath.toString());
            boolean state = fixedVersionRecordLock.compareAndSetRecordValue(latest, recordValue, Instant.now().toEpochMilli());
            log.trace("Update version record value, version: {}, oldValue: {}, newValue: {}, state: {}",
                    latest, recordValue, Instant.now().toEpochMilli(), state);
            return Optional.of(db);
        } catch (Exception e) {
            log.error("Failed to open RocksDB at path: {}", verDBPath, e);
        }
        return Optional.empty();
    }

    /**
     * 查询, 如果有多个key时，最好使用multiGetAsList批量查询
     * @param key key
     * @return value
     */
    public V get(K key) {
        if (key == null) {
            return null;
        }
        Optional<RocksDB> optional = rocksDBCache.getUnchecked(version());
        if (optional.isEmpty()) {
            return null;
        }
        RocksDB rocksDB = optional.get();
        try {
            byte[] kBytes = kRocksdbSerde.serializer(key);
            byte[] vBytes = rocksDB.get(kBytes);
            if (vBytes == null || vBytes.length == 0) {
                return null;
            }
            return vRocksdbSerde.deserializer(vBytes);
        } catch (Exception e) {
            log.error("Failed to search key: {}", key, e);
            return null;
        }
    }

    /**
     * 批量查询
     * @param keys key列表
     * @return value列表，顺序与key列表一致，如果某个key不存在则对应位置为null
     * @throws IllegalStateException 如果数据库未就绪
     */
    public List<V> multiGetAsList(List<K> keys) {
        Optional<RocksDB> optional = rocksDBCache.getUnchecked(version());
        if (optional.isEmpty()) {
            return List.of();
        }
        RocksDB rocksDB = optional.get();
        try {
            List<byte[]> kBytes = new ArrayList<>();
            for (K key : keys) {
                kBytes.add(kRocksdbSerde.serializer(key));
            }
            List<byte[]> vBytesList = rocksDB.multiGetAsList(kBytes);
            if (vBytesList == null || vBytesList.isEmpty()) {
                return List.of();
            }
            List<V> result = new ArrayList<>();
            for (byte[] vBytes : vBytesList) {
                if (vBytes == null) {
                    result.add(null);
                } else {
                    result.add(vRocksdbSerde.deserializer(vBytes));
                }
            }
            return result;
        } catch (Exception e) {
            log.error("Failed to search keys: {}", keys, e);
            return List.of();
        }
    }

    public void close() {
        try {
            rocksDBCache.invalidateAll();
            fixedVersionRecordLock.close();
        } catch (Exception e) {
            log.error("Failed to close RocksdbWrapper", e);
        }
    }



}

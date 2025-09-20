package com.tingyun.db;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.tingyun.db.rocksdb.RocksdbWrapper;
import lombok.NonNull;

import java.time.Duration;

/**
 * 多数据库管理器, 用于管理多个只读数据库实例, 不支持任何修改操作
 * @author chenlt
 */
public class ReadonlyMultiDBManager<K, V> extends MultiDBManager {

    // 最大的打开数据库个数
    private final int maxOpenDB;
    // 数据库最大空闲时间, 超过该时间未被访问则关闭, 单位: 分钟
    private final long maxIdleTime;
    // 数据库持有者缓存, key: dbName, value: Database实例
    private LoadingCache<String, Database<K, V>> dbHolderCache;

    public ReadonlyMultiDBManager(DBManagerConfig dbManagerConfig) {
        this(dbManagerConfig.getDataDir(), dbManagerConfig.getMaxOpenDB(), dbManagerConfig.getMaxIdleTime());
    }

    public ReadonlyMultiDBManager(String dataDir, int maxOpenDB, long maxIdleTime) {
        super(dataDir);
        this.maxOpenDB = maxOpenDB;
        this.maxIdleTime = maxIdleTime;
    }

    public void init() {
        this.dbHolderCache = CacheBuilder.newBuilder()
                .maximumSize(this.maxOpenDB)
                .expireAfterAccess(Duration.ofMinutes(this.maxIdleTime))
                .removalListener((RemovalListener<String, Database<K, V>>) notification -> {
                    Database<K, V> db = notification.getValue();
                    if (db != null) {
                        db.close();
                    }
                })
                .build(new DatabaseCacheLoader<>(dataDir));
    }

    private static class DatabaseCacheLoader<K, V> extends CacheLoader<String, Database<K, V>> {

        private final String dataDir;

        public DatabaseCacheLoader(String dataDir) {
            this.dataDir = dataDir;
        }

        @Override
        public Database<K, V> load(@NonNull String dbName) {
            // 这里可以根据dbName选择不同的数据库实现
            Database<K, V> db = new RocksdbWrapper<>();
            String dbPath = dataDir + "/" + dbName;
            db.open(dbPath);
            return db;
        }

    }

    public void close() {
        dbHolderCache.invalidateAll();
    }


}

package com.tingyun.db;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tingyun.db.rocksdb.RocksdbWrapper;
import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import com.tingyun.db.rocksdb.writer.RocksdbOnceWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 多数据库管理器
 * 管理多个只读的RocksDB实例
 * @param <K> key类型
 * @param <V> value类型
 */
@Slf4j
public class MultiDBManager<K, V> {
    /**
     * 同时打开的数据库个数
     */
    private int maxOpenDb;
    /**
     * 数据库最大空闲时间, 超过该时间未被访问则关闭, 单位: 分钟
     */
    private int maxIdleTime;
    /**
     * 数据存储目录
     */
    private final String dataDir;
    /**
     * 数据存储目录路径
     */
    private final Path dataPath;
    /**
     * 该数据存储目录下所有数据库总和, 单位: GB
     */
    private int maxDataSize; // GB

    private final RocksdbSerde<K> kRocksdbSerde;
    private final RocksdbSerde<V> vRocksdbSerde;
    private final LoadingCache<String, Optional<RocksdbWrapper<K, V>>> rocksdbWrapperLoadingCache;
    private final ScheduledExecutorService scheduledExecutorService;
    private MultiDBManagerConfig dbManagerConfig;


    public MultiDBManager(MultiDBManagerConfig config, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        this(config.getDataDir(), config.getMaxOpenDB(), config.getMaxIdleTime(), config.getMaxDBSize(),
                config.getCleanTaskDelay(), config.getCleanTaskPeriod(), kRocksdbSerde, vRocksdbSerde);
    }

    public MultiDBManager(String dataDir, int maxOpenDb, int maxIdleTime, int maxDataSize,
                          int cleanTaskDelayMinutes, int cleanTaskPeriodMinutes,
                          RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        this.dataDir = dataDir;
        Path path = Path.of(dataDir);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        // dataDir 必须是一个目录
        if (!Files.isDirectory(path)) {
            throw new IOException("Data directory is not a directory: " + dataDir);
        }
        this.maxOpenDb = maxOpenDb;
        this.maxIdleTime = maxIdleTime;
        this.dataPath = path;
        this.kRocksdbSerde = kRocksdbSerde;
        this.vRocksdbSerde = vRocksdbSerde;
        this.maxDataSize = maxDataSize;
        this.rocksdbWrapperLoadingCache = CacheBuilder.newBuilder()
                .expireAfterAccess(this.maxIdleTime, TimeUnit.MINUTES)
                .maximumSize(this.maxOpenDb)
                .removalListener(new RocksdbWrapperRemoveListener<K, V>())
                .build(new RocksdbWrapperLoader<>(dataDir, kRocksdbSerde, vRocksdbSerde, null));
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("MultiDBManager-Cleanup-Thread-%d")
                .setDaemon(true)
                .build();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(5, threadFactory);
        // 启动定时清理任务
        startCleanupTask(cleanTaskDelayMinutes, cleanTaskPeriodMinutes);
    }

    public RocksdbWrapper<K, V> getDB(String dbName) {
        if (StringUtils.isBlank(dbName)) {
            return null;
        }
        Optional<RocksdbWrapper<K, V>> rocksdbWrapper = rocksdbWrapperLoadingCache.getUnchecked(dbName);
        return rocksdbWrapper.orElse(null);
    }

    /**
     * RocksdbWrapper 加载器, 如果数据存在加载，否则创建一个空的
     * @param <K> key类型
     * @param <V> value类型
     */
    static class RocksdbWrapperLoader<K, V> extends CacheLoader<String, Optional<RocksdbWrapper<K, V>>> {
        private final String dataDir;
        private final RocksdbSerde<K> kRocksdbSerde;
        private final RocksdbSerde<V> vRocksdbSerde;
        private final MultiDBManagerConfig.DatabaseConfig databaseConfig;

        public RocksdbWrapperLoader(String dataDir, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde,
                                    MultiDBManagerConfig.DatabaseConfig databaseConfig) {
            this.dataDir = dataDir;
            this.kRocksdbSerde = kRocksdbSerde;
            this.vRocksdbSerde = vRocksdbSerde;
            this.databaseConfig = databaseConfig;
        }

        @Override
        public Optional<RocksdbWrapper<K, V>> load(String dbName) {
            try {
                RocksdbWrapper<K, V> rocksdbWrapper = new RocksdbWrapper<>(dataDir, dbName, kRocksdbSerde, vRocksdbSerde);
                return Optional.of(rocksdbWrapper);
            } catch (Exception e) {
                log.error("Failed to load RocksDB: {} in dir: {}", dbName, dataDir, e);
                return Optional.empty();
            }
        }

    }

    /**
     * RocksdbWrapper 移除监听器, 关闭RocksDB实例
     * @param <K> key类型
     * @param <V> value类型
     */
    static class RocksdbWrapperRemoveListener<K, V> implements RemovalListener<String, Optional<RocksdbWrapper<K, V>>> {

        @Override
        public void onRemoval(RemovalNotification<String, Optional<RocksdbWrapper<K, V>>> notification) {
            notification.getValue().ifPresent(wrapper -> {
                try {
                    wrapper.close();
                    log.info("Closed DB: {}", notification.getKey());
                } catch (Exception e) {
                    log.error("Failed to close DB: {}", notification.getKey(), e);
                }
            });
        }

    }

    /**
     * 创建数据库并写入数据
     * @param dbName 数据库名称
     * @param proxy 数据写入器
     * @throws RocksDBException 如果RocksDB异常
     * @throws IOException 如果IO异常
     */
    public void createAndFillData(String dbName, RocksdbOnceWriter<K, V> proxy) throws RocksDBException, IOException {
        if (proxy == null || StringUtils.isBlank(dbName)) {
            log.warn("RocksdbWriterProxy is null, dbName: {}", dbName);
            return;
        }
        Optional<RocksdbWrapper<K, V>> rocksdbWrapper = rocksdbWrapperLoadingCache.getUnchecked(dbName);
        if (rocksdbWrapper.isEmpty()) {
            RocksdbWrapper<K, V> newRocksdbWrapper = new RocksdbWrapper<>(dataDir, dbName, kRocksdbSerde, vRocksdbSerde);
            newRocksdbWrapper.writeOnceWithWriter(proxy);
            rocksdbWrapperLoadingCache.put(dbName, Optional.of(newRocksdbWrapper));
        } else {
            rocksdbWrapper.get().writeOnceWithWriter(proxy);
        }
    }

    /**
     * 启动定时清理任务, 定期清理未被访问的RocksdbWrapper实例
     * 每60分钟执行一次, 首次延迟10分钟执行
     */
    public void startCleanupTask(int initialDelayMinutes, int periodMinutes) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                rocksdbWrapperLoadingCache.asMap().values()
                        .forEach(opt -> opt.ifPresent(RocksdbWrapper::clear));
            } catch (Exception e) {
                log.error("Error during periodic cleanup", e);
            }
        }, initialDelayMinutes, periodMinutes, TimeUnit.MINUTES);
    }

    public void close() {
        try {
            rocksdbWrapperLoadingCache.invalidateAll();
            scheduledExecutorService.shutdownNow();
        } catch (Exception e) {
            log.error("Failed to close MultiDBManager", e);
        }
    }

}

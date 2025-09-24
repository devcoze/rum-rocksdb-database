package com.tingyun.db;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tingyun.db.lock.FixedVersionRecordLock;
import com.tingyun.db.rocksdb.RocksdbWrapper;
import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import com.tingyun.db.rocksdb.writer.RocksdbOnceWriter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.tingyun.db.rocksdb.ReadonlyRocksDBWrapper.CLEAR_STATE;

/**
 * 多数据库管理器
 * 管理多个只读的RocksDB实例
 * @param <K> key类型
 * @param <V> value类型
 */
@Slf4j
public class MultiDBManager<K, V> implements AutoCloseable {
    /**
     * 数据存储目录路径
     */
    private final Path dataPath;
    /**
     * 最大磁盘使用量，单位字节
     */
    private final long maxDiskUsageBytes;

    private final RocksdbSerde<K> kRocksdbSerde;
    private final RocksdbSerde<V> vRocksdbSerde;
    private final LoadingCache<String, Optional<RocksdbWrapper<K, V>>> rocksdbWrapperLoadingCache;
    private final ScheduledExecutorService scheduledExecutorService;
    private final MultiDBManagerConfig dbManagerConfig;

    public MultiDBManager(MultiDBManagerConfig config,
                          RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) throws IOException {
        Preconditions.checkNotNull(config, "MultiDBManagerConfig cannot be null");
        this.dbManagerConfig = config;
        String dataDir = config.getDataDir();
        Path path = Path.of(dataDir);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        // dataDir 必须是一个目录
        if (!Files.isDirectory(path)) {
            throw new IOException("Data directory is not a directory: " + dataDir);
        }
        int maxOpenDb = config.getMaxOpenDB();
        int maxIdleTime = config.getMaxIdleTime();
        this.dataPath = path;
        this.kRocksdbSerde = kRocksdbSerde;
        this.vRocksdbSerde = vRocksdbSerde;
        this.maxDiskUsageBytes = config.getMaxDiskUsageGB() * FileUtils.ONE_GB;
        this.rocksdbWrapperLoadingCache = CacheBuilder.newBuilder()
                .expireAfterAccess(maxIdleTime, TimeUnit.MINUTES)
                .maximumSize(maxOpenDb)
                .removalListener(new RocksdbWrapperRemoveListener<K, V>())
                .build(new RocksdbWrapperLoader<>(dbManagerConfig, kRocksdbSerde, vRocksdbSerde));
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("MultiDBManager-Cleanup-Thread-%d")
                .setDaemon(true)
                .build();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(5, threadFactory);
        // 启动定时清理任务
        startCleanupTask(config.getCleanTaskDelay(), config.getCleanTaskPeriod());
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
        private final RocksdbSerde<K> kRocksdbSerde;
        private final RocksdbSerde<V> vRocksdbSerde;
        private final MultiDBManagerConfig dbManagerConfig;

        public RocksdbWrapperLoader(MultiDBManagerConfig dbManagerConfig, RocksdbSerde<K> kRocksdbSerde, RocksdbSerde<V> vRocksdbSerde) {
            this.kRocksdbSerde = kRocksdbSerde;
            this.vRocksdbSerde = vRocksdbSerde;
            this.dbManagerConfig = dbManagerConfig;
        }

        @Override
        public @NonNull Optional<RocksdbWrapper<K, V>> load(@NonNull String dbName) {
            try {
                RocksdbWrapper<K, V> rocksdbWrapper = new RocksdbWrapper<>(dbManagerConfig, dbName, kRocksdbSerde, vRocksdbSerde);
                return Optional.of(rocksdbWrapper);
            } catch (Exception e) {
                log.error("Failed to load RocksDB: {} in dir: {}", dbName, dbManagerConfig.getDataDir(), e);
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
            RocksdbWrapper<K, V> newRocksdbWrapper = new RocksdbWrapper<>(dbManagerConfig, dbName, kRocksdbSerde, vRocksdbSerde);
            newRocksdbWrapper.writeOnceWithWriter(proxy);
            rocksdbWrapperLoadingCache.put(dbName, Optional.of(newRocksdbWrapper));
        } else {
            rocksdbWrapper.get().writeOnceWithWriter(proxy);
        }

        // 写入数据后，检查磁盘使用量
        enforceDiskQuota();
    }

    public void enforceDiskQuota() throws IOException {
        long totalSize = getDirectorySize(dataPath);
        if (totalSize <= maxDiskUsageBytes) {
            return;
        }
        log.warn("Disk usage exceeds limit: {} > {}, start cleaning old versions",
                FileUtils.byteCountToDisplaySize(totalSize), FileUtils.byteCountToDisplaySize(maxDiskUsageBytes));
        try (DirectoryStream<Path> paths =  Files.newDirectoryStream(dataPath)) {
            for (Path path : paths) {
                if (Files.isDirectory(path)) {
                    try (FixedVersionRecordLock lock = new FixedVersionRecordLock(path)) {
                        int latestVersion = lock.latest();
                        long now = Instant.now().toEpochMilli();
                        long expireTime = Duration.ofHours(24).toMillis();
                        for (int v = 1; v < latestVersion; v++) {
                            long lastOpenTime = lock.recordValue(v);
                            if (lastOpenTime <= CLEAR_STATE || now - lastOpenTime <= expireTime) {
                                continue;
                            }
                            if (lock.compareAndSetRecordValue(v, lastOpenTime, CLEAR_STATE)) {
                                Path verPath = path.resolve(String.valueOf(v));
                                try {
                                    FileUtils.deleteDirectory(verPath.toFile());
                                    log.info("Deleted expired version {} in DB {}", v, path.getFileName());
                                } catch (IOException e) {
                                    log.error("Failed to delete expired version {} in DB {}", v, path.getFileName(), e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to clean versions in DB: {}", path.getFileName().toString(), e);
                    }
                }
            }
        }
        log.info("Disk usage after cleanup: {}", FileUtils.byteCountToDisplaySize(totalSize));
    }

    /**
     * 获取目录大小
     */
    private long getDirectorySize(Path path) throws IOException {
        final AtomicLong size = new AtomicLong(0);
        try (Stream<Path> walk = Files.walk(path)) {
            walk.filter(Files::isRegularFile)
                    .forEach(p -> {
                        try {
                            size.addAndGet(Files.size(p));
                        } catch (IOException ignored) {}
                    });
        }
        return size.get();
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

    @Override
    public void close() {
        try {
            rocksdbWrapperLoadingCache.invalidateAll();
            scheduledExecutorService.shutdownNow();
        } catch (Exception e) {
            log.error("Failed to close MultiDBManager", e);
        }
    }

}

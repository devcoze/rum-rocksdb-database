package com.tingyun.db;

import com.tingyun.db.rocksdb.RocksdbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * 支持写入的多数据库管理器
 * @author chenlt
 */
@Slf4j
public class WriteMultiDBManager<K, V> extends MultiDBManager {

    // 最大存储量, 单位: GB
    private long maxDBSize;

    // 数据库状态文件, 用于标识数据库是否完整
    // /dataDir/dbName/.state
    private static final String _state = ".state";

    public WriteMultiDBManager(String dataDir) {
        super(dataDir);

    }


    public Database<K, V> createDB(String dbName) throws IOException {
        String dbPath = dataDir + "/" + dbName;
        Path path = Path.of(dbPath);
        // 判断目录是否存在
        if (!Files.exists(path)) {
            // 目录不存在, 创建临时数据库, 并从源文件导入数据，
            // 完成之后关闭并重命名，这样可以防止竞争
            doCreateTempDB(dbName, path);
        } else {

        }
        return null;
    }


    private void doCreateTempDB(String dbName, Path source) throws IOException {

        if (!Files.exists(source)) {
            log.error("source file is null");
            return;
        }


        // 1. 创建临时数据库目录
        Path path = tempDBName(dbName);
        log.debug("create temp db path: {}", path);
        Files.createDirectories(path);
        if (!Files.exists(path)) {
            log.error("create temp db path failed: {}", path);
            return;
        }

        // 2. 创建RocksDB实例
        Database<K, V> db = new RocksdbWrapper<>();



        long lineCount = 0;
        long batchSize = 1000;

        try (Stream<String> lines = Files.lines(source)) {

        } catch (Exception e) {

        }

    }



    private static final String _tempDbName = "_temp_%s_%d";

    // 生成临时数据库目录. /dataDir/_temp_dbName_timestamp
    private Path tempDBName(String dbName) {
        String tempDBName = String.format(_tempDbName, dbName, System.currentTimeMillis());
        return Path.of(dataDir, _tempDbName);
    }

    private File createOrGetState(String dbName) {
        // dataDir/dbName/.state
        String filePath = dataDir + "/" + dbName + "/" + _state;
        Path path = Paths.get(filePath);


    }

}

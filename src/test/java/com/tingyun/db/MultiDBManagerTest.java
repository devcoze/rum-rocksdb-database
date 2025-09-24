package com.tingyun.db;

import com.tingyun.db.rocksdb.MapWriteOnce;
import com.tingyun.db.rocksdb.serde.LongRocksdbSerde;
import com.tingyun.db.rocksdb.RocksdbWrapper;
import com.tingyun.db.rocksdb.serde.StringRocksdbSerde;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
public class MultiDBManagerTest {

    @Test
    public void write() throws IOException, RocksDBException {
        MultiDBManagerConfig config = new MultiDBManagerConfig();
        config.setDataDir("/Users/chenlt/tingyun/rocksdb_test");
        config.setMaxOpenDB(10);
        config.setMaxIdleTime(30);
        config.setMaxDiskUsageGB(50);
        MultiDBManager<Long, String> multiDBManager = new MultiDBManager<>(config, LongRocksdbSerde.INSTANCE, StringRocksdbSerde.INSTANCE);

        MapWriteOnce<Long, String> longStringMapWriteProxy = new MapWriteOnce<>(Map.of(1234L, "hello", 1235L, "world", 1236L, "!!!"));


        multiDBManager.createAndFillData("testdbccc", longStringMapWriteProxy);

    }

    @Test
    public void get() throws IOException, InterruptedException {
        MultiDBManagerConfig config = new MultiDBManagerConfig();
        config.setDataDir("/Users/chenlt/tingyun/rocksdb_test");
        config.setMaxOpenDB(10);
        config.setMaxIdleTime(30);
        config.setMaxDiskUsageGB(50);
        MultiDBManager<Long, String> multiDBManager = new MultiDBManager<>(config, LongRocksdbSerde.INSTANCE, StringRocksdbSerde.INSTANCE);
        RocksdbWrapper<Long, String> testdb = multiDBManager.getDB("testdbccc");
        if (testdb != null) {
            String v1 = testdb.get(1234L);
            String v2 = testdb.get(1235L);
            String v3 = testdb.get(1236L);
            log.info("v1: {}, v2: {}, v3: {}", v1, v2, v3);

            List<String> strings = testdb.multiGetAsList(List.of(1234L, 1235L, 1236L));
            log.info("strings: {}", strings);

        }

        Thread.currentThread().sleep(100000000000L);

    }



}
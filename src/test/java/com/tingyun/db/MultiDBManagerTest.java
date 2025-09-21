package com.tingyun.db;

import com.tingyun.db.rocksdb.LongSerde;
import com.tingyun.db.rocksdb.RocksdbWrapper;
import com.tingyun.db.rocksdb.StringSerde;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class MultiDBManagerTest {

    @Test
    public void init() throws IOException, RocksDBException {
        RocksdbWrapper<Long, String> stringLongRocksdbWrapper = new RocksdbWrapper<>("/Users/chenlt/tingyun/rocksdb_test", "testdb", LongSerde.INSTANCE, StringSerde.INSTANCE);
        // boolean open = stringLongRocksdbWrapper.open();

        stringLongRocksdbWrapper.write(Map.of(1235L, "sssssss", 5678L, "ssss"));

        boolean open = stringLongRocksdbWrapper.open();
        log.info("open: {}", open);
        String sss = stringLongRocksdbWrapper.search(1235L);
        log.info("sss: {}", sss);

        stringLongRocksdbWrapper.close();

    }



}
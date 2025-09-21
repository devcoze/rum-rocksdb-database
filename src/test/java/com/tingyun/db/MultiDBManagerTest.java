package com.tingyun.db;

import com.tingyun.db.rocksdb.RocksdbWrapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class MultiDBManagerTest {

    @Test
    public void init() throws IOException, RocksDBException {
        RocksdbWrapper<String, String> stringLongRocksdbWrapper = new RocksdbWrapper<>("/Users/chenlt/tingyun/rocksdb_test", "testdb");
        // boolean open = stringLongRocksdbWrapper.open();

        stringLongRocksdbWrapper.write(Map.of("sss", "sssssss", "aaa", "ssss"));

        boolean open = stringLongRocksdbWrapper.open();
        log.info("open: {}", open);
        String sss = stringLongRocksdbWrapper.search("sss");
        log.info("sss: {}", sss);

        stringLongRocksdbWrapper.close();

    }

}
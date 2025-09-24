package com.tingyun.db.rocksdb;

import com.tingyun.db.rocksdb.serde.LongRocksdbSerde;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class LongRocksdbSerdeTest {

    long key = 24L;

    @Test
    void serializer() {
        byte[] serializer = LongRocksdbSerde.INSTANCE.serializer(key);
        log.info("serializer: {}", serializer);
    }

    @Test
    void deserializer() {
        byte[] bytes = {24, 0, 0, 0, 0, 0, 0, 0};
        Long deserializer = LongRocksdbSerde.INSTANCE.deserializer(bytes);
        log.info("deserializer: {}", deserializer);
    }

}
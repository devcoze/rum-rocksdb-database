package com.tingyun.db.rocksdb;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class LongSerdeTest {

    long key = 24L;

    @Test
    void serializer() {
        byte[] serializer = LongSerde.INSTANCE.serializer(key);
        log.info("serializer: {}", serializer);
    }

    @Test
    void deserializer() {
        byte[] bytes = {24, 0, 0, 0, 0, 0, 0, 0};
        Long deserializer = LongSerde.INSTANCE.deserializer(bytes);
        log.info("deserializer: {}", deserializer);
    }

}
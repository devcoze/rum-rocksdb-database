package com.tingyun.db.rocksdb.serde;

/**
 * String 序列化器
 * @author chenlt
 */
public class StringRocksdbSerde implements RocksdbSerde<String> {

    public static final StringRocksdbSerde INSTANCE = new StringRocksdbSerde();

    @Override
    public byte[] serializer(String key) {
        return key.getBytes();
    }

    @Override
    public String deserializer(byte[] buf) {
        return new String(buf);
    }

}

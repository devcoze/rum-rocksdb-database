package com.tingyun.db.rocksdb;

public class StringSerde implements Serde<String> {

    public static final StringSerde INSTANCE = new StringSerde();

    @Override
    public byte[] serializer(String key) {
        return key.getBytes();
    }

    @Override
    public String deserializer(byte[] buf) {
        return new String(buf);
    }

}

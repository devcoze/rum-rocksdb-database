package com.tingyun.db.rocksdb;

public interface Serde<T> {

    byte[] serializer(T key);

    T deserializer(byte[] buf);

}

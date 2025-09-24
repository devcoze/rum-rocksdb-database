package com.tingyun.db.rocksdb.serde;

/**
 * 序列化/反序列化接口
 * @param <T>
 */
public interface RocksdbSerde<T> {

    /**
     * 序列化
     * @param key 待序列化对象
     * @return 字节数组
     */
    byte[] serializer(T key);

    /**
     * 反序列化
     * @param buf 字节数组
     * @return 反序列化对象
     */
    T deserializer(byte[] buf);

}

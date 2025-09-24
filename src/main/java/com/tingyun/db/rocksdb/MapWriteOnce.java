package com.tingyun.db.rocksdb;

import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import com.tingyun.db.rocksdb.writer.RocksdbOnceWriter;
import org.rocksdb.RocksDB;

import java.util.Map;

/**
 * Map写入代理示例
 * @param <K> key类型
 * @param <V> value类型
 */
public class MapWriteOnce<K, V> implements RocksdbOnceWriter<K, V> {

    private final Map<K, V> data;

    public MapWriteOnce(Map<K, V> data) {
        this.data = data;
    }

    @Override
    public boolean write(RocksDB rocksDB, RocksdbSerde<K> keyRocksdbSerde, RocksdbSerde<V> valueRocksdbSerde) {
        if (rocksDB == null) {
            return false;
        }
        if (data == null || data.isEmpty()) {
            return false;
        }
        for (Map.Entry<K, V> entry : data.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            byte[] keyBytes = keyRocksdbSerde.serializer(key);
            byte[] valueBytes = valueRocksdbSerde.serializer(value);
            try {
                rocksDB.put(keyBytes, valueBytes);
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

}

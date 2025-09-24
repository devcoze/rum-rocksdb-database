package com.tingyun.db.rocksdb.writer;

import com.tingyun.db.rocksdb.serde.RocksdbSerde;
import org.rocksdb.RocksDB;

/**
 * RocksDB 一次性写入器
 * 数据来源可以是内存, 文件, InputStream, 网络, 数据库等等
 * @author chenlt
 */
public interface RocksdbOnceWriter<K, V> {

    /**
     * 写入数据
     * @param rocksDB RocksDB 实例
     * @param keyRocksdbSerde key 序列化器
     * @param valueRocksdbSerde value 序列化器
     * @return 是否写入成功
     */
    boolean write(RocksDB rocksDB, RocksdbSerde<K> keyRocksdbSerde, RocksdbSerde<V> valueRocksdbSerde);

}

package com.tingyun.db.rocksdb;

import com.tingyun.db.Database;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.util.Map;

public class RocksdbWrapper<K, V> implements Database<K, V> {

    private RocksDB db;

    static {
        RocksDB.loadLibrary();
    }

    public RocksdbWrapper(String path) {

        Options options = new Options();
        options.setCreateIfMissing(true);

        // Initialize RocksDB instance
        try {
            db = RocksDB.open(options, path);
        } catch (Exception e) {
        }
    }

    @Override
    public String name() {
        return "rocksdb";
    }

    @Override
    public void open(String dbName) {
    }

    @Override
    public void write(Map<K, V> data) {

    }

    @Override
    public V search(K key) {
        return null;
    }

    @Override
    public void finishWrite() {

    }

    @Override
    public void close() {

    }

}

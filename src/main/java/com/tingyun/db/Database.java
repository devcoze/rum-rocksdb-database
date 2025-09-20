package com.tingyun.db;

import java.util.Map;

/**
 * Database interface for basic database operations.
 * @param <K>
 * @param <V>
 */
public interface Database<K, V> {

    String name();

    void open(String path);

    void write(Map<K, V> data);

    V search(K key);

    void finishWrite();

    void close();

}

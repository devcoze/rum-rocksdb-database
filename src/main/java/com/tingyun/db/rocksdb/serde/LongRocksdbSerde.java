package com.tingyun.db.rocksdb.serde;

import java.nio.ByteOrder;

/**
 * Long 序列化器
 * @author chenlt
 */
public class LongRocksdbSerde implements RocksdbSerde<Long> {

    public static final LongRocksdbSerde INSTANCE = new LongRocksdbSerde();

    /**
     * 字节序
     */
    private static final ByteOrder byteOrder;
    static {
        // 获取本地字节序
        byteOrder = ByteOrder.nativeOrder();
    }

    @Override
    public byte[] serializer(Long key) {
        byte[] result = new byte[8];
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
            // 小端：低位在前
            for (int i = 0; i < 8; i++) {
                result[i] = (byte) (key & 0xFF);
                key >>= 8;
            }
        } else {
            // 大端：高位在前
            for (int i = 7; i >= 0; i--) {
                result[i] = (byte) (key & 0xFF);
                key >>= 8;
            }
        }
        return result;
    }

    @Override
    public Long deserializer(byte[] bytes) {
        if (bytes.length > 8) {
            throw new IllegalArgumentException("字节数组不能超过8字节");
        }

        long value = 0;
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
            // 小端：低位在前
            for (int i = bytes.length - 1; i >= 0; i--) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        } else {
            // 大端：高位在前
            for (byte b : bytes) {
                value = (value << 8) | (b & 0xFF);
            }
        }
        return value;
    }

}

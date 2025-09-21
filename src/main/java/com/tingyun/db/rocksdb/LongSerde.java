package com.tingyun.db.rocksdb;

import java.nio.ByteOrder;

public class LongSerde implements Serde<Long> {

    private static final ByteOrder byteOrder;

    public static final LongSerde INSTANCE = new LongSerde();

    static {
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

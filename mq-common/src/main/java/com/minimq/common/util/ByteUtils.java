package com.minimq.common.util;

/**
 * 字节数组辅助工具，封装整型与字符串的固定编码读写逻辑。
 */
public final class ByteUtils {

    private ByteUtils() {
    }

    public static int toPositive(int n) {
        return n & 0x7fffffff;
    }

    @SuppressWarnings("fallthrough")
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        int length4 = length >> 2;

        for (int i = 0; i < length4; i++) {
            int i4 = i << 2;
            int k = readIntLittleEndian(data, i4);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int index = length4 << 2;
        switch (length - index) {
            case 3:
                h ^= (data[index + 2] & 0xff) << 16;
            case 2:
                h ^= (data[index + 1] & 0xff) << 8;
            case 1:
                h ^= data[index] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    private static int readIntLittleEndian(byte[] data, int offset) {
        return (data[offset] & 0xff)
                + ((data[offset + 1] & 0xff) << 8)
                + ((data[offset + 2] & 0xff) << 16)
                + ((data[offset + 3] & 0xff) << 24);
    }
}

package com.minimq.common.util;

public final class Murmur2Partitioner {

    private Murmur2Partitioner() {
    }

    public static int partition(byte[] key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive");
        }
        return ByteUtils.toPositive(ByteUtils.murmur2(key)) % numPartitions;
    }
}

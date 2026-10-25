package com.minimq.common.util;

/**
 * 与 Kafka 风格接近的 Murmur2 分区器，用 key 的哈希值稳定选择分区。
 */
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

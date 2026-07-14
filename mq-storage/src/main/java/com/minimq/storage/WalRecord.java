package com.minimq.storage;

import java.util.Arrays;

/**
 * One durable write intent stored in the WAL.
 */
public record WalRecord(
        WalRecordType type,
        String topic,
        int partitionId,
        long offset,
        byte[] key,
        byte[] value) {

    public WalRecord {
        if (type == null) {
            throw new IllegalArgumentException("WAL record type is required");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("WAL record topic is required");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("WAL record partitionId must be non-negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("WAL record offset must be non-negative");
        }
        key = copy(key);
        value = copy(value);
    }

    public static WalRecord dataAppend(String topic, int partitionId, long offset, byte[] key, byte[] value) {
        return new WalRecord(WalRecordType.DATA_APPEND, topic, partitionId, offset, key, value);
    }

    @Override
    public byte[] key() {
        return copy(key);
    }

    @Override
    public byte[] value() {
        return copy(value);
    }

    boolean payloadEquals(WalRecord other) {
        return other != null
                && type == other.type
                && topic.equals(other.topic)
                && partitionId == other.partitionId
                && offset == other.offset
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    private static byte[] copy(byte[] data) {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }
}

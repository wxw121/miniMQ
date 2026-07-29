package com.minimq.storage;

import com.minimq.common.validation.TopicNames;

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
        byte[] value,
        long deliverAtEpochMs) {

    public WalRecord {
        if (type == null) {
            throw new IllegalArgumentException("WAL record type is required");
        }
        TopicNames.requireValidReference(topic);
        if (partitionId < 0) {
            throw new IllegalArgumentException("WAL record partitionId must be non-negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("WAL record offset must be non-negative");
        }
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("WAL record delivery time must be non-negative");
        }
        key = copy(key);
        value = copy(value);
    }

    public WalRecord(
            WalRecordType type,
            String topic,
            int partitionId,
            long offset,
            byte[] key,
            byte[] value) {
        this(type, topic, partitionId, offset, key, value, 0L);
    }

    public static WalRecord dataAppend(String topic, int partitionId, long offset, byte[] key, byte[] value) {
        return new WalRecord(WalRecordType.DATA_APPEND, topic, partitionId, offset, key, value, 0L);
    }

    public static WalRecord dataAppend(
            String topic,
            int partitionId,
            long offset,
            byte[] key,
            byte[] value,
            long deliverAtEpochMs) {
        if (deliverAtEpochMs <= 0L) {
            return dataAppend(topic, partitionId, offset, key, value);
        }
        return scheduledDataAppend(topic, partitionId, offset, key, value, deliverAtEpochMs);
    }

    public static WalRecord scheduledDataAppend(
            String topic,
            int partitionId,
            long offset,
            byte[] key,
            byte[] value,
            long deliverAtEpochMs) {
        if (deliverAtEpochMs <= 0L) {
            throw new IllegalArgumentException("scheduled delivery time must be positive");
        }
        return new WalRecord(
                WalRecordType.SCHEDULED_DATA_APPEND,
                topic,
                partitionId,
                offset,
                key,
                value,
                deliverAtEpochMs);
    }

    public static WalRecord truncateBegin(String topic, int partitionId, long offset) {
        return new WalRecord(WalRecordType.TRUNCATE_BEGIN, topic, partitionId, offset, null, null, 0L);
    }

    public static WalRecord truncateCommit(String topic, int partitionId, long offset) {
        return new WalRecord(WalRecordType.TRUNCATE_COMMIT, topic, partitionId, offset, null, null, 0L);
    }

    /** Topic-wide marker uses partition id 0 and stores the tombstone version in offset. */
    public static WalRecord topicDeleteBegin(String topic, long tombstoneVersion) {
        return new WalRecord(WalRecordType.TOPIC_DELETE_BEGIN, topic, 0, tombstoneVersion, null, null, 0L);
    }

    /** Topic-wide marker uses partition id 0 and stores the tombstone version in offset. */
    public static WalRecord topicDeleteCommit(String topic, long tombstoneVersion) {
        return new WalRecord(WalRecordType.TOPIC_DELETE_COMMIT, topic, 0, tombstoneVersion, null, null, 0L);
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
                && deliverAtEpochMs == other.deliverAtEpochMs
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    private static byte[] copy(byte[] data) {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }
}

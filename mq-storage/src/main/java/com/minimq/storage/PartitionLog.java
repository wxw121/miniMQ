package com.minimq.storage;

import com.minimq.common.model.Record;

import java.util.List;

/**
 * 分区日志接口，定义追加、读取和 high watermark 查询能力。
 */
public interface PartitionLog extends AutoCloseable {
    long append(byte[] key, byte[] value);

    void appendAt(long offset, byte[] key, byte[] value);

    List<Record> read(long fromOffset, int maxBytes);

    long highWatermark();

    /** Removes all records whose offset is greater than or equal to {@code offset}. */
    void truncateTo(long offset);

    void flush();

    @Override
    void close();
}

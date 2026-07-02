package com.minimq.storage;

import com.minimq.common.model.Record;

import java.util.List;

/**
 * 分区日志接口，定义追加、读取和 high watermark 查询能力。
 */
public interface PartitionLog {
    long append(byte[] key, byte[] value);

    List<Record> read(long fromOffset, int maxBytes);

    long highWatermark();
}

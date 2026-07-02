package com.minimq.storage;

import com.minimq.common.model.Record;

import java.util.List;

public interface PartitionLog {
    long append(byte[] key, byte[] value);

    List<Record> read(long fromOffset, int maxBytes);

    long highWatermark();
}

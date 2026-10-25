package com.minimq.storage;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FetchPlan implements AutoCloseable {
    private final long highWatermark;
    private final int recordCount;
    private final int recordBytes;
    private final List<FileRegionSlice> slices;
    private final Runnable closeAction;
    private final AtomicBoolean closed = new AtomicBoolean();

    FetchPlan(long highWatermark, int recordCount, int recordBytes,
              List<FileRegionSlice> slices, Runnable closeAction) {
        if (highWatermark < 0 || recordCount < 0 || recordBytes < 0) {
            throw new IllegalArgumentException("fetch plan values must be non-negative");
        }
        this.highWatermark = highWatermark;
        this.recordCount = recordCount;
        this.recordBytes = recordBytes;
        this.slices = List.copyOf(slices);
        this.closeAction = closeAction;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public int recordCount() {
        return recordCount;
    }

    public int recordBytes() {
        return recordBytes;
    }

    public List<FileRegionSlice> slices() {
        return slices;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeAction.run();
        }
    }
}

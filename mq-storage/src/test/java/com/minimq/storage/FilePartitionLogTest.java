package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class FilePartitionLogTest {

    @Test
    void appendAndRead(@TempDir Path dir) throws IOException {
        FilePartitionLog log = new FilePartitionLog(dir.resolve("orders-0.log"));
        long off0 = log.append("k1".getBytes(), "v1".getBytes());
        long off1 = log.append(null, "v2".getBytes());
        assertEquals(0, off0);
        assertEquals(1, off1);
        assertEquals(2, log.highWatermark());

        List<Record> records = log.read(0, 1024 * 1024);
        assertEquals(2, records.size());
        assertEquals("v1", new String(records.get(0).value()));
        assertEquals("v2", new String(records.get(1).value()));
    }

    @Test
    void recoverAfterReopen(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        FilePartitionLog log1 = new FilePartitionLog(logPath);
        log1.append("k".getBytes(), "v1".getBytes());
        log1.append("k".getBytes(), "v2".getBytes());

        FilePartitionLog log2 = new FilePartitionLog(logPath);
        assertEquals(2, log2.highWatermark());
        assertEquals(2, log2.read(0, 1024 * 1024).size());
    }

    @Test
    void logManagerCachesPartitionLogs(@TempDir Path dir) {
        LogManager manager = new LogManager(dir);
        PartitionLog log = manager.getOrCreate("orders", 0);

        assertSame(log, manager.getOrCreate("orders", 0));
        assertEquals(0, log.append("k".getBytes(), "v".getBytes()));
        assertEquals(1, manager.getOrCreate("orders", 0).highWatermark());
    }
}

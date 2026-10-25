package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void closeReleasesPartitionLogAndRejectsFurtherUse(@TempDir Path dir) throws IOException {
        FilePartitionLog log = new FilePartitionLog(dir.resolve("orders-0.log"));
        log.append("k".getBytes(), "v1".getBytes());

        log.close();
        log.close();

        assertThrows(IllegalStateException.class, () -> log.append("k".getBytes(), "v2".getBytes()));
        assertThrows(IllegalStateException.class, () -> log.appendAt(1, "k".getBytes(), "v2".getBytes()));
        assertThrows(IllegalStateException.class, () -> log.read(0, 1024 * 1024));
        assertThrows(IllegalStateException.class, log::flush);
    }

    @Test
    void recoverTruncatesIncompleteTailHeader(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        FilePartitionLog log = new FilePartitionLog(logPath);
        log.append("k".getBytes(), "v1".getBytes());
        long validSize = Files.size(logPath);
        Files.write(logPath, new byte[] {0, 1, 2}, StandardOpenOption.APPEND);

        FilePartitionLog recovered = new FilePartitionLog(logPath);

        assertEquals(validSize, Files.size(logPath));
        assertEquals(1, recovered.highWatermark());
        assertEquals(1, recovered.read(0, 1024 * 1024).size());
        assertEquals(1, recovered.append("k".getBytes(), "v2".getBytes()));
    }

    @Test
    void recoverTruncatesIncompleteValuePayload(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        FilePartitionLog log = new FilePartitionLog(logPath);
        log.append("k".getBytes(), "v1".getBytes());
        long validSize = Files.size(logPath);
        ByteBuffer partialRecord = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + 1 + Integer.BYTES + 1);
        partialRecord.putLong(1);
        partialRecord.putInt(1);
        partialRecord.put((byte) 'k');
        partialRecord.putInt(2);
        partialRecord.put((byte) 'v');
        Files.write(logPath, partialRecord.array(), StandardOpenOption.APPEND);

        FilePartitionLog recovered = new FilePartitionLog(logPath);

        assertEquals(validSize, Files.size(logPath));
        assertEquals(1, recovered.highWatermark());
        assertEquals(1, recovered.read(0, 1024 * 1024).size());
    }

    @Test
    void recoverTruncatesIncompleteKeyPayload(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        FilePartitionLog log = new FilePartitionLog(logPath);
        log.append("k".getBytes(), "v1".getBytes());
        long validSize = Files.size(logPath);
        ByteBuffer partialRecord = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + 1);
        partialRecord.putLong(1);
        partialRecord.putInt(2);
        partialRecord.put((byte) 'k');
        Files.write(logPath, partialRecord.array(), StandardOpenOption.APPEND);

        FilePartitionLog recovered = new FilePartitionLog(logPath);

        assertEquals(validSize, Files.size(logPath));
        assertEquals(1, recovered.highWatermark());
        assertEquals(1, recovered.read(0, 1024 * 1024).size());
    }

    @Test
    void recoveryFailsOnInvalidNegativeLength(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        ByteBuffer corruptRecord = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        corruptRecord.putLong(0);
        corruptRecord.putInt(-2);
        Files.write(logPath, corruptRecord.array());

        IOException failure = assertThrows(IOException.class, () -> new FilePartitionLog(logPath));

        assertTrue(failure.getMessage().contains("invalid key length"));
    }

    @Test
    void recoveryFailsOnNonMonotonicOffset(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        ByteBuffer corruptRecord = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Integer.BYTES);
        corruptRecord.putLong(5);
        corruptRecord.putInt(-1);
        corruptRecord.putInt(-1);
        Files.write(logPath, corruptRecord.array());

        IOException failure = assertThrows(IOException.class, () -> new FilePartitionLog(logPath));

        assertTrue(failure.getMessage().contains("non-monotonic offset"));
    }

    @Test
    void recoveryFailsOnImpossiblePayloadSize(@TempDir Path dir) throws IOException {
        Path logPath = dir.resolve("orders-0.log");
        ByteBuffer corruptRecord = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        corruptRecord.putLong(0);
        corruptRecord.putInt(64 * 1024 * 1024 + 1);
        Files.write(logPath, corruptRecord.array());

        IOException failure = assertThrows(IOException.class, () -> new FilePartitionLog(logPath));

        assertTrue(failure.getMessage().contains("invalid key length"));
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

package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FilePartitionLogTruncationTest {
    @TempDir Path tempDir;

    @Test
    void truncatesTailAndContinuesFromRecoveryOffsetAfterReopen() throws Exception {
        Path path = tempDir.resolve("orders-0.log");
        try (FilePartitionLog log = new FilePartitionLog(path)) {
            for (int i = 0; i < 5; i++) {
                log.append(null, new byte[] {(byte) i});
            }

            log.truncateTo(3);

            assertEquals(3, log.highWatermark());
            assertEquals(List.of(0L, 1L, 2L), log.read(0, 1024).stream()
                    .map(com.minimq.common.model.Record::offset).toList());
            assertEquals(3, log.append(null, new byte[] {9}));
        }

        try (FilePartitionLog reopened = new FilePartitionLog(path)) {
            assertEquals(4, reopened.highWatermark());
            assertEquals(9, reopened.read(3, 1024).get(0).value()[0]);
        }
    }

    @Test
    void validatesNoOpRangeAndClosedState() throws Exception {
        FilePartitionLog log = new FilePartitionLog(tempDir.resolve("range.log"));
        log.append(null, new byte[] {1});
        log.truncateTo(1);
        assertEquals(1, log.highWatermark());
        assertThrows(IllegalArgumentException.class, () -> log.truncateTo(-1));
        assertThrows(IllegalArgumentException.class, () -> log.truncateTo(2));
        log.close();
        assertThrows(IllegalStateException.class, () -> log.truncateTo(0));
    }
}

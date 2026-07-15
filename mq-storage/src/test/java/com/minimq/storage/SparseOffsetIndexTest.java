package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseOffsetIndexTest {

    @Test
    void findsFloorPositionAfterReopen(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("00000000000000000000.index");
        try (SparseOffsetIndex index = SparseOffsetIndex.open(path)) {
            index.append(0, 0);
            index.append(64, 4096);
            index.append(128, 8192);
            index.flush();
        }

        try (SparseOffsetIndex reopened = SparseOffsetIndex.open(path)) {
            assertEquals(0, reopened.floorPosition(63));
            assertEquals(4096, reopened.floorPosition(80));
            assertEquals(8192, reopened.floorPosition(128));
        }
    }

    @Test
    void rejectsTruncatedIndex(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("broken.index");
        Files.write(path, new byte[] {1, 2, 3});

        IOException failure = assertThrows(IOException.class, () -> SparseOffsetIndex.open(path));

        assertTrue(failure.getMessage().contains("truncated"));
    }

    @Test
    void rejectsNonIncreasingIndexOffsets(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("unordered.index");
        ByteBuffer entries = ByteBuffer.allocate(Long.BYTES * 4);
        entries.putLong(10).putLong(100);
        entries.putLong(10).putLong(200);
        Files.write(path, entries.array());

        IOException failure = assertThrows(IOException.class, () -> SparseOffsetIndex.open(path));

        assertTrue(failure.getMessage().contains("non-increasing offset"));
    }
}

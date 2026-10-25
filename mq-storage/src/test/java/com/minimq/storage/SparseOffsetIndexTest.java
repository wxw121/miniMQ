package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    @Test
    void replacesEntriesAndRejectsInvalidAppendOrUseAfterClose(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("replace.index");
        SparseOffsetIndex index = SparseOffsetIndex.open(path);
        index.append(10, 100);

        index.replace(List.of(
                new SparseOffsetIndex.IndexEntry(1, 0),
                new SparseOffsetIndex.IndexEntry(4, 40)));

        assertEquals(40, index.floorPosition(8));
        assertThrows(IllegalArgumentException.class, () -> index.append(4, 80));
        assertThrows(IllegalArgumentException.class, () -> index.append(5, -1));
        assertThrows(IllegalArgumentException.class, () -> index.append(5, 40));

        index.close();
        index.close();

        assertThrows(IllegalStateException.class, () -> index.floorPosition(1));
        assertThrows(IllegalStateException.class, index::flush);
    }

    @Test
    void rejectsNegativeAndNonIncreasingFilePositionsOnLoad(@TempDir Path dir) throws Exception {
        Path negative = dir.resolve("negative.index");
        ByteBuffer negativeEntries = ByteBuffer.allocate(Long.BYTES * 2);
        negativeEntries.putLong(-1).putLong(0);
        Files.write(negative, negativeEntries.array());

        IOException negativeFailure = assertThrows(IOException.class, () -> SparseOffsetIndex.open(negative));
        assertTrue(negativeFailure.getMessage().contains("negative"));

        Path unorderedPosition = dir.resolve("unordered-position.index");
        ByteBuffer unorderedEntries = ByteBuffer.allocate(Long.BYTES * 4);
        unorderedEntries.putLong(1).putLong(100);
        unorderedEntries.putLong(2).putLong(100);
        Files.write(unorderedPosition, unorderedEntries.array());

        IOException unorderedFailure = assertThrows(IOException.class,
                () -> SparseOffsetIndex.open(unorderedPosition));
        assertTrue(unorderedFailure.getMessage().contains("non-increasing file position"));
    }

    @Test
    void rejectsInvalidReplacementEntries(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("invalid-replacement.index");
        try (SparseOffsetIndex index = SparseOffsetIndex.open(path)) {
            List<SparseOffsetIndex.IndexEntry> nullReplacement = new java.util.ArrayList<>();
            nullReplacement.add(new SparseOffsetIndex.IndexEntry(1, 0));
            nullReplacement.add(null);

            assertThrows(IllegalArgumentException.class, () -> index.replace(nullReplacement));
            assertThrows(IllegalArgumentException.class, () -> index.replace(List.of(
                    new SparseOffsetIndex.IndexEntry(1, 0),
                    new SparseOffsetIndex.IndexEntry(2, 0))));
        }
    }

    @Test
    void replacePublishesAtomicallyAndKeepsChannelBoundForLaterAppends(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("live.index");
        Files.write(path.resolveSibling("live.index.tmp"), new byte[] {1, 2, 3});

        try (SparseOffsetIndex index = SparseOffsetIndex.open(path)) {
            assertFalse(Files.exists(path.resolveSibling("live.index.tmp")));
            index.append(10, 100);
            index.replace(List.of(
                    new SparseOffsetIndex.IndexEntry(1, 0),
                    new SparseOffsetIndex.IndexEntry(4, 40)));
            index.append(8, 80);
            index.flush();
            assertEquals(40, index.floorPosition(7));
            assertEquals(80, index.floorPosition(8));
        }

        try (SparseOffsetIndex reopened = SparseOffsetIndex.open(path)) {
            assertEquals(0, reopened.floorPosition(1));
            assertEquals(40, reopened.floorPosition(4));
            assertEquals(80, reopened.floorPosition(8));
        }
    }

    @Test
    void replaceKeepsOriginalIndexWhenTempPathCannotBeReplaced(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("blocked.index");
        Path tempFile = path.resolveSibling("blocked.index.tmp");
        try (SparseOffsetIndex index = SparseOffsetIndex.open(path)) {
            index.append(10, 100);
            index.flush();
            Files.createDirectory(tempFile);
            Files.write(tempFile.resolve("blocker"), new byte[] {1});

            IllegalStateException failure = assertThrows(
                    IllegalStateException.class,
                    () -> index.replace(List.of(new SparseOffsetIndex.IndexEntry(1, 0))));
            assertTrue(failure.getMessage().contains("Failed to replace sparse index"));
            assertEquals(100, index.floorPosition(10));
            index.append(20, 200);
            index.flush();
        }

        Files.delete(tempFile.resolve("blocker"));
        Files.delete(tempFile);

        try (SparseOffsetIndex reopened = SparseOffsetIndex.open(path)) {
            assertEquals(100, reopened.floorPosition(10));
            assertEquals(200, reopened.floorPosition(20));
        }
    }
}

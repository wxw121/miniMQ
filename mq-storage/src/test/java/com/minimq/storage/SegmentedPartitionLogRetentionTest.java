package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedPartitionLogRetentionTest {

    @Test
    void retentionPublishesCatalogBeforeDeletingOldSegmentFiles(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            log.append("k0".getBytes(), "v0".getBytes());
            log.append("k1".getBytes(), "v1".getBytes());
            log.append("k2".getBytes(), "v2".getBytes());

            log.applyRetention(RetentionPolicy.of(1, 0), System.currentTimeMillis() + 10_000);

            assertFalse(Files.exists(dir.resolve("00000000000000000000.log")));
            assertFalse(Files.exists(dir.resolve("00000000000000000000.index")));
            assertFalse(Files.exists(dir.resolve("00000000000000000001.log")));
            assertFalse(Files.exists(dir.resolve("00000000000000000001.index")));
            assertEquals(List.of(2L), log.read(0, 1_024).stream().map(Record::offset).toList());

            SegmentCatalog catalog = new SegmentCatalogStore(dir).load().orElseThrow();
            assertEquals(2, catalog.activeBaseOffset());
            assertEquals(2, catalog.logStartOffset());
            assertEquals(List.of(2L), catalog.segments().stream().map(SegmentDescriptor::baseOffset).toList());
        }
    }

    @Test
    void protectedSegmentLookupReadsScheduledOffsetsLinearly(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(
                dir, new SegmentedLogConfig(1, 1))) {
            for (int offset = 0; offset < 128; offset++) {
                log.append(("k" + offset).getBytes(), ("v" + offset).getBytes());
            }
            CountingOffsets offsets = new CountingOffsets(128);
            Method lookup = SegmentedPartitionLog.class.getDeclaredMethod(
                    "protectedSegmentBases", List.class);
            lookup.setAccessible(true);

            @SuppressWarnings("unchecked")
            Set<Long> protectedBases = (Set<Long>) lookup.invoke(log, offsets);

            assertEquals(128, protectedBases.size());
            assertTrue(offsets.readCount() < 512,
                    "protected offset reads were not linear: " + offsets.readCount());
        }
    }

    private static final class CountingOffsets extends AbstractList<Long> {
        private final int size;
        private int reads;

        private CountingOffsets(int size) {
            this.size = size;
        }

        @Override
        public Long get(int index) {
            reads++;
            return (long) index;
        }

        @Override
        public int size() {
            return size;
        }

        private int readCount() {
            return reads;
        }
    }
}

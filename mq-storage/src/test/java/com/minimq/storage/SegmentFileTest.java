package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentFileTest {

    @Test
    void createsEmptySegmentAndRejectsUseAfterClose(@TempDir Path dir) throws Exception {
        SegmentDescriptor descriptor = new SegmentDescriptor(5, 100);
        SegmentFile segment = SegmentFile.create(dir, descriptor, new SegmentedLogConfig(1_024, 1));

        assertEquals(5, segment.baseOffset());
        assertEquals(-1, segment.firstOffset());
        assertEquals(-1, segment.lastOffset());
        assertEquals(5, segment.nextOffset());
        assertEquals(0, segment.size());
        assertFalse(segment.hasRecords());
        assertEquals(List.of(), segment.read(5, 0));
        segment.truncateTo(5);
        assertThrows(IllegalArgumentException.class, () -> segment.truncateTo(4));

        segment.flush();
        segment.close();
        segment.close();

        assertThrows(IllegalStateException.class, segment::size);
        assertThrows(IllegalStateException.class, segment::flush);
        assertThrows(IOException.class, () -> segment.read(5, 1));
        assertThrows(IOException.class, () -> segment.append(5, null, "v".getBytes()));
    }

    @Test
    void readsWithinBudgetAndReportsMissingRecords(@TempDir Path dir) throws Exception {
        byte[] firstKey = "k0".getBytes();
        byte[] firstValue = "v0".getBytes();
        byte[] secondKey = "k1".getBytes();
        byte[] secondValue = "value-that-exceeds-first-budget".getBytes();

        try (SegmentFile segment = SegmentFile.create(
                dir,
                new SegmentDescriptor(10, 100),
                new SegmentedLogConfig(1_024, 1))) {
            segment.append(10, firstKey, firstValue);
            segment.append(11, secondKey, secondValue);

            int firstRecordBudget = PartitionLogRecordCodec.encodedSize(10, firstKey, firstValue);
            assertEquals(List.of(10L), segment.read(10, firstRecordBudget).stream().map(Record::offset).toList());
            assertNull(segment.recordAt(9));
            assertNull(segment.recordAt(12));
            assertThrows(IOException.class, () -> segment.append(11, "k".getBytes(), "v".getBytes()));
        }
    }

    @Test
    void planReadHonorsHighWatermarkStartOffsetAndBudgetBoundaries(@TempDir Path dir) throws Exception {
        byte[] key = "k".getBytes();
        byte[] firstValue = "first".getBytes();
        byte[] secondValue = "second-value".getBytes();
        byte[] thirdValue = "third-value".getBytes();
        try (SegmentFile segment = SegmentFile.create(
                dir,
                new SegmentDescriptor(0, 100),
                new SegmentedLogConfig(1_024, 10))) {
            segment.append(0, key, firstValue);
            segment.append(1, key, secondValue);
            segment.append(2, key, thirdValue);

            int firstRecordBytes = PartitionLogRecordCodec.encodedSize(0, key, firstValue);
            SegmentFile.PlannedRead zeroBudget = segment.planRead(0, 0, 10, true);
            assertEquals(0, zeroBudget.recordCount());
            assertNull(zeroBudget.slice());

            SegmentFile.PlannedRead oversizedDisallowed =
                    segment.planRead(0, firstRecordBytes - 1, 10, false);
            assertEquals(0, oversizedDisallowed.recordCount());
            assertNull(oversizedDisallowed.slice());

            SegmentFile.PlannedRead boundedByHighWatermark =
                    segment.planRead(0, 10_000, 1, true);
            assertEquals(1, boundedByHighWatermark.recordCount());

            SegmentFile.PlannedRead startsAfterIndexedFloor =
                    segment.planRead(1, 10_000, 10, true);
            assertEquals(2, startsAfterIndexedFloor.recordCount());

            SegmentFile.PlannedRead boundedByBudget =
                    segment.planRead(0, firstRecordBytes, 10, true);
            assertEquals(1, boundedByBudget.recordCount());
        }
    }

    @Test
    void appendWaitsForSegmentMonitorHeldByReadPlanning(@TempDir Path dir) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (SegmentFile segment = SegmentFile.create(
                dir,
                new SegmentDescriptor(0, 100),
                new SegmentedLogConfig(1_024, 1))) {
            segment.append(0, "k0".getBytes(), "v0".getBytes());
            CountDownLatch appendAttempted = new CountDownLatch(1);

            Future<Void> append = executor.submit(() -> {
                appendAttempted.countDown();
                segment.append(1, "k1".getBytes(), "v1".getBytes());
                return null;
            });

            synchronized (segment) {
                assertTrue(appendAttempted.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> append.get(150, TimeUnit.MILLISECONDS));
                assertEquals(1, segment.planRead(0, 1_024, 10, true).recordCount());
            }

            append.get(1, TimeUnit.SECONDS);
            assertEquals(List.of(0L, 1L), segment.read(0, 1_024).stream().map(Record::offset).toList());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsMissingAndNonIncreasingSegmentLog(@TempDir Path dir) throws Exception {
        SegmentDescriptor descriptor = new SegmentDescriptor(0, 100);
        SegmentedLogConfig config = new SegmentedLogConfig(1_024, 1);

        IOException missing = assertThrows(IOException.class,
                () -> SegmentFile.open(dir, descriptor, true, config));
        assertTrue(missing.getMessage().contains("Missing segment log"));

        try (FileChannel channel = FileChannel.open(SegmentFile.logPath(dir, 0), CREATE, READ, WRITE)) {
            PartitionLogRecordCodec.write(channel, 0, "k0".getBytes(), "v0".getBytes());
            PartitionLogRecordCodec.write(channel, 0, "k1".getBytes(), "v1".getBytes());
        }

        IOException corrupt = assertThrows(IOException.class,
                () -> SegmentFile.open(dir, descriptor, true, config));
        assertTrue(corrupt.getMessage().contains("non-increasing offset"));
    }

    @Test
    void closesCreatedLogWhenIndexOpenFails(@TempDir Path dir) throws Exception {
        Files.write(SegmentFile.indexPath(dir, 0), new byte[] {1, 2, 3});

        assertThrows(IOException.class,
                () -> SegmentFile.create(dir, new SegmentDescriptor(0, 100), new SegmentedLogConfig(1_024, 1)));
    }

    @Test
    void rebuildsCorruptSparseIndexOnOpen(@TempDir Path dir) throws Exception {
        SegmentDescriptor descriptor = new SegmentDescriptor(0, 100);
        SegmentedLogConfig config = new SegmentedLogConfig(1_024, 1);
        try (SegmentFile segment = SegmentFile.create(dir, descriptor, config)) {
            segment.append(0, "k0".getBytes(), "v0".getBytes());
            segment.append(1, "k1".getBytes(), "v1".getBytes());
        }
        Files.write(SegmentFile.indexPath(dir, 0), new byte[] {1, 2, 3});

        try (SegmentFile recovered = SegmentFile.open(dir, descriptor, true, config)) {
            assertEquals(List.of(0L, 1L), recovered.read(0, 1_024).stream().map(Record::offset).toList());
        }
    }
}

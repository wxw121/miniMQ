package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedPartitionLogSnapshotTest {

    @Test
    void readUsesSnapshotInsteadOfPartitionWideMonitor() throws Exception {
        int modifiers = SegmentedPartitionLog.class
                .getDeclaredMethod("read", long.class, int.class)
                .getModifiers();

        assertFalse(Modifier.isSynchronized(modifiers));
    }

    @Test
    void concurrentReadsDoNotObserveDeletedRetentionFiles(@TempDir Path dir) throws Exception {
        try (SegmentedPartitionLog log = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            for (int i = 0; i < 100; i++) {
                log.append(("k" + i).getBytes(), ("v" + i).getBytes());
            }

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicReference<Throwable> readerFailure = new AtomicReference<>();
            CountDownLatch readersStarted = new CountDownLatch(4);
            List<Thread> readers = IntStream.range(0, 4)
                    .mapToObj(index -> new Thread(() -> {
                        readersStarted.countDown();
                        while (!stop.get() && readerFailure.get() == null) {
                            try {
                                log.read(0, 64 * 1024);
                            } catch (Throwable failure) {
                                readerFailure.compareAndSet(null, failure);
                            }
                        }
                    }))
                    .toList();
            readers.forEach(Thread::start);
            assertTrue(readersStarted.await(5, TimeUnit.SECONDS));

            for (int i = 0; i < 20; i++) {
                log.applyRetention(RetentionPolicy.of(1, 0), System.currentTimeMillis() + 10_000 + i);
            }

            stop.set(true);
            for (Thread reader : readers) {
                reader.join(5_000);
            }

            assertNull(readerFailure.get());
            log.applyRetention(RetentionPolicy.of(1, 0), System.currentTimeMillis() + 20_000);
            assertEquals(List.of(99L), log.read(0, 64 * 1024).stream().map(Record::offset).toList());
        }
    }

    @Test
    void startupCleansInterruptedMaintenanceArtifacts(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        Files.write(dir.resolve("00000000000000000000.cleaned.log"), new byte[] {1});
        Files.write(dir.resolve("00000000000000000000.cleaned.index"), new byte[] {1});
        Files.write(dir.resolve("00000000000000000000.obsolete.log"), new byte[] {1});
        Files.write(dir.resolve("00000000000000000000.obsolete.index"), new byte[] {1});
        Files.write(dir.resolve("catalog.json.tmp"), new byte[] {1});

        try (SegmentedPartitionLog ignored = new SegmentedPartitionLog(dir, new SegmentedLogConfig(1, 1))) {
            assertFalse(Files.exists(dir.resolve("00000000000000000000.cleaned.log")));
            assertFalse(Files.exists(dir.resolve("00000000000000000000.cleaned.index")));
            assertFalse(Files.exists(dir.resolve("00000000000000000000.obsolete.log")));
            assertFalse(Files.exists(dir.resolve("00000000000000000000.obsolete.index")));
            assertFalse(Files.exists(dir.resolve("catalog.json.tmp")));
        }
    }
}

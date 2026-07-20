package com.minimq.broker.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class MetadataWatchServiceTest {
    @Test
    void installsOnlyMonotonicallyIncreasingRevisionsOnInstallerThread() throws Exception {
        FakeRepository repository = new FakeRepository(version(1));
        FakeSource source = new FakeSource();
        List<Long> installed = new CopyOnWriteArrayList<>();
        List<String> threads = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);
        try (MetadataWatchService service = new MetadataWatchService(repository, source, value -> {
            installed.add(value.revision());
            threads.add(Thread.currentThread().getName());
            latch.countDown();
        })) {
            service.start();
            source.awaitStarted();
            source.emit(version(3));
            source.emit(version(2));
            source.emit(version(4));
            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals(List.of(1L, 3L, 4L), installed);
            assertTrue(threads.stream().allMatch(name -> name.equals("metadata-installer")));
        }
    }

    @Test
    void fullReadsAndRebuildsWatchAfterFailure() throws Exception {
        FakeRepository repository = new FakeRepository(version(5));
        FakeSource source = new FakeSource();
        CountDownLatch latch = new CountDownLatch(2);
        List<Long> installed = new CopyOnWriteArrayList<>();
        try (MetadataWatchService service = new MetadataWatchService(repository, source, value -> {
            installed.add(value.revision());
            latch.countDown();
        })) {
            service.start();
            source.awaitStarted();
            repository.current = version(8);
            source.fail();
            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals(List.of(5L, 8L), installed);
            assertEquals(2, source.starts);
            assertEquals(9, source.revision);
        }
    }

    private static VersionedClusterMetadata version(long revision) {
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.metadataVersion = revision;
        return new VersionedClusterMetadata(metadata, revision);
    }

    private static final class FakeRepository implements ClusterMetadataRepository {
        volatile VersionedClusterMetadata current;
        FakeRepository(VersionedClusterMetadata current) { this.current = current; }
        @Override public VersionedClusterMetadata read() { return current; }
        @Override public UpdateResult initialize(ClusterMetadata metadata) { throw new UnsupportedOperationException(); }
        @Override public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) { throw new UnsupportedOperationException(); }
    }

    private static final class FakeSource implements MetadataWatchService.Source {
        volatile Consumer<VersionedClusterMetadata> event;
        volatile Runnable failure;
        volatile long revision;
        volatile int starts;
        final CountDownLatch started = new CountDownLatch(1);
        @Override public MetadataWatchService.Subscription watch(long startRevision,
                Consumer<VersionedClusterMetadata> event, Runnable failure) {
            this.revision = startRevision;
            this.event = event;
            this.failure = failure;
            starts++;
            started.countDown();
            return () -> { };
        }
        void awaitStarted() throws InterruptedException { assertTrue(started.await(3, TimeUnit.SECONDS)); }
        void emit(VersionedClusterMetadata value) { event.accept(value); }
        void fail() { failure.run(); }
    }
}

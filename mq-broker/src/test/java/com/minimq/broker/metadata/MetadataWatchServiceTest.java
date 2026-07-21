package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

    @Test
    void etcdSourceParsesEventsAndClosesWatcher() throws Exception {
        FakeRepository repository = new FakeRepository(version(1));
        Watch watch = mock(Watch.class);
        Watch.Watcher watcher = mock(Watch.Watcher.class);
        java.util.concurrent.atomic.AtomicReference<Consumer<WatchResponse>> events =
                new java.util.concurrent.atomic.AtomicReference<>();
        CountDownLatch watching = new CountDownLatch(1);
        doAnswer(invocation -> {
            events.set(invocation.getArgument(2));
            watching.countDown();
            return watcher;
        }).when(watch).watch(any(ByteSequence.class), any(WatchOption.class),
                any(Consumer.class), any(Consumer.class), any(Runnable.class));
        CountDownLatch installed = new CountDownLatch(2);
        try (MetadataWatchService service = new MetadataWatchService(
                repository, watch, "demo", ignored -> installed.countDown())) {
            service.start();
            assertTrue(watching.await(3, TimeUnit.SECONDS));
            ClusterMetadata metadata = new ClusterMetadata();
            metadata.metadataVersion = 2;
            KeyValue keyValue = mock(KeyValue.class);
            when(keyValue.getValue()).thenReturn(ByteSequence.from(
                    new ObjectMapper().writeValueAsBytes(metadata)));
            when(keyValue.getModRevision()).thenReturn(2L);
            WatchEvent event = mock(WatchEvent.class);
            when(event.getKeyValue()).thenReturn(keyValue);
            WatchResponse response = mock(WatchResponse.class);
            when(response.getEvents()).thenReturn(List.of(event));

            events.get().accept(response);

            assertTrue(installed.await(3, TimeUnit.SECONDS));
            assertEquals(2, service.installedRevision());
        }
        verify(watcher).close();
    }

    @Test
    void etcdSourceReportsInvalidSnapshotOnlyOnce() throws Exception {
        FakeRepository repository = new FakeRepository(version(1));
        Watch watch = mock(Watch.class);
        Watch.Watcher watcher = mock(Watch.Watcher.class);
        java.util.concurrent.atomic.AtomicReference<Consumer<WatchResponse>> events =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Consumer<Throwable>> errors =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Runnable> completed =
                new java.util.concurrent.atomic.AtomicReference<>();
        CountDownLatch watching = new CountDownLatch(2);
        doAnswer(invocation -> {
            events.set(invocation.getArgument(2));
            errors.set(invocation.getArgument(3));
            completed.set(invocation.getArgument(4));
            watching.countDown();
            return watcher;
        }).when(watch).watch(any(ByteSequence.class), any(WatchOption.class),
                any(Consumer.class), any(Consumer.class), any(Runnable.class));
        try (MetadataWatchService service = new MetadataWatchService(
                repository, watch, "demo", ignored -> { })) {
            service.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (watching.getCount() == 2 && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertEquals(1, watching.getCount());
            KeyValue keyValue = mock(KeyValue.class);
            when(keyValue.getValue()).thenReturn(ByteSequence.from(new byte[] {1}));
            WatchEvent event = mock(WatchEvent.class);
            when(event.getKeyValue()).thenReturn(keyValue);
            WatchResponse response = mock(WatchResponse.class);
            when(response.getEvents()).thenReturn(List.of(event));

            events.get().accept(response);
            errors.get().accept(new RuntimeException("closed"));
            completed.get().run();

            assertTrue(watching.await(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeForcesExecutorShutdownAfterTimeoutAndIsIdempotent() throws Exception {
        ExecutorService executor = mock(ExecutorService.class);
        when(executor.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(false);
        MetadataWatchService service = new MetadataWatchService(
                new FakeRepository(version(1)), new FakeSource(), ignored -> { }, executor);

        service.close();
        service.close();

        verify(executor).shutdown();
        verify(executor).shutdownNow();
        verify(executor, times(1)).execute(any(Runnable.class));
    }

    @Test
    void closePreservesInterruptionAndForcesShutdown() throws Exception {
        ExecutorService executor = mock(ExecutorService.class);
        when(executor.awaitTermination(5, TimeUnit.SECONDS))
                .thenThrow(new InterruptedException("stop"));
        MetadataWatchService service = new MetadataWatchService(
                new FakeRepository(version(1)), new FakeSource(), ignored -> { }, executor);

        try {
            service.close();
            assertTrue(Thread.currentThread().isInterrupted());
            verify(executor).shutdownNow();
        } finally {
            Thread.interrupted();
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

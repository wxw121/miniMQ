package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Maintains a monotonic metadata view. jetcd callbacks only enqueue work; parsing and
 * installation are serialized on the metadata-installer thread.
 */
public final class MetadataWatchService implements AutoCloseable {
    private static final long RELOAD_RETRY_MS = 100L;

    public interface Source {
        Subscription watch(long startRevision, Consumer<VersionedClusterMetadata> event, Runnable failure);
    }

    public interface Subscription extends AutoCloseable {
        @Override
        void close();
    }

    private final ClusterMetadataRepository repository;
    private final Source source;
    private final Consumer<VersionedClusterMetadata> installer;
    private final ExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Long> initialLoad = new CompletableFuture<>();
    private long installedRevision = -1;
    private Subscription subscription;

    public MetadataWatchService(ClusterMetadataRepository repository, Source source,
                                Consumer<VersionedClusterMetadata> installer) {
        this(repository, source, installer, Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "metadata-installer");
            thread.setDaemon(true);
            return thread;
        }));
    }

    public MetadataWatchService(ClusterMetadataRepository repository, Watch watch, String clusterId,
                                Consumer<VersionedClusterMetadata> installer) {
        this(repository, new EtcdSource(watch, clusterId), installer);
    }

    MetadataWatchService(ClusterMetadataRepository repository, Source source,
                         Consumer<VersionedClusterMetadata> installer, ExecutorService executor) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.source = Objects.requireNonNull(source, "source");
        this.installer = Objects.requireNonNull(installer, "installer");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public void start() {
        enqueue(this::reloadAndWatch);
    }

    public CompletionStage<Long> initialLoad() {
        return initialLoad;
    }

    private void reloadAndWatch() {
        if (closed.get()) {
            return;
        }
        try {
            closeSubscription();
            VersionedClusterMetadata current = repository.read();
            install(current);
            subscription = source.watch(Math.addExact(current.revision(), 1L),
                    value -> enqueue(() -> install(value)),
                    () -> enqueue(this::reloadAndWatch));
            initialLoad.complete(current.revision());
        } catch (RuntimeException failure) {
            retryReload();
        } catch (Error failure) {
            initialLoad.completeExceptionally(failure);
            throw failure;
        }
    }

    private void retryReload() {
        if (closed.get()) {
            return;
        }
        try {
            Thread.sleep(RELOAD_RETRY_MS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        enqueue(this::reloadAndWatch);
    }

    private void install(VersionedClusterMetadata value) {
        if (value.revision() <= installedRevision) {
            return;
        }
        installer.accept(value);
        installedRevision = value.revision();
    }

    public long installedRevision() {
        return installedRevision;
    }

    private void enqueue(Runnable task) {
        if (!closed.get()) {
            try {
                executor.execute(task);
            } catch (RejectedExecutionException rejected) {
                if (!closed.get()) {
                    throw rejected;
                }
            }
        }
    }

    private void closeSubscription() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.execute(this::closeSubscription);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static final class EtcdSource implements Source {
        private final Watch watch;
        private final ByteSequence key;
        private final ObjectMapper mapper = new ObjectMapper();

        private EtcdSource(Watch watch, String clusterId) {
            this.watch = Objects.requireNonNull(watch, "watch");
            this.key = ByteSequence.from(EtcdClusterMetadataRepository.snapshotKey(clusterId),
                    StandardCharsets.UTF_8);
        }

        @Override
        public Subscription watch(long startRevision, Consumer<VersionedClusterMetadata> event,
                                  Runnable failure) {
            AtomicBoolean finished = new AtomicBoolean();
            WatchOption option = WatchOption.builder().withRevision(startRevision).build();
            Watch.Watcher watcher = watch.watch(key, option, response -> {
                response.getEvents().forEach(watchEvent -> {
                    try {
                        var keyValue = watchEvent.getKeyValue();
                        ClusterMetadata metadata = mapper.readValue(
                                keyValue.getValue().getBytes(), ClusterMetadata.class);
                        MetadataStore.migrateAndValidate(metadata);
                        event.accept(new VersionedClusterMetadata(metadata, keyValue.getModRevision()));
                    } catch (Exception invalidSnapshot) {
                        if (finished.compareAndSet(false, true)) {
                            failure.run();
                        }
                    }
                });
            }, ignored -> {
                if (finished.compareAndSet(false, true)) {
                    failure.run();
                }
            }, () -> {
                if (finished.compareAndSet(false, true)) {
                    failure.run();
                }
            });
            return () -> {
                finished.set(true);
                watcher.close();
            };
        }
    }
}

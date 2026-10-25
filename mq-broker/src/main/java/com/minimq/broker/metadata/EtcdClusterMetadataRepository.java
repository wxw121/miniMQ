package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minimq.broker.coordination.ControllerCoordinationStore;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Leader-fenced, revision-CAS cluster metadata stored in etcd. */
public final class EtcdClusterMetadataRepository implements ClusterMetadataRepository {
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(10);
    private final KV kv;
    private final ObjectMapper mapper;
    private final ByteSequence snapshotKey;
    private final ByteSequence leaderKey;
    private final ByteSequence leaderIdentity;

    public EtcdClusterMetadataRepository(KV kv, String clusterId,
                                         ControllerCoordinationStore.Leadership leadership) {
        this(kv, clusterId, leadership.identityBytes(), new ObjectMapper());
    }

    public EtcdClusterMetadataRepository(KV kv, String clusterId, byte[] identityBytes, ObjectMapper mapper) {
        this.kv = java.util.Objects.requireNonNull(kv, "kv");
        this.mapper = java.util.Objects.requireNonNull(mapper, "mapper");
        this.snapshotKey = bytes(snapshotKey(clusterId));
        this.leaderKey = bytes("/minimq/" + clusterId + "/controller/leader");
        this.leaderIdentity = ByteSequence.from(identityBytes.clone());
    }

    public static String snapshotKey(String clusterId) {
        if (clusterId == null || clusterId.isBlank() || clusterId.contains("/")) {
            throw new IllegalArgumentException("invalid clusterId");
        }
        return "/minimq/" + clusterId + "/metadata/snapshot";
    }

    @Override
    public VersionedClusterMetadata read() {
        return read(OPERATION_TIMEOUT);
    }

    @Override
    public VersionedClusterMetadata read(Duration timeout) {
        return readUntil(deadlineNanos(timeout));
    }

    private VersionedClusterMetadata readUntil(long deadlineNanos) {
        try {
            List<KeyValue> values = await(() -> kv.get(snapshotKey), deadlineNanos).getKvs();
            if (values.isEmpty()) {
                return new VersionedClusterMetadata(new ClusterMetadata(), 0);
            }
            KeyValue value = values.get(0);
            ClusterMetadata metadata = mapper.readValue(
                    value.getValue().getBytes(), ClusterMetadata.class);
            MetadataStore.migrateAndValidate(metadata);
            return new VersionedClusterMetadata(
                    metadata,
                    value.getModRevision());
        } catch (Exception e) {
            throw new IllegalStateException("failed to read cluster metadata from etcd", e);
        }
    }

    @Override
    public UpdateResult initialize(ClusterMetadata metadata) {
        return initialize(metadata, OPERATION_TIMEOUT);
    }

    @Override
    public UpdateResult initialize(ClusterMetadata metadata, Duration timeout) {
        long deadlineNanos = deadlineNanos(timeout);
        ClusterMetadata candidate = copy(metadata);
        candidate.metadataVersion = 0;
        try {
            byte[] serialized = mapper.writeValueAsBytes(candidate);
            boolean written = await(
                    () -> kv.txn()
                            .If(leaderComparison(),
                                    new Cmp(
                                            snapshotKey,
                                            Cmp.Op.EQUAL,
                                            CmpTarget.createRevision(0)))
                            .Then(Op.put(
                                    snapshotKey,
                                    ByteSequence.from(serialized),
                                    PutOption.DEFAULT))
                            .commit(),
                    deadlineNanos).isSucceeded();
            VersionedClusterMetadata current = readUntil(deadlineNanos);
            if (written || mapper.valueToTree(candidate).equals(mapper.valueToTree(current.metadata()))) {
                return new UpdateResult(ErrorCode.NONE, current);
            }
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        } catch (Exception e) {
            throw new IllegalStateException("failed to initialize cluster metadata", e);
        }
    }

    @Override
    public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
        return compareAndSet(expectedRevision, metadata, OPERATION_TIMEOUT);
    }

    @Override
    public UpdateResult compareAndSet(
            long expectedRevision, ClusterMetadata metadata, Duration timeout) {
        long deadlineNanos = deadlineNanos(timeout);
        VersionedClusterMetadata current = readUntil(deadlineNanos);
        if (current.revision() != expectedRevision) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }
        ClusterMetadata candidate = copy(metadata);
        candidate.metadataVersion = Math.addExact(current.metadata().metadataVersion, 1);
        try {
            byte[] serialized = mapper.writeValueAsBytes(candidate);
            boolean written = await(
                    () -> kv.txn()
                            .If(leaderComparison(),
                                    new Cmp(
                                            snapshotKey,
                                            Cmp.Op.EQUAL,
                                            CmpTarget.modRevision(expectedRevision)))
                            .Then(Op.put(
                                    snapshotKey,
                                    ByteSequence.from(serialized),
                                    PutOption.DEFAULT))
                            .commit(),
                    deadlineNanos).isSucceeded();
            VersionedClusterMetadata result = readUntil(deadlineNanos);
            return new UpdateResult(written ? ErrorCode.NONE : ErrorCode.CONTROLLER_FENCED, result);
        } catch (Exception e) {
            throw new IllegalStateException("failed to CAS cluster metadata", e);
        }
    }

    private Cmp leaderComparison() {
        return new Cmp(leaderKey, Cmp.Op.EQUAL, CmpTarget.value(leaderIdentity));
    }

    private ClusterMetadata copy(ClusterMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        ClusterMetadata copy = mapper.convertValue(metadata, ClusterMetadata.class);
        try {
            MetadataStore.migrateAndValidate(copy);
        } catch (java.io.IOException invalidMetadata) {
            throw new IllegalArgumentException("invalid cluster metadata", invalidMetadata);
        }
        return copy;
    }

    private static ByteSequence bytes(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }

    private static long deadlineNanos(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        Duration bounded = timeout.compareTo(OPERATION_TIMEOUT) > 0
                ? OPERATION_TIMEOUT : timeout;
        long now = System.nanoTime();
        long budgetNanos = bounded.toNanos();
        return now > Long.MAX_VALUE - budgetNanos
                ? Long.MAX_VALUE : now + budgetNanos;
    }

    private static <T> T await(
            Supplier<CompletableFuture<T>> operation, long deadlineNanos) {
        try {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new java.util.concurrent.TimeoutException("etcd operation budget exhausted");
            }
            CompletableFuture<T> future = operation.get();
            remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                future.cancel(true);
                throw new java.util.concurrent.TimeoutException("etcd operation budget exhausted");
            }
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("etcd operation interrupted", interrupted);
        } catch (Exception e) {
            throw new IllegalStateException("etcd operation failed", e);
        }
    }
}

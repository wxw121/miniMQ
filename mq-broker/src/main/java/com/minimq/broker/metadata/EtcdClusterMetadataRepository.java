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
import java.util.concurrent.TimeUnit;

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

    EtcdClusterMetadataRepository(KV kv, String clusterId, byte[] identityBytes, ObjectMapper mapper) {
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
        try {
            List<KeyValue> values = await(kv.get(snapshotKey)).getKvs();
            if (values.isEmpty()) {
                return new VersionedClusterMetadata(new ClusterMetadata(), 0);
            }
            KeyValue value = values.get(0);
            return new VersionedClusterMetadata(
                    mapper.readValue(value.getValue().getBytes(), ClusterMetadata.class),
                    value.getModRevision());
        } catch (Exception e) {
            throw new IllegalStateException("failed to read cluster metadata from etcd", e);
        }
    }

    @Override
    public UpdateResult initialize(ClusterMetadata metadata) {
        ClusterMetadata candidate = copy(metadata);
        candidate.metadataVersion = 0;
        try {
            boolean written = await(kv.txn()
                    .If(leaderComparison(),
                            new Cmp(snapshotKey, Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                    .Then(Op.put(snapshotKey, ByteSequence.from(mapper.writeValueAsBytes(candidate)), PutOption.DEFAULT))
                    .commit()).isSucceeded();
            VersionedClusterMetadata current = read();
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
        VersionedClusterMetadata current = read();
        if (current.revision() != expectedRevision) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }
        ClusterMetadata candidate = copy(metadata);
        candidate.metadataVersion = Math.addExact(current.metadata().metadataVersion, 1);
        try {
            boolean written = await(kv.txn()
                    .If(leaderComparison(),
                            new Cmp(snapshotKey, Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision)))
                    .Then(Op.put(snapshotKey, ByteSequence.from(mapper.writeValueAsBytes(candidate)), PutOption.DEFAULT))
                    .commit()).isSucceeded();
            VersionedClusterMetadata result = read();
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
        return mapper.convertValue(metadata, ClusterMetadata.class);
    }

    private static ByteSequence bytes(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }

    private static <T> T await(java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("etcd operation failed", e);
        }
    }
}

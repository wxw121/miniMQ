package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EtcdClusterMetadataRepositoryTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void validatesInputsAndReadsEmptySnapshot() {
        KV kv = mock(KV.class);
        doReturn(completed(getResponse())).when(kv).get(any());

        EtcdClusterMetadataRepository repository = repository(kv);

        assertEquals("/minimq/demo/metadata/snapshot",
                EtcdClusterMetadataRepository.snapshotKey("demo"));
        assertThrows(IllegalArgumentException.class,
                () -> EtcdClusterMetadataRepository.snapshotKey(" "));
        assertThrows(IllegalArgumentException.class,
                () -> EtcdClusterMetadataRepository.snapshotKey("a/b"));
        assertThrows(NullPointerException.class,
                () -> new EtcdClusterMetadataRepository(null, "demo", new byte[] {1}, MAPPER));
        VersionedClusterMetadata empty = repository.read();
        assertEquals(0, empty.revision());
        assertEquals(0, empty.metadata().metadataVersion);
    }

    @Test
    void readsSnapshotAndRejectsInvalidPayload() throws Exception {
        KV kv = mock(KV.class);
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.metadataVersion = 7;
        doReturn(completed(getResponse(keyValue(MAPPER.writeValueAsBytes(metadata), 12))))
                .when(kv).get(any());

        VersionedClusterMetadata value = repository(kv).read();

        assertEquals(12, value.revision());
        assertEquals(7, value.metadata().metadataVersion);

        doReturn(completed(getResponse(keyValue(new byte[] {1}, 13)))).when(kv).get(any());
        assertThrows(IllegalStateException.class, () -> repository(kv).read());
        when(kv.get(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("down")));
        assertThrows(IllegalStateException.class, () -> repository(kv).read());
    }

    @Test
    void initializesOnlyWhenCreateCasWinsOrSnapshotIsIdentical() throws Exception {
        KV kv = mock(KV.class);
        Txn txn = successfulTxn(kv, true);
        ClusterMetadata candidate = new ClusterMetadata();
        candidate.metadataVersion = 99;
        ClusterMetadata stored = new ClusterMetadata();
        stored.metadataVersion = 0;
        doReturn(completed(getResponse(keyValue(MAPPER.writeValueAsBytes(stored), 3))))
                .when(kv).get(any());

        ClusterMetadataRepository.UpdateResult created = repository(kv).initialize(candidate);

        assertEquals(ErrorCode.NONE, created.errorCode());
        assertEquals(3, created.value().revision());
        verify(txn).If(any(io.etcd.jetcd.op.Cmp[].class));
        verify(txn).Then(any(io.etcd.jetcd.op.Op[].class));

        reset(kv);
        successfulTxn(kv, false);
        doReturn(completed(getResponse(keyValue(MAPPER.writeValueAsBytes(stored), 4))))
                .when(kv).get(any());
        assertEquals(ErrorCode.NONE, repository(kv).initialize(candidate).errorCode());

        stored.metadataVersion = 4;
        doReturn(completed(getResponse(keyValue(MAPPER.writeValueAsBytes(stored), 5))))
                .when(kv).get(any());
        assertEquals(ErrorCode.CONTROLLER_FENCED, repository(kv).initialize(candidate).errorCode());
        assertThrows(IllegalArgumentException.class, () -> repository(kv).initialize(null));
    }

    @Test
    void compareAndSetChecksObservedRevisionAndReturnsTxnOutcome() throws Exception {
        KV kv = mock(KV.class);
        ClusterMetadata current = new ClusterMetadata();
        current.metadataVersion = 8;
        ClusterMetadata next = new ClusterMetadata();
        doReturn(completed(getResponse(keyValue(MAPPER.writeValueAsBytes(current), 20))))
                .when(kv).get(any());

        ClusterMetadataRepository.UpdateResult stale = repository(kv).compareAndSet(19, next);
        assertEquals(ErrorCode.CONTROLLER_FENCED, stale.errorCode());
        verify(kv, never()).txn();

        reset(kv);
        successfulTxn(kv, true);
        ClusterMetadata committed = new ClusterMetadata();
        committed.metadataVersion = 9;
        doReturn(
                completed(getResponse(keyValue(MAPPER.writeValueAsBytes(current), 20))),
                completed(getResponse(keyValue(MAPPER.writeValueAsBytes(committed), 21))))
                .when(kv).get(any());
        ClusterMetadataRepository.UpdateResult success = repository(kv).compareAndSet(20, next);
        assertEquals(ErrorCode.NONE, success.errorCode());
        assertEquals(9, success.value().metadata().metadataVersion);

        reset(kv);
        successfulTxn(kv, false);
        doReturn(
                completed(getResponse(keyValue(MAPPER.writeValueAsBytes(current), 20))),
                completed(getResponse(keyValue(MAPPER.writeValueAsBytes(current), 20))))
                .when(kv).get(any());
        assertEquals(ErrorCode.CONTROLLER_FENCED,
                repository(kv).compareAndSet(20, next).errorCode());
    }

    private static EtcdClusterMetadataRepository repository(KV kv) {
        return new EtcdClusterMetadataRepository(kv, "demo", new byte[] {1, 2}, MAPPER);
    }

    private static Txn successfulTxn(KV kv, boolean succeeded) {
        Txn txn = mock(Txn.class);
        TxnResponse response = mock(TxnResponse.class);
        when(response.isSucceeded()).thenReturn(succeeded);
        when(kv.txn()).thenReturn(txn);
        when(txn.If(any(io.etcd.jetcd.op.Cmp[].class))).thenReturn(txn);
        when(txn.Then(any(io.etcd.jetcd.op.Op[].class))).thenReturn(txn);
        when(txn.commit()).thenReturn(completed(response));
        return txn;
    }

    private static GetResponse getResponse(KeyValue... values) {
        GetResponse response = mock(GetResponse.class);
        when(response.getKvs()).thenReturn(List.of(values));
        return response;
    }

    private static KeyValue keyValue(byte[] value, long revision) {
        KeyValue keyValue = mock(KeyValue.class);
        when(keyValue.getValue()).thenReturn(ByteSequence.from(value));
        when(keyValue.getModRevision()).thenReturn(revision);
        return keyValue;
    }

    private static <T> CompletableFuture<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }
}

package com.minimq.broker.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.*;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EtcdControllerCoordinationStoreTest {
    @Test
    void validatesIdentityClusterAndDependencies() {
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "", 9001, 1, "s"));
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "host", 0, 1, "s"));
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerIdentity(1, "host", 9001, -1, "s"));
        assertEquals("/minimq/c/controller/leader",
                EtcdControllerCoordinationStore.leaderKey("c"));
        assertThrows(IllegalArgumentException.class,
                () -> EtcdControllerCoordinationStore.leaderKey("a/b"));
        assertThrows(NullPointerException.class,
                () -> new EtcdControllerCoordinationStore(null, "c"));
        assertThrows(IllegalArgumentException.class, () -> EtcdClientFactory.create(null));
        assertThrows(IllegalArgumentException.class, () -> EtcdClientFactory.create(List.of()));
    }

    @Test
    void campaignsPublishesIdentityAndCleansInvalidLeaseExactlyOnce() throws Exception {
        Fixture f = new Fixture();
        f.emptyEpoch();
        f.txnOutcomes(true, true);
        EtcdControllerCoordinationStore store = f.store();

        ControllerCoordinationStore.Leadership leadership = store.campaign(2, "broker", 9092, 5);

        assertEquals(2, leadership.identity().brokerId());
        assertEquals(1, leadership.identity().controllerEpoch());
        assertArrayEquals(new ObjectMapper().writeValueAsBytes(leadership.identity()),
                leadership.identityBytes());
        byte[] defensive = leadership.identityBytes();
        defensive[0] ^= 1;
        assertNotEquals(defensive[0], leadership.identityBytes()[0]);
        assertTrue(leadership.valid());
        assertThrows(IllegalStateException.class, () -> store.campaign(3, "other", 9093, 5));

        f.keepAliveObserver.getValue().onError(new RuntimeException("lease lost"));
        assertFalse(leadership.valid());
        store.close();
        store.close();
        verify(f.keepAlive, times(1)).close();
        verify(f.election, times(1)).resign(f.leaderKey);
        verify(f.lease, times(1)).revoke(41);
    }

    @Test
    void incrementsExistingEpochAndReadsCurrentLeader() throws Exception {
        Fixture f = new Fixture();
        f.existingEpoch(7, 30);
        f.txnOutcomes(true, true);
        EtcdControllerCoordinationStore store = f.store();
        ControllerCoordinationStore.Leadership leadership = store.campaign(4, "host", 9004, 3);
        ControllerIdentity identity = leadership.identity();
        KeyValue current = keyValue(new ObjectMapper().writeValueAsBytes(identity), 40);
        doReturn(completed(getResponse(current))).when(f.kv).get(any());

        assertEquals(8, identity.controllerEpoch());
        assertEquals(identity, store.currentLeader());
        doReturn(completed(getResponse())).when(f.kv).get(any());
        assertNull(store.currentLeader());
        doReturn(completed(getResponse(keyValue(new byte[] {1}, 41)))).when(f.kv).get(any());
        assertThrows(IllegalStateException.class, store::currentLeader);
        leadership.close();
        assertFalse(leadership.valid());
    }

    @Test
    void failedLeaderInstallCleansCampaignResourcesAndFences() {
        Fixture f = new Fixture();
        f.emptyEpoch();
        f.txnOutcomes(true, false);
        EtcdControllerCoordinationStore store = f.store();

        assertThrows(ControllerFencedException.class,
                () -> store.campaign(1, "host", 9001, 3));
        verify(f.keepAlive).close();
        verify(f.election).resign(f.leaderKey);
        verify(f.lease).revoke(41);
    }

    @Test
    void repeatedEpochConflictsFenceCampaign() {
        Fixture f = new Fixture();
        f.emptyEpoch();
        Txn conflict = f.txn(false);
        when(f.kv.txn()).thenReturn(conflict);
        EtcdControllerCoordinationStore store = f.store();

        ControllerFencedException error = assertThrows(ControllerFencedException.class,
                () -> store.campaign(1, "host", 9001, 3));

        assertTrue(error.getMessage().contains("epoch CAS"));
        verify(f.kv, times(16)).txn();
        verify(f.keepAlive).close();
    }

    private static final class Fixture {
        final Client client = mock(Client.class);
        final KV kv = mock(KV.class);
        final Lease lease = mock(Lease.class);
        final Election election = mock(Election.class);
        final CloseableClient keepAlive = mock(CloseableClient.class);
        final LeaderKey leaderKey = mock(LeaderKey.class);
        final ArgumentCaptor<StreamObserver<io.etcd.jetcd.lease.LeaseKeepAliveResponse>> keepAliveObserver =
                ArgumentCaptor.forClass(StreamObserver.class);

        Fixture() {
            when(client.getKVClient()).thenReturn(kv);
            when(client.getLeaseClient()).thenReturn(lease);
            when(client.getElectionClient()).thenReturn(election);
            LeaseGrantResponse grant = mock(LeaseGrantResponse.class);
            when(grant.getID()).thenReturn(41L);
            when(lease.grant(3)).thenReturn(completed(grant));
            when(lease.grant(5)).thenReturn(completed(grant));
            when(lease.keepAlive(eq(41L), keepAliveObserver.capture())).thenReturn(keepAlive);
            CampaignResponse campaign = mock(CampaignResponse.class);
            when(campaign.getLeader()).thenReturn(leaderKey);
            when(election.campaign(any(), eq(41L), any())).thenReturn(completed(campaign));
            when(election.resign(leaderKey)).thenReturn(completed(mock(io.etcd.jetcd.election.ResignResponse.class)));
            when(lease.revoke(41L)).thenReturn(completed(mock(LeaseRevokeResponse.class)));
        }

        EtcdControllerCoordinationStore store() {
            return new EtcdControllerCoordinationStore(client, "demo", new ObjectMapper());
        }

        void emptyEpoch() {
            doReturn(completed(getResponse())).when(kv).get(any());
        }

        void existingEpoch(long epoch, long revision) {
            doReturn(completed(getResponse(
                    keyValue(Long.toString(epoch).getBytes(java.nio.charset.StandardCharsets.UTF_8), revision))))
                    .when(kv).get(any());
        }

        void txnOutcomes(boolean epoch, boolean leader) {
            Txn epochTxn = txn(epoch);
            Txn leaderTxn = txn(leader);
            when(kv.txn()).thenReturn(epochTxn, leaderTxn);
        }

        Txn txn(boolean result) {
            Txn txn = mock(Txn.class);
            TxnResponse response = mock(TxnResponse.class);
            when(response.isSucceeded()).thenReturn(result);
            when(txn.If(any(io.etcd.jetcd.op.Cmp[].class))).thenReturn(txn);
            when(txn.Then(any(io.etcd.jetcd.op.Op[].class))).thenReturn(txn);
            when(txn.commit()).thenReturn(completed(response));
            return txn;
        }
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

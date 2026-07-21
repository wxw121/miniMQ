package com.minimq.broker.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.*;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** etcd election, lease and monotonically increasing controller epoch. */
public final class EtcdControllerCoordinationStore implements ControllerCoordinationStore {
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(10);
    private final Client client;
    private final KV kv;
    private final Lease lease;
    private final Election election;
    private final ObjectMapper mapper;
    private final String electionName;
    private final String leaderKey;
    private final String epochKey;
    private volatile LeadershipImpl active;

    public EtcdControllerCoordinationStore(Client client, String clusterId) {
        this(client, clusterId, new ObjectMapper());
    }

    EtcdControllerCoordinationStore(Client client, String clusterId, ObjectMapper mapper) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        requireClusterId(clusterId);
        this.kv = client.getKVClient();
        this.lease = client.getLeaseClient();
        this.election = client.getElectionClient();
        this.mapper = mapper;
        String root = "/minimq/" + clusterId + "/controller/";
        this.electionName = root + "election/";
        this.leaderKey = root + "leader";
        this.epochKey = root + "epoch";
    }

    public static String leaderKey(String clusterId) {
        requireClusterId(clusterId);
        return "/minimq/" + clusterId + "/controller/leader";
    }

    @Override
    public synchronized Leadership campaign(int brokerId, String host, int port, int ttlSeconds) {
        if (active != null) {
            throw new IllegalStateException("leadership session already active");
        }
        long leaseId = await(lease.grant(ttlSeconds)).getID();
        AtomicBoolean valid = new AtomicBoolean(true);
        CloseableClient keepAlive = lease.keepAlive(leaseId, new StreamObserver<>() {
            @Override public void onNext(io.etcd.jetcd.lease.LeaseKeepAliveResponse value) { }
            @Override public void onError(Throwable t) { valid.set(false); }
            @Override public void onCompleted() { valid.set(false); }
        });
        LeaderKey elected = null;
        try {
            String sessionId = UUID.randomUUID().toString();
            elected = awaitCampaign(election.campaign(
                    bytes(electionName), leaseId, bytes(sessionId))).getLeader();
            long epoch = incrementEpoch();
            ControllerIdentity identity = new ControllerIdentity(brokerId, host, port, epoch, sessionId);
            byte[] identityBytes = mapper.writeValueAsBytes(identity);
            boolean installed = await(kv.txn()
                    .If(new Cmp(bytes(leaderKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                    .Then(Op.put(bytes(leaderKey), ByteSequence.from(identityBytes),
                            PutOption.builder().withLeaseId(leaseId).build()))
                    .commit()).isSucceeded();
            if (!installed) {
                throw new ControllerFencedException("another lease owns the controller leader key");
            }
            active = new LeadershipImpl(identity, identityBytes, valid, keepAlive, elected, leaseId);
            return active;
        } catch (Exception e) {
            valid.set(false);
            keepAlive.close();
            if (elected != null) {
                try { await(election.resign(elected)); } catch (RuntimeException ignored) { }
            }
            try { await(lease.revoke(leaseId)); } catch (RuntimeException ignored) { }
            if (e instanceof ControllerFencedException fenced) {
                throw fenced;
            }
            throw new ControllerFencedException("controller campaign failed", e);
        }
    }

    @Override
    public ControllerIdentity currentLeader() {
        List<KeyValue> values = await(kv.get(bytes(leaderKey))).getKvs();
        if (values.isEmpty()) {
            return null;
        }
        try {
            return mapper.readValue(values.get(0).getValue().getBytes(), ControllerIdentity.class);
        } catch (IOException e) {
            throw new IllegalStateException("invalid controller identity in etcd", e);
        }
    }

    private long incrementEpoch() {
        for (int attempt = 0; attempt < 16; attempt++) {
            List<KeyValue> values = await(kv.get(bytes(epochKey))).getKvs();
            long next;
            Cmp compare;
            if (values.isEmpty()) {
                next = 1;
                compare = new Cmp(bytes(epochKey), Cmp.Op.EQUAL, CmpTarget.createRevision(0));
            } else {
                KeyValue current = values.get(0);
                next = Math.addExact(Long.parseLong(current.getValue().toString(StandardCharsets.UTF_8)), 1);
                compare = new Cmp(bytes(epochKey), Cmp.Op.EQUAL,
                        CmpTarget.modRevision(current.getModRevision()));
            }
            if (await(kv.txn().If(compare)
                    .Then(Op.put(bytes(epochKey), bytes(Long.toString(next)), PutOption.DEFAULT))
                    .commit()).isSucceeded()) {
                return next;
            }
        }
        throw new ControllerFencedException("controller epoch CAS repeatedly conflicted");
    }

    @Override
    public synchronized void close() {
        if (active != null) {
            active.close();
            active = null;
        }
    }

    private final class LeadershipImpl implements Leadership {
        private final ControllerIdentity identity;
        private final byte[] identityBytes;
        private final AtomicBoolean valid;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CloseableClient keepAlive;
        private final LeaderKey elected;
        private final long leaseId;

        private LeadershipImpl(ControllerIdentity identity, byte[] identityBytes, AtomicBoolean valid,
                               CloseableClient keepAlive, LeaderKey elected, long leaseId) {
            this.identity = identity;
            this.identityBytes = identityBytes.clone();
            this.valid = valid;
            this.keepAlive = keepAlive;
            this.elected = elected;
            this.leaseId = leaseId;
        }

        @Override public ControllerIdentity identity() { return identity; }
        @Override public byte[] identityBytes() { return identityBytes.clone(); }
        @Override public boolean valid() { return valid.get(); }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            valid.set(false);
            keepAlive.close();
            try { await(election.resign(elected)); } catch (RuntimeException ignored) { }
            try { await(lease.revoke(leaseId)); } catch (RuntimeException ignored) { }
            synchronized (EtcdControllerCoordinationStore.this) {
                if (active == this) {
                    active = null;
                }
            }
        }
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

    private static <T> T awaitCampaign(java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new IllegalStateException("etcd campaign failed", e);
        }
    }

    private static void requireClusterId(String clusterId) {
        if (clusterId == null || clusterId.isBlank() || clusterId.contains("/")) {
            throw new IllegalArgumentException("invalid clusterId");
        }
    }
}

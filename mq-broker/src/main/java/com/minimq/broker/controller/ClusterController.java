package com.minimq.broker.controller;

import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.LeaderReadyRequest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Cluster controller that maintains a consistent view of brokers, topics, partition leaders, and consumer groups.
 */
public class ClusterController {
    private static final long ISR_LAG_TOLERANCE_RECORDS = 10L;
    private static final long DEFAULT_LEASE_TIMEOUT_MS = 5_000L;

    private final ClusterMetadataRepository metadataRepository;
    private final long leaseTimeoutMs;
    private final LongSupplier nanoTime;
    private final Map<Integer, BrokerLease> brokerLeases = new HashMap<>();
    private ClusterMetadata metadata;
    private long metadataRevision;
    private boolean recovering;
    private long recoveryDeadlineNanos;
    private final Set<Integer> awaitingRecoveryBrokers = new HashSet<>();

    public record BrokerRegistration(ErrorCode errorCode, RegisterBrokerResponse response) {
        static BrokerRegistration failed(ErrorCode errorCode) {
            return new BrokerRegistration(errorCode, null);
        }
    }

    private record BrokerLease(long brokerEpoch, long deadlineNanos) {
    }

    private record BrokerLeaseKey(int brokerId, long brokerEpoch) {
    }

    public ClusterController(ClusterMetadataRepository metadataRepository) {
        this(metadataRepository, DEFAULT_LEASE_TIMEOUT_MS);
    }

    public ClusterController(ClusterMetadataRepository metadataRepository, long leaseTimeoutMs) {
        this(metadataRepository, leaseTimeoutMs, System::nanoTime);
    }

    ClusterController(
            ClusterMetadataRepository metadataRepository, long leaseTimeoutMs, LongSupplier nanoTime) {
        this.metadataRepository = metadataRepository;
        this.leaseTimeoutMs = leaseTimeoutMs;
        this.nanoTime = nanoTime;
        VersionedClusterMetadata loaded = metadataRepository.read();
        this.metadata = copyMetadata(loaded.metadata());
        this.metadataRevision = loaded.revision();
        normalizeMetadata();
        long deadline = deadlineFrom(nanoTime.getAsLong());
        metadata.brokers.values().stream()
                .filter(broker -> broker.online)
                .forEach(broker -> brokerLeases.put(
                        broker.brokerId, new BrokerLease(broker.brokerEpoch, deadline)));
    }

    // Public controller methods are synchronized to keep in-memory and persisted metadata ordered consistently.
    public synchronized BrokerRegistration registerBroker(RegisterBrokerRequest request) {
        long now = nanoTime.getAsLong();
        BrokerEntry current = metadata.brokers.get(request.brokerId());
        BrokerLease currentLease = brokerLeases.get(request.brokerId());
        boolean expectedRecovery = recovering && awaitingRecoveryBrokers.contains(request.brokerId());
        if (!expectedRecovery && current != null && current.online && currentLease != null
                && now < currentLease.deadlineNanos()) {
            return BrokerRegistration.failed(ErrorCode.BROKER_ID_IN_USE);
        }

        ClusterMetadata next = copyMetadata(metadata);
        BrokerEntry entry = next.brokers.computeIfAbsent(request.brokerId(), id -> new BrokerEntry());
        entry.brokerId = request.brokerId();
        entry.host = request.host();
        entry.port = request.port();
        entry.online = true;
        entry.brokerEpoch = current == null ? 1L : Math.addExact(current.brokerEpoch, 1L);
        restoreFrozenPartitions(next, request.brokerId());
        ErrorCode commitError = commit(next);
        if (commitError != ErrorCode.NONE) {
            return BrokerRegistration.failed(commitError);
        }
        brokerLeases.put(request.brokerId(), new BrokerLease(entry.brokerEpoch, deadlineFrom(now)));
        awaitingRecoveryBrokers.remove(request.brokerId());
        return new BrokerRegistration(
                ErrorCode.NONE, new RegisterBrokerResponse(entry.brokerEpoch, leaseTimeoutMs));
    }

    public synchronized ErrorCode heartbeatBroker(BrokerHeartbeatRequest request) {
        BrokerEntry broker = metadata.brokers.get(request.brokerId());
        BrokerLease lease = brokerLeases.get(request.brokerId());
        long now = nanoTime.getAsLong();
        if (broker == null || !broker.online || broker.brokerEpoch != request.brokerEpoch()
                || lease == null || lease.brokerEpoch() != request.brokerEpoch()
                || now >= lease.deadlineNanos()) {
            return ErrorCode.BROKER_FENCED;
        }
        brokerLeases.put(request.brokerId(), new BrokerLease(request.brokerEpoch(), deadlineFrom(now)));
        return ErrorCode.NONE;
    }

    public synchronized int expireOverdueBrokerLeases() {
        if (recovering) {
            return 0;
        }
        long now = nanoTime.getAsLong();
        List<BrokerLeaseKey> expired = brokerLeases.entrySet().stream()
                .filter(entry -> now >= entry.getValue().deadlineNanos())
                .map(entry -> new BrokerLeaseKey(entry.getKey(), entry.getValue().brokerEpoch()))
                .toList();
        int applied = 0;
        for (BrokerLeaseKey key : expired) {
            if (expireBrokerLease(key.brokerId(), key.brokerEpoch())) {
                applied++;
            }
        }
        return applied;
    }

    public synchronized boolean expireBrokerLease(int brokerId, long expectedEpoch) {
        BrokerEntry current = metadata.brokers.get(brokerId);
        BrokerLease lease = brokerLeases.get(brokerId);
        if (current == null || current.brokerEpoch != expectedEpoch
                || lease == null || lease.brokerEpoch() != expectedEpoch) {
            return false;
        }
        if (!current.online) {
            brokerLeases.remove(brokerId, lease);
            return false;
        }
        if (nanoTime.getAsLong() < lease.deadlineNanos()) {
            return false;
        }

        ClusterMetadata next = copyMetadata(metadata);
        for (TopicEntry topic : next.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                normalizePartition(next, partition);
            }
        }
        next.brokers.get(brokerId).online = false;
        for (TopicEntry topic : next.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                long cleanRecoveryOffset = recoveryOffset(partition);
                partition.isrBrokerIds.removeIf(id -> id == brokerId);
                if (partition.leaderBrokerId != brokerId) {
                    continue;
                }
                Integer replacement = preferredSurvivingLeader(next, partition, brokerId);
                if (replacement == null) {
                    partition.frozen = true;
                    partition.leaderReady = false;
                    partition.isrBrokerIds.clear();
                } else {
                    partition.leaderBrokerId = replacement;
                    partition.frozen = false;
                    partition.leaderEpoch = Math.addExact(partition.leaderEpoch, 1L);
                    partition.leaderEpochStartOffset = cleanRecoveryOffset;
                    partition.leaderReady = false;
                }
            }
        }

        if (commit(next) != ErrorCode.NONE) {
            return false;
        }
        brokerLeases.remove(brokerId, lease);
        return true;
    }

    public synchronized ErrorCode createTopic(CreateTopicRequest request) {
        if (recovering) {
            return ErrorCode.CONTROLLER_NOT_READY;
        }
        if (request.topicName() == null || request.topicName().isBlank()
                || request.numPartitions() <= 0 || request.replicationFactor() <= 0) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (metadata.topics.containsKey(request.topicName())) {
            return ErrorCode.INVALID_REQUEST;
        }
        List<BrokerEntry> onlineBrokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .collect(Collectors.toList());
        if (onlineBrokers.isEmpty() || request.replicationFactor() > onlineBrokers.size()) {
            return ErrorCode.INVALID_REQUEST;
        }
        ClusterMetadata next = copyMetadata(metadata);
        TopicEntry topic = new TopicEntry();
        topic.topicName = request.topicName();
        for (int i = 0; i < request.numPartitions(); i++) {
            PartitionEntry partition = new PartitionEntry();
            partition.partitionId = i;
            int firstReplicaIndex = i % onlineBrokers.size();
            List<Integer> replicas = new ArrayList<>();
            for (int replica = 0; replica < request.replicationFactor(); replica++) {
                replicas.add(onlineBrokers.get((firstReplicaIndex + replica) % onlineBrokers.size()).brokerId);
            }
            partition.replicaBrokerIds = replicas;
            partition.isrBrokerIds = new ArrayList<>(replicas);
            partition.leaderBrokerId = replicas.get(0);
            replicas.forEach(replicaId -> partition.replicaLogEndOffsets.put(replicaId, 0L));
            topic.partitions.add(partition);
        }
        next.topics.put(request.topicName(), topic);
        return commit(next);
    }

    public synchronized ErrorCode markBrokerOffline(int brokerId) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null) {
            return ErrorCode.INVALID_REQUEST;
        }
        ClusterMetadata next = copyMetadata(metadata);
        Map<PartitionEntry, Integer> replacementLeaders = new IdentityHashMap<>();
        Map<PartitionEntry, Long> recoveryOffsets = new IdentityHashMap<>();
        for (TopicEntry topic : next.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                normalizePartition(next, partition);
                if (partition.leaderBrokerId == brokerId) {
                    Integer replacementLeader = preferredSurvivingLeader(next, partition, brokerId);
                    if (replacementLeader == null) {
                        return ErrorCode.INVALID_REQUEST;
                    }
                    replacementLeaders.put(partition, replacementLeader);
                    recoveryOffsets.put(partition, recoveryOffset(partition));
                }
            }
        }
        next.brokers.get(brokerId).online = false;
        for (TopicEntry topic : next.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                partition.isrBrokerIds.removeIf(id -> id == brokerId);
                Integer replacementLeader = replacementLeaders.get(partition);
                if (replacementLeader != null) {
                    partition.leaderBrokerId = replacementLeader;
                    partition.leaderEpoch = Math.addExact(partition.leaderEpoch, 1L);
                    partition.leaderEpochStartOffset = recoveryOffsets.get(partition);
                    partition.leaderReady = false;
                }
            }
        }
        ErrorCode commitError = commit(next);
        if (commitError != ErrorCode.NONE) {
            return commitError;
        }
        brokerLeases.remove(brokerId);
        return ErrorCode.NONE;
    }

    private Integer preferredSurvivingLeader(PartitionEntry partition, int offlineBrokerId) {
        return preferredSurvivingLeader(metadata, partition, offlineBrokerId);
    }

    private Integer preferredSurvivingLeader(
            ClusterMetadata source, PartitionEntry partition, int offlineBrokerId) {
        Integer selectedBrokerId = null;
        long selectedLeo = Long.MIN_VALUE;
        for (int replicaId : partition.replicaBrokerIds) {
            BrokerEntry replica = source.brokers.get(replicaId);
            if (replicaId != offlineBrokerId
                    && partition.isrBrokerIds.contains(replicaId)
                    && replica != null
                    && replica.online) {
                long replicaLeo = partition.replicaLogEndOffsets.getOrDefault(replicaId, 0L);
                if (selectedBrokerId == null || replicaLeo > selectedLeo
                        || (replicaLeo == selectedLeo && replicaId < selectedBrokerId)) {
                    selectedBrokerId = replicaId;
                    selectedLeo = replicaLeo;
                }
            }
        }
        return selectedBrokerId;
    }

    private long recoveryOffset(PartitionEntry partition) {
        if (partition.isrBrokerIds.isEmpty()) {
            return 0L;
        }
        long recoveryOffset = Long.MAX_VALUE;
        for (int brokerId : partition.isrBrokerIds) {
            Long leo = partition.replicaLogEndOffsets.get(brokerId);
            if (leo == null || leo < 0) {
                return 0L;
            }
            recoveryOffset = Math.min(recoveryOffset, leo);
        }
        return recoveryOffset == Long.MAX_VALUE ? 0L : recoveryOffset;
    }

    public synchronized ErrorCode leaderReady(LeaderReadyRequest request) {
        if (request.controllerEpoch() != metadata.controllerEpoch) {
            return ErrorCode.CONTROLLER_FENCED;
        }
        BrokerEntry broker = metadata.brokers.get(request.brokerId());
        if (broker == null || !broker.online || broker.brokerEpoch != request.brokerEpoch()) {
            return ErrorCode.BROKER_FENCED;
        }
        ClusterMetadata next = copyMetadata(metadata);
        PartitionEntry partition = getPartitionEntry(next, request.topicName(), request.partitionId());
        if (partition == null || partition.leaderBrokerId != request.brokerId()) {
            return ErrorCode.NOT_LEADER;
        }
        if (partition.leaderEpoch != request.leaderEpoch()) {
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        if (request.logEndOffset() < partition.leaderEpochStartOffset) {
            return ErrorCode.LEADER_NOT_READY;
        }
        if (partition.leaderReady) {
            return ErrorCode.NONE;
        }
        partition.leaderReady = true;
        return commit(next);
    }

    private long deadlineFrom(long now) {
        return Math.addExact(now, TimeUnit.MILLISECONDS.toNanos(leaseTimeoutMs));
    }

    /** Starts the takeover grace period during which persisted online brokers may re-register. */
    public synchronized void beginRecovery(long graceMs) {
        if (graceMs < 0) {
            throw new IllegalArgumentException("recovery grace must be non-negative");
        }
        recovering = true;
        recoveryDeadlineNanos = Math.addExact(
                nanoTime.getAsLong(), TimeUnit.MILLISECONDS.toNanos(graceMs));
        awaitingRecoveryBrokers.clear();
        metadata.brokers.values().stream()
                .filter(broker -> broker.online)
                .map(broker -> broker.brokerId)
                .forEach(awaitingRecoveryBrokers::add);
    }

    /** Ends recovery when due and expires only brokers that did not re-register. */
    public synchronized boolean advanceRecoveryWindow() {
        if (!recovering || nanoTime.getAsLong() < recoveryDeadlineNanos) {
            return false;
        }
        recovering = false;
        long now = nanoTime.getAsLong();
        List<Integer> missing = List.copyOf(awaitingRecoveryBrokers);
        awaitingRecoveryBrokers.clear();
        for (int brokerId : missing) {
            BrokerEntry broker = metadata.brokers.get(brokerId);
            if (broker != null && broker.online) {
                brokerLeases.put(brokerId, new BrokerLease(broker.brokerEpoch, now));
                expireBrokerLease(brokerId, broker.brokerEpoch);
            }
        }
        return true;
    }

    public synchronized boolean isRecovering() {
        return recovering;
    }

    private static void restoreFrozenPartitions(ClusterMetadata metadata, int brokerId) {
        for (TopicEntry topic : metadata.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                if (partition.frozen && partition.leaderBrokerId == brokerId) {
                    partition.frozen = false;
                    partition.leaderReady = true;
                    partition.isrBrokerIds = new ArrayList<>(List.of(brokerId));
                }
            }
        }
    }

    public synchronized MetadataResponse metadata(MetadataRequest request) {
        List<BrokerMetadata> brokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .map(b -> new BrokerMetadata(b.brokerId, b.host, b.port))
                .collect(Collectors.toList());

        List<TopicMetadata> topics = new ArrayList<>();
        for (TopicEntry topic : metadata.topics.values()) {
            if (!request.topicName().isEmpty() && !topic.topicName.equals(request.topicName())) {
                continue;
            }
            List<PartitionMetadata> partitions = topic.partitions.stream()
                    .map(this::toPartitionMetadata)
                    .collect(Collectors.toList());
            topics.add(new TopicMetadata(topic.topicName, partitions));
        }
        return new MetadataResponse(brokers, topics, null, metadata.metadataVersion);
    }

    public synchronized TopicEntry getTopic(String topicName) {
        TopicEntry topic = metadata.topics.get(topicName);
        return topic != null ? copyTopic(topic) : null;
    }

    public synchronized int getLeaderBrokerId(String topicName, int partitionId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        return partition != null ? partition.leaderBrokerId : -1;
    }

    public synchronized List<Integer> getReplicaBrokerIds(String topicName, int partitionId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return List.of();
        }
        normalizePartition(partition);
        return List.copyOf(partition.replicaBrokerIds);
    }

    public synchronized List<Integer> getIsrBrokerIds(String topicName, int partitionId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return List.of();
        }
        normalizePartition(partition);
        return List.copyOf(partition.isrBrokerIds);
    }

    public synchronized boolean isReplica(String topicName, int partitionId, int brokerId) {
        return getReplicaBrokerIds(topicName, partitionId).contains(brokerId);
    }

    public synchronized ErrorCode updateReplicaLogEndOffset(
            String topicName, int partitionId, int brokerId, long logEndOffset) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null) {
            return ErrorCode.INVALID_REQUEST;
        }
        return updateReplicaLogEndOffset(
                topicName, partitionId, brokerId,
                broker.brokerEpoch,
                metadata.controllerEpoch,
                logEndOffset);
    }

    public synchronized ErrorCode updateReplicaLogEndOffset(
            String topicName, int partitionId, int brokerId,
            long brokerEpoch, long controllerEpoch, long logEndOffset) {
        if (controllerEpoch != metadata.controllerEpoch) {
            return ErrorCode.CONTROLLER_FENCED;
        }
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null || !broker.online || broker.brokerEpoch != brokerEpoch) {
            return ErrorCode.BROKER_FENCED;
        }
        ClusterMetadata next = copyMetadata(metadata);
        PartitionEntry partition = getPartitionEntry(next, topicName, partitionId);
        if (partition == null || logEndOffset < 0
                || !partition.replicaBrokerIds.contains(brokerId)) {
            return ErrorCode.INVALID_REQUEST;
        }
        normalizePartition(next, partition);
        long current = partition.replicaLogEndOffsets.getOrDefault(brokerId, 0L);
        boolean leoChanged = false;
        if (logEndOffset > current) {
            partition.replicaLogEndOffsets.put(brokerId, logEndOffset);
            leoChanged = true;
        }
        boolean isrChanged = reconcileIsrByLeo(next, partition);
        if (leoChanged || isrChanged) {
            return commit(next);
        }
        return ErrorCode.NONE;
    }

    public synchronized long replicaLogEndOffset(String topicName, int partitionId, int brokerId) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return 0L;
        }
        normalizePartition(partition);
        return partition.replicaLogEndOffsets.getOrDefault(brokerId, 0L);
    }

    public synchronized BrokerEntry getBroker(int brokerId) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        return broker != null ? copyBroker(broker) : null;
    }

    public synchronized ClusterMetadata snapshot() {
        return copyMetadata(metadata);
    }

    public synchronized void replaceMetadata(ClusterMetadata metadata) {
        ClusterMetadata next = copyMetadata(metadata);
        normalizeMetadata(next);
        ErrorCode error = commit(next);
        if (error != ErrorCode.NONE) {
            throw new IllegalStateException("Metadata replacement failed: " + error);
        }
    }

    public synchronized <T> T updateMetadata(Function<ClusterMetadata, T> updater) {
        ClusterMetadata next = copyMetadata(metadata);
        T result = updater.apply(next);
        normalizeMetadata(next);
        ErrorCode error = commit(next);
        if (error != ErrorCode.NONE) {
            throw new IllegalStateException("Metadata update failed: " + error);
        }
        return result;
    }

    private ErrorCode commit(ClusterMetadata next) {
        ClusterMetadataRepository.UpdateResult result =
                metadataRepository.compareAndSet(metadataRevision, next);
        if (result.errorCode() != ErrorCode.NONE) {
            return result.errorCode();
        }
        metadata = copyMetadata(result.value().metadata());
        metadataRevision = result.value().revision();
        return ErrorCode.NONE;
    }

    public synchronized ErrorCode validateLeaderEpoch(
            String topicName, int partitionId, long requestLeaderEpoch) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return ErrorCode.UNKNOWN_PARTITION;
        }
        if (partition.frozen || !partition.leaderReady) {
            return ErrorCode.LEADER_NOT_READY;
        }
        return partition.leaderEpoch == requestLeaderEpoch
                ? ErrorCode.NONE : ErrorCode.STALE_LEADER_EPOCH;
    }

    private PartitionEntry getPartitionEntry(String topicName, int partitionId) {
        return getPartitionEntry(metadata, topicName, partitionId);
    }

    private PartitionEntry getPartitionEntry(ClusterMetadata source, String topicName, int partitionId) {
        TopicEntry topic = source.topics.get(topicName);
        if (topic == null) {
            return null;
        }
        return topic.partitions.stream()
                .filter(partition -> partition.partitionId == partitionId)
                .findFirst()
                .orElse(null);
    }

    private PartitionMetadata toPartitionMetadata(PartitionEntry partition) {
        normalizePartition(partition);
        return new PartitionMetadata(
                partition.partitionId,
                partition.leaderBrokerId,
                List.copyOf(partition.replicaBrokerIds),
                List.copyOf(partition.isrBrokerIds),
                Map.copyOf(partition.replicaLogEndOffsets),
                partition.frozen,
                partition.leaderEpoch,
                partition.leaderEpochStartOffset,
                partition.leaderReady);
    }

    private void normalizeMetadata() {
        normalizeMetadata(metadata);
    }

    private void normalizeMetadata(ClusterMetadata source) {
        for (TopicEntry topic : source.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                normalizePartition(source, partition);
            }
        }
    }

    private void normalizePartition(PartitionEntry partition) {
        normalizePartition(metadata, partition);
    }

    private void normalizePartition(ClusterMetadata source, PartitionEntry partition) {
        if (partition.replicaBrokerIds == null) {
            partition.replicaBrokerIds = new ArrayList<>();
        }
        if (partition.isrBrokerIds == null) {
            partition.isrBrokerIds = new ArrayList<>();
        }
        if (partition.replicaLogEndOffsets == null) {
            partition.replicaLogEndOffsets = new java.util.HashMap<>();
        }
        if (partition.replicaBrokerIds.isEmpty()) {
            partition.replicaBrokerIds.add(partition.leaderBrokerId);
        }
        if (!partition.replicaBrokerIds.contains(partition.leaderBrokerId)) {
            partition.leaderBrokerId = partition.replicaBrokerIds.get(0);
        }
        for (int replicaId : partition.replicaBrokerIds) {
            partition.replicaLogEndOffsets.putIfAbsent(replicaId, 0L);
        }
        if (partition.frozen) {
            partition.leaderReady = false;
            partition.isrBrokerIds.clear();
            return;
        }
        if (partition.isrBrokerIds.isEmpty()) {
            for (int replicaId : partition.replicaBrokerIds) {
                BrokerEntry replica = source.brokers.get(replicaId);
                if (replica != null && replica.online) {
                    partition.isrBrokerIds.add(replicaId);
                }
            }
            if (partition.isrBrokerIds.isEmpty()) {
                partition.isrBrokerIds.add(partition.leaderBrokerId);
            }
        }
        partition.isrBrokerIds.removeIf(replicaId -> {
            BrokerEntry replica = source.brokers.get(replicaId);
            return replica == null || !replica.online;
        });
        if (!partition.isrBrokerIds.contains(partition.leaderBrokerId)) {
            partition.isrBrokerIds.add(0, partition.leaderBrokerId);
        }
        reconcileIsrByLeo(source, partition);
    }

    private boolean reconcileIsrByLeo(PartitionEntry partition) {
        return reconcileIsrByLeo(metadata, partition);
    }

    private boolean reconcileIsrByLeo(ClusterMetadata source, PartitionEntry partition) {
        if (partition.frozen) {
            return false;
        }
        List<Integer> before = new ArrayList<>(partition.isrBrokerIds);
        long leaderLeo = partition.replicaLogEndOffsets.getOrDefault(partition.leaderBrokerId, 0L);
        partition.isrBrokerIds.removeIf(replicaId -> {
            if (replicaId == partition.leaderBrokerId) {
                return false;
            }
            BrokerEntry replica = source.brokers.get(replicaId);
            if (replica == null || !replica.online) {
                return true;
            }
            long replicaLeo = partition.replicaLogEndOffsets.getOrDefault(replicaId, 0L);
            return leaderLeo - replicaLeo > ISR_LAG_TOLERANCE_RECORDS;
        });
        for (int replicaId : partition.replicaBrokerIds) {
            BrokerEntry replica = source.brokers.get(replicaId);
            if (replica == null || !replica.online || partition.isrBrokerIds.contains(replicaId)) {
                continue;
            }
            long replicaLeo = partition.replicaLogEndOffsets.getOrDefault(replicaId, 0L);
            if (replicaLeo >= leaderLeo) {
                partition.isrBrokerIds.add(replicaId);
            }
        }
        partition.isrBrokerIds.sort(Comparator.comparingInt(partition.replicaBrokerIds::indexOf));
        return !partition.isrBrokerIds.equals(before);
    }

    // Return deep copies so callers cannot mutate controller-owned state directly.
    private static ClusterMetadata copyMetadata(ClusterMetadata source) {
        ClusterMetadata copy = new ClusterMetadata();
        copy.metadataFormatVersion = source.metadataFormatVersion;
        copy.controllerEpoch = source.controllerEpoch;
        copy.metadataVersion = source.metadataVersion;
        source.brokers.forEach((id, broker) -> copy.brokers.put(id, copyBroker(broker)));
        source.topics.forEach((name, topic) -> copy.topics.put(name, copyTopic(topic)));
        source.consumerGroups.forEach((id, group) -> copy.consumerGroups.put(id, copyConsumerGroup(group)));
        return copy;
    }

    private static BrokerEntry copyBroker(BrokerEntry source) {
        BrokerEntry copy = new BrokerEntry();
        copy.brokerId = source.brokerId;
        copy.host = source.host;
        copy.port = source.port;
        copy.online = source.online;
        copy.brokerEpoch = source.brokerEpoch;
        return copy;
    }

    private static TopicEntry copyTopic(TopicEntry source) {
        TopicEntry copy = new TopicEntry();
        copy.topicName = source.topicName;
        for (PartitionEntry partition : source.partitions) {
            PartitionEntry partitionCopy = new PartitionEntry();
            partitionCopy.partitionId = partition.partitionId;
            partitionCopy.leaderBrokerId = partition.leaderBrokerId;
            partitionCopy.replicaBrokerIds = new ArrayList<>(partition.replicaBrokerIds);
            partitionCopy.isrBrokerIds = new ArrayList<>(partition.isrBrokerIds);
            partitionCopy.replicaLogEndOffsets = new java.util.HashMap<>(partition.replicaLogEndOffsets);
            partitionCopy.frozen = partition.frozen;
            partitionCopy.leaderEpoch = partition.leaderEpoch;
            partitionCopy.leaderEpochStartOffset = partition.leaderEpochStartOffset;
            partitionCopy.leaderReady = partition.leaderReady;
            copy.partitions.add(partitionCopy);
        }
        return copy;
    }

    private static ClusterMetadata.ConsumerGroupEntry copyConsumerGroup(
            ClusterMetadata.ConsumerGroupEntry source) {
        ClusterMetadata.ConsumerGroupEntry copy = new ClusterMetadata.ConsumerGroupEntry();
        copy.groupId = source.groupId;
        copy.topicName = source.topicName;
        copy.generationId = source.generationId;
        copy.leaderMemberId = source.leaderMemberId;
        copy.rebalancing = source.rebalancing;
        copy.committedOffsets.putAll(source.committedOffsets);
        source.members.forEach((id, member) -> copy.members.put(id, copyMember(member)));
        return copy;
    }

    private static ClusterMetadata.MemberEntry copyMember(ClusterMetadata.MemberEntry source) {
        ClusterMetadata.MemberEntry copy = new ClusterMetadata.MemberEntry();
        copy.memberId = source.memberId;
        copy.lastHeartbeatMs = source.lastHeartbeatMs;
        copy.assignedPartitions = new ArrayList<>(source.assignedPartitions);
        return copy;
    }
}

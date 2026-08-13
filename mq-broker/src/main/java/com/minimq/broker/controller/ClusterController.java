package com.minimq.broker.controller;

import com.minimq.broker.audit.FailoverAuditEventType;
import com.minimq.broker.audit.FailoverAuditLog;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.BrokerEntry;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.validation.TopicNames;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.LeaderReadyRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
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
    private final FailoverAuditLog auditLog;
    private final Object brokerLeaseLock = new Object();
    private final Map<Integer, BrokerLease> brokerLeases = new HashMap<>();
    private ClusterMetadata metadata;
    private long metadataRevision;
    private boolean recovering;
    private long recoveryDeadlineNanos;
    private final Set<Integer> awaitingRecoveryBrokers = new HashSet<>();
    private final Map<String, LeaderActivationDeadline> leaderActivationDeadlines = new HashMap<>();

    public record BrokerRegistration(ErrorCode errorCode, RegisterBrokerResponse response) {
        static BrokerRegistration failed(ErrorCode errorCode) {
            return new BrokerRegistration(errorCode, null);
        }
    }

    public record IncreasePartitionsResult(
            ErrorCode errorCode, int previousPartitionCount, int newPartitionCount, long metadataVersion) {
    }

    public record ReliabilityConfigResult(
            ErrorCode errorCode, String retryTopicName, String deadLetterTopicName, long metadataVersion) {
    }

    private record BrokerLease(long brokerEpoch, long deadlineNanos) {
    }

    private record BrokerLeaseKey(int brokerId, long brokerEpoch) {
    }

    private record LeaderActivationDeadline(long leaderEpoch, long deadlineNanos) {
    }

    public ClusterController(ClusterMetadataRepository metadataRepository) {
        this(metadataRepository, DEFAULT_LEASE_TIMEOUT_MS);
    }

    public ClusterController(ClusterMetadataRepository metadataRepository, long leaseTimeoutMs) {
        this(metadataRepository, leaseTimeoutMs, System::nanoTime);
    }

    public ClusterController(
            ClusterMetadataRepository metadataRepository,
            FailoverAuditLog auditLog) {
        this(metadataRepository, DEFAULT_LEASE_TIMEOUT_MS, System::nanoTime, auditLog);
    }

    public ClusterController(
            ClusterMetadataRepository metadataRepository,
            long leaseTimeoutMs,
            FailoverAuditLog auditLog) {
        this(metadataRepository, leaseTimeoutMs, System::nanoTime, auditLog);
    }

    ClusterController(
            ClusterMetadataRepository metadataRepository, long leaseTimeoutMs, LongSupplier nanoTime) {
        this(metadataRepository, leaseTimeoutMs, nanoTime, null);
    }

    public ClusterController(
            ClusterMetadataRepository metadataRepository,
            long leaseTimeoutMs,
            LongSupplier nanoTime,
            FailoverAuditLog auditLog) {
        this.metadataRepository = metadataRepository;
        this.leaseTimeoutMs = leaseTimeoutMs;
        this.nanoTime = nanoTime;
        this.auditLog = auditLog;
        VersionedClusterMetadata loaded = metadataRepository.read();
        this.metadata = copyMetadata(loaded.metadata());
        this.metadataRevision = loaded.revision();
        normalizeMetadata();
        long deadline = deadlineFrom(nanoTime.getAsLong());
        synchronized (brokerLeaseLock) {
            metadata.brokers.values().stream()
                    .filter(broker -> broker.online)
                    .forEach(broker -> brokerLeases.put(
                            broker.brokerId, new BrokerLease(broker.brokerEpoch, deadline)));
        }
    }

    // Public controller methods are synchronized to keep in-memory and persisted metadata ordered consistently.
    public synchronized BrokerRegistration registerBroker(RegisterBrokerRequest request) {
        long now = nanoTime.getAsLong();
        BrokerEntry current = metadata.brokers.get(request.brokerId());
        BrokerLease currentLease;
        boolean expectedRecovery;
        synchronized (brokerLeaseLock) {
            currentLease = brokerLeases.get(request.brokerId());
            expectedRecovery = recovering && awaitingRecoveryBrokers.contains(request.brokerId());
        }
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
        synchronized (brokerLeaseLock) {
            brokerLeases.put(
                    request.brokerId(), new BrokerLease(entry.brokerEpoch, deadlineFrom(now)));
            awaitingRecoveryBrokers.remove(request.brokerId());
        }
        return new BrokerRegistration(
                ErrorCode.NONE, new RegisterBrokerResponse(entry.brokerEpoch, leaseTimeoutMs));
    }

    public ErrorCode heartbeatBroker(BrokerHeartbeatRequest request) {
        synchronized (brokerLeaseLock) {
            BrokerLease lease = brokerLeases.get(request.brokerId());
            long now = nanoTime.getAsLong();
            if (lease == null || lease.brokerEpoch() != request.brokerEpoch()
                    || now >= lease.deadlineNanos()) {
                return ErrorCode.BROKER_FENCED;
            }
            brokerLeases.put(
                    request.brokerId(), new BrokerLease(request.brokerEpoch(), deadlineFrom(now)));
            awaitingRecoveryBrokers.remove(request.brokerId());
            return ErrorCode.NONE;
        }
    }

    public synchronized int expireOverdueBrokerLeases() {
        if (recovering) {
            return 0;
        }
        long now = nanoTime.getAsLong();
        List<BrokerLeaseKey> expired;
        synchronized (brokerLeaseLock) {
            expired = brokerLeases.entrySet().stream()
                    .filter(entry -> now >= entry.getValue().deadlineNanos())
                    .map(entry -> new BrokerLeaseKey(entry.getKey(), entry.getValue().brokerEpoch()))
                    .toList();
        }
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
        BrokerLease lease;
        synchronized (brokerLeaseLock) {
            lease = brokerLeases.get(brokerId);
        }
        if (current == null || current.brokerEpoch != expectedEpoch
                || lease == null || lease.brokerEpoch() != expectedEpoch) {
            return false;
        }
        if (!current.online) {
            synchronized (brokerLeaseLock) {
                brokerLeases.remove(brokerId, lease);
            }
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
                    partition.activationRequired = false;
                    partition.leaderEpoch = Math.addExact(partition.leaderEpoch, 1L);
                    partition.leaderEpochStartOffset = cleanRecoveryOffset;
                    partition.leaderReady = false;
                    invalidateWatermarkSnapshot(partition);
                }
            }
        }

        if (commit(next) != ErrorCode.NONE) {
            return false;
        }
        synchronized (brokerLeaseLock) {
            brokerLeases.remove(brokerId, lease);
        }
        audit(
                FailoverAuditEventType.BROKER_LEASE_EXPIRED,
                brokerId,
                null,
                null,
                "online",
                "offline",
                "broker lease expired at epoch " + expectedEpoch);
        return true;
    }

    public synchronized ErrorCode createTopic(CreateTopicRequest request) {
        if (recovering) {
            return ErrorCode.CONTROLLER_NOT_READY;
        }
        if (!TopicNames.isValid(request.topicName())
                || request.numPartitions() <= 0 || request.replicationFactor() <= 0) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (metadata.topics.containsKey(request.topicName())
                || metadata.topicTombstones.containsKey(request.topicName())
                || hasPortableTopicNameCollision(request.topicName())) {
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
            return ErrorCode.UNKNOWN_BROKER;
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
                    partition.activationRequired = false;
                    invalidateWatermarkSnapshot(partition);
                }
            }
        }
        ErrorCode commitError = commit(next);
        if (commitError != ErrorCode.NONE) {
            return commitError;
        }
        synchronized (brokerLeaseLock) {
            brokerLeases.remove(brokerId);
        }
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
            auditStaleEpoch(request.topicName(), request.partitionId(),
                    request.brokerId(), request.controllerEpoch(), metadata.controllerEpoch,
                    "controller epoch rejected during leader-ready");
            return ErrorCode.CONTROLLER_FENCED;
        }
        BrokerEntry broker = metadata.brokers.get(request.brokerId());
        if (broker == null || !broker.online || broker.brokerEpoch != request.brokerEpoch()) {
            if (broker != null && broker.brokerEpoch != request.brokerEpoch()) {
                auditStaleEpoch(request.topicName(), request.partitionId(),
                        request.brokerId(), request.brokerEpoch(), broker.brokerEpoch,
                        "broker epoch rejected during leader-ready");
            }
            return ErrorCode.BROKER_FENCED;
        }
        ClusterMetadata next = copyMetadata(metadata);
        PartitionEntry partition = getPartitionEntry(next, request.topicName(), request.partitionId());
        if (partition == null || partition.leaderBrokerId != request.brokerId()) {
            return ErrorCode.NOT_LEADER;
        }
        if (partition.leaderEpoch != request.leaderEpoch()) {
            auditStaleEpoch(request.topicName(), request.partitionId(),
                    request.brokerId(), request.leaderEpoch(), partition.leaderEpoch,
                    "leader epoch rejected during leader-ready");
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        if (request.logEndOffset() < partition.leaderEpochStartOffset) {
            return ErrorCode.LEADER_NOT_READY;
        }
        if (partition.leaderReady) {
            return ErrorCode.NONE;
        }
        partition.leaderReady = true;
        partition.activationRequired = false;
        return commit(next);
    }

    /**
     * Applies a Controller-committed LEADER_READY result to the local cache.
     * Etcd-mode {@link com.minimq.broker.MetadataSync#refresh} is a no-op, so
     * the same produce/fetch would otherwise still see {@code leaderReady=false}.
     */
    public synchronized void noteCommittedLeaderReady(
            String topicName, int partitionId, long leaderEpoch) {
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null || partition.frozen || partition.leaderEpoch != leaderEpoch) {
            return;
        }
        partition.leaderReady = true;
        partition.activationRequired = false;
    }

    /** Creates a topic through the V4 scoped-admin API. */
    public synchronized ErrorCode createTopicAdmin(CreateTopicAdminRequest request) {
        if (request == null
                || !TopicNames.isValid(request.topicName())
                || request.numPartitions() < 1
                || request.numPartitions() > 10_000
                || request.replicationFactor() < 1
                || request.replicationFactor() > 10_000) {
            return ErrorCode.INVALID_REQUEST;
        }
        return createTopic(new CreateTopicRequest(
                request.topicName(), request.numPartitions(), request.replicationFactor()));
    }

    /** Mutates explicit retry/DLQ links for one active source topic. */
    public synchronized ReliabilityConfigResult configureTopicReliability(
            ConfigureTopicReliabilityRequest request) {
        if (recovering) {
            return reliabilityResult(ErrorCode.CONTROLLER_NOT_READY, request);
        }
        if (request == null
                || !TopicNames.isValidReference(request.sourceTopicName())) {
            return new ReliabilityConfigResult(ErrorCode.INVALID_REQUEST, null, null, metadata.metadataVersion);
        }
        String sourceTopic = request.sourceTopicName();
        if (!metadata.topics.containsKey(sourceTopic)) {
            return reliabilityResult(ErrorCode.UNKNOWN_TOPIC, request);
        }

        ClusterMetadata next = copyMetadata(metadata);
        ErrorCode retryError = applyReliabilityLink(
                next,
                next.retryTopicBySource,
                sourceTopic,
                request.retryTopicSupplied(),
                request.retryTopicName(),
                request.clearRetryTopic());
        if (retryError != ErrorCode.NONE) {
            return reliabilityResult(retryError, request);
        }
        ErrorCode deadLetterError = applyReliabilityLink(
                next,
                next.deadLetterTopicBySource,
                sourceTopic,
                request.deadLetterTopicSupplied(),
                request.deadLetterTopicName(),
                request.clearDeadLetterTopic());
        if (deadLetterError != ErrorCode.NONE) {
            return reliabilityResult(deadLetterError, request);
        }
        if (hasReliabilityCycle(next)) {
            return reliabilityResult(ErrorCode.INVALID_REQUEST, request);
        }

        if (metadata.retryTopicBySource.equals(next.retryTopicBySource)
                && metadata.deadLetterTopicBySource.equals(next.deadLetterTopicBySource)) {
            return new ReliabilityConfigResult(
                    ErrorCode.NONE,
                    metadata.retryTopicBySource.get(sourceTopic),
                    metadata.deadLetterTopicBySource.get(sourceTopic),
                    metadata.metadataVersion);
        }
        ErrorCode error = commit(next);
        return new ReliabilityConfigResult(
                error,
                error == ErrorCode.NONE ? metadata.retryTopicBySource.get(sourceTopic) : null,
                error == ErrorCode.NONE ? metadata.deadLetterTopicBySource.get(sourceTopic) : null,
                metadata.metadataVersion);
    }

    /**
     * Appends partitions without rebuilding existing entries.  The returned
     * version is the controller's committed metadata version when successful.
     */
    public synchronized IncreasePartitionsResult increasePartitions(
            String topicName, int targetPartitionCount) {
        TopicEntry currentTopic = metadata.topics.get(topicName);
        if (recovering) {
            return new IncreasePartitionsResult(
                    ErrorCode.CONTROLLER_NOT_READY, 0, 0, metadata.metadataVersion);
        }
        if (currentTopic == null) {
            return new IncreasePartitionsResult(
                    ErrorCode.UNKNOWN_TOPIC, 0, 0, metadata.metadataVersion);
        }
        int previousCount = currentTopic.partitions == null ? 0 : currentTopic.partitions.size();
        if (targetPartitionCount <= previousCount || targetPartitionCount > 10_000) {
            return new IncreasePartitionsResult(
                    ErrorCode.INVALID_REQUEST, previousCount, previousCount, metadata.metadataVersion);
        }
        List<BrokerEntry> onlineBrokers = metadata.brokers.values().stream()
                .filter(b -> b.online)
                .sorted(Comparator.comparingInt(b -> b.brokerId))
                .toList();
        if (onlineBrokers.isEmpty()) {
            return new IncreasePartitionsResult(
                    ErrorCode.INVALID_REQUEST, previousCount, previousCount, metadata.metadataVersion);
        }
        int replicationFactor = currentTopic.partitions.isEmpty()
                ? 1
                : currentTopic.partitions.get(0).replicaBrokerIds.size();
        if (replicationFactor < 1 || replicationFactor > onlineBrokers.size()) {
            return new IncreasePartitionsResult(
                    ErrorCode.INVALID_REQUEST, previousCount, previousCount, metadata.metadataVersion);
        }

        ClusterMetadata next = copyMetadata(metadata);
        TopicEntry topic = next.topics.get(topicName);
        for (int partitionId = previousCount; partitionId < targetPartitionCount; partitionId++) {
            PartitionEntry partition = new PartitionEntry();
            partition.partitionId = partitionId;
            int firstReplicaIndex = partitionId % onlineBrokers.size();
            List<Integer> replicas = new ArrayList<>(replicationFactor);
            for (int replica = 0; replica < replicationFactor; replica++) {
                replicas.add(onlineBrokers
                        .get((firstReplicaIndex + replica) % onlineBrokers.size()).brokerId);
            }
            partition.replicaBrokerIds = replicas;
            partition.isrBrokerIds = new ArrayList<>(replicas);
            partition.leaderBrokerId = replicas.get(0);
            for (int replicaId : replicas) {
                partition.replicaLogEndOffsets.put(replicaId, 0L);
            }
            topic.partitions.add(partition);
        }
        ErrorCode error = commit(next);
        return new IncreasePartitionsResult(
                error,
                previousCount,
                error == ErrorCode.NONE ? targetPartitionCount : previousCount,
                error == ErrorCode.NONE ? metadata.metadataVersion : metadata.metadataVersion);
    }

    /** Returns active topic names in deterministic order. */
    public synchronized List<String> listTopicNames() {
        return metadata.topics.keySet().stream().sorted().toList();
    }

    private ReliabilityConfigResult reliabilityResult(
            ErrorCode errorCode, ConfigureTopicReliabilityRequest request) {
        String sourceTopic = request == null ? null : request.sourceTopicName();
        return new ReliabilityConfigResult(
                errorCode,
                sourceTopic == null ? null : metadata.retryTopicBySource.get(sourceTopic),
                sourceTopic == null ? null : metadata.deadLetterTopicBySource.get(sourceTopic),
                metadata.metadataVersion);
    }

    private static ErrorCode applyReliabilityLink(
            ClusterMetadata metadata,
            Map<String, String> links,
            String sourceTopic,
            boolean supplied,
            String targetTopic,
            boolean clear) {
        if (clear) {
            links.remove(sourceTopic);
            return ErrorCode.NONE;
        }
        if (!supplied) {
            return ErrorCode.NONE;
        }
        if (!TopicNames.isValidReference(targetTopic)) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (!metadata.topics.containsKey(targetTopic)) {
            return ErrorCode.UNKNOWN_TOPIC;
        }
        if (sourceTopic.equals(targetTopic)) {
            return ErrorCode.INVALID_REQUEST;
        }
        links.put(sourceTopic, targetTopic);
        return ErrorCode.NONE;
    }

    private static boolean hasReliabilityCycle(ClusterMetadata metadata) {
        Set<String> topics = metadata.topics.keySet();
        Set<String> checked = new HashSet<>();
        for (String topicName : topics) {
            if (visitsReliabilityCycle(topicName, metadata, checked, new HashSet<>())) {
                return true;
            }
        }
        return false;
    }

    private static boolean visitsReliabilityCycle(
            String topicName,
            ClusterMetadata metadata,
            Set<String> checked,
            Set<String> path) {
        if (!metadata.topics.containsKey(topicName)) {
            return false;
        }
        if (checked.contains(topicName)) {
            return false;
        }
        if (!path.add(topicName)) {
            return true;
        }
        String retryTopic = metadata.retryTopicBySource.get(topicName);
        if (retryTopic != null && visitsReliabilityCycle(retryTopic, metadata, checked, path)) {
            return true;
        }
        String deadLetterTopic = metadata.deadLetterTopicBySource.get(topicName);
        if (deadLetterTopic != null && visitsReliabilityCycle(deadLetterTopic, metadata, checked, path)) {
            return true;
        }
        path.remove(topicName);
        checked.add(topicName);
        return false;
    }

    public synchronized long metadataVersion() {
        return metadata.metadataVersion;
    }

    public synchronized ClusterMetadata.TopicTombstone getTopicTombstone(String topicName) {
        ClusterMetadata.TopicTombstone tombstone = metadata.topicTombstones.get(topicName);
        return tombstone == null ? null : copyTombstone(tombstone);
    }

    /** Returns a deep copy of an active topic, or null when it is absent. */
    public synchronized TopicEntry getActiveTopic(String topicName) {
        TopicEntry topic = metadata.topics.get(topicName);
        return topic == null ? null : copyTopic(topic);
    }

    /**
     * Atomically removes active topic state and installs its durable tombstone.
     * The tombstone version is the metadata version allocated by this first
     * deletion CAS, not the later DELETED transition version.
     */
    public synchronized ErrorCode deleteTopic(String topicName, long deletedAtEpochMs) {
        return deleteTopicInternal(topicName, deletedAtEpochMs, null);
    }

    public ErrorCode deleteTopic(
            String topicName, long deletedAtEpochMs, Duration timeout) {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        long deadlineNanos = mutationDeadline(timeout);
        synchronized (this) {
            return deleteTopicInternal(topicName, deletedAtEpochMs, deadlineNanos);
        }
    }

    private ErrorCode deleteTopicInternal(
            String topicName, long deletedAtEpochMs, Long deadlineNanos) {
        if (!TopicNames.isValidReference(topicName) || deletedAtEpochMs < 0) {
            return ErrorCode.INVALID_REQUEST;
        }
        ClusterMetadata.TopicTombstone existing = metadata.topicTombstones.get(topicName);
        if (existing != null) {
            if ("DELETING".equals(existing.state) && existing.requiredBrokerIds.isEmpty()) {
                ClusterMetadata terminal = copyMetadata(metadata);
                terminal.topicTombstones.get(topicName).state = "DELETED";
                return commit(terminal, deadlineNanos);
            }
            return ErrorCode.NONE;
        }
        if (!metadata.topics.containsKey(topicName)) {
            return ErrorCode.UNKNOWN_TOPIC;
        }
        ClusterMetadata next = copyMetadata(metadata);
        next.topics.remove(topicName);
        next.consumerGroups.entrySet().removeIf(entry ->
                topicName.equals(entry.getValue().topicName));
        next.retryTopicBySource.entrySet().removeIf(entry -> topicName.equals(entry.getKey()));
        next.deadLetterTopicBySource.entrySet().removeIf(entry -> topicName.equals(entry.getKey()));
        next.retryTopicBySource.entrySet().removeIf(entry -> topicName.equals(entry.getValue()));
        next.deadLetterTopicBySource.entrySet().removeIf(entry -> topicName.equals(entry.getValue()));

        ClusterMetadata.TopicTombstone tombstone = new ClusterMetadata.TopicTombstone();
        tombstone.topicName = topicName;
        tombstone.state = "DELETING";
        tombstone.tombstoneVersion = Math.addExact(metadata.metadataVersion, 1L);
        tombstone.deletedAtEpochMs = deletedAtEpochMs;
        metadata.brokers.values().stream()
                .filter(broker -> broker.online)
                .map(broker -> broker.brokerId)
                .sorted()
                .forEach(tombstone.requiredBrokerIds::add);
        next.topicTombstones.put(topicName, tombstone);
        ErrorCode error = commit(next, deadlineNanos);
        if (error != ErrorCode.NONE || !tombstone.requiredBrokerIds.isEmpty()) {
            return error;
        }
        // No online broker needs to clean local files; persist the terminal
        // state as a second CAS so clients can query DELETED durably.
        ClusterMetadata terminal = copyMetadata(metadata);
        terminal.topicTombstones.get(topicName).state = "DELETED";
        return commit(terminal, deadlineNanos);
    }

    /** Applies one broker's idempotent local-cleanup acknowledgement. */
    public synchronized ErrorCode acknowledgeTopicDeletion(
            String topicName,
            long tombstoneVersion,
            int brokerId,
            long brokerEpoch,
            long controllerEpoch) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null) {
            return ErrorCode.UNKNOWN_BROKER;
        }
        if (controllerEpoch != metadata.controllerEpoch) {
            return ErrorCode.CONTROLLER_FENCED;
        }
        if (!broker.online || broker.brokerEpoch != brokerEpoch) {
            return ErrorCode.BROKER_FENCED;
        }
        ClusterMetadata.TopicTombstone current = metadata.topicTombstones.get(topicName);
        if (current == null || current.tombstoneVersion != tombstoneVersion) {
            return ErrorCode.STALE_TOMBSTONE;
        }
        if (current.acknowledgedBrokerIds.contains(brokerId)
                && "DELETED".equals(current.state)) {
            return ErrorCode.NONE;
        }
        ClusterMetadata next = copyMetadata(metadata);
        ClusterMetadata.TopicTombstone tombstone = next.topicTombstones.get(topicName);
        boolean changed = tombstone.acknowledgedBrokerIds.add(brokerId);
        if ("DELETING".equals(tombstone.state)
                && tombstone.acknowledgedBrokerIds.containsAll(tombstone.requiredBrokerIds)) {
            tombstone.state = "DELETED";
            changed = true;
        }
        return changed ? commit(next) : ErrorCode.NONE;
    }

    private long deadlineFrom(long now) {
        return Math.addExact(now, TimeUnit.MILLISECONDS.toNanos(leaseTimeoutMs));
    }

    /** Starts the takeover grace period during which persisted online brokers may re-register. */
    public synchronized void beginRecovery(long graceMs) {
        if (graceMs < 0) {
            throw new IllegalArgumentException("recovery grace must be non-negative");
        }
        long now = nanoTime.getAsLong();
        recovering = true;
        recoveryDeadlineNanos = Math.addExact(
                now, TimeUnit.MILLISECONDS.toNanos(graceMs));
        synchronized (brokerLeaseLock) {
            brokerLeases.clear();
            awaitingRecoveryBrokers.clear();
            metadata.brokers.values().stream()
                    .filter(broker -> broker.online)
                    .forEach(broker -> {
                        brokerLeases.put(
                                broker.brokerId,
                                new BrokerLease(broker.brokerEpoch, deadlineFrom(now)));
                        awaitingRecoveryBrokers.add(broker.brokerId);
                    });
        }
    }

    /** Ends recovery when due and expires only brokers that did not re-register. */
    public synchronized boolean advanceRecoveryWindow() {
        if (!recovering || nanoTime.getAsLong() < recoveryDeadlineNanos) {
            return false;
        }
        recovering = false;
        long now = nanoTime.getAsLong();
        List<Integer> missing;
        synchronized (brokerLeaseLock) {
            missing = List.copyOf(awaitingRecoveryBrokers);
            awaitingRecoveryBrokers.clear();
        }
        for (int brokerId : missing) {
            BrokerEntry broker = metadata.brokers.get(brokerId);
            if (broker != null && broker.online) {
                synchronized (brokerLeaseLock) {
                    brokerLeases.put(brokerId, new BrokerLease(broker.brokerEpoch, now));
                }
                expireBrokerLease(brokerId, broker.brokerEpoch);
            }
        }
        return true;
    }

    /** Freezes candidates that never complete the exact-epoch LEADER_READY handshake. */
    public synchronized int expireLeaderActivationTimeouts(long timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("leader activation timeout must be positive");
        }
        long now = nanoTime.getAsLong();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        ClusterMetadata next = copyMetadata(metadata);
        Set<String> pending = new HashSet<>();
        int expired = 0;
        for (Map.Entry<String, TopicEntry> topicEntry : next.topics.entrySet()) {
            List<PartitionEntry> partitions = topicEntry.getValue().partitions;
            for (int partitionId = 0; partitionId < partitions.size(); partitionId++) {
                PartitionEntry partition = partitions.get(partitionId);
                String key = topicEntry.getKey() + "\u0000" + partitionId;
                if (partition.frozen || partition.leaderReady) {
                    leaderActivationDeadlines.remove(key);
                    continue;
                }
                pending.add(key);
                LeaderActivationDeadline deadline = leaderActivationDeadlines.get(key);
                if (deadline == null || deadline.leaderEpoch() != partition.leaderEpoch) {
                    deadline = new LeaderActivationDeadline(
                            partition.leaderEpoch, Math.addExact(now, timeoutNanos));
                    leaderActivationDeadlines.put(key, deadline);
                }
                if (now >= deadline.deadlineNanos()) {
                    partition.frozen = true;
                    partition.activationRequired = true;
                    partition.leaderReady = false;
                    partition.isrBrokerIds.clear();
                    leaderActivationDeadlines.remove(key);
                    expired++;
                }
            }
        }
        leaderActivationDeadlines.keySet().removeIf(key -> !pending.contains(key));
        if (expired == 0) {
            return 0;
        }
        return commit(next) == ErrorCode.NONE ? expired : 0;
    }

    public synchronized boolean isRecovering() {
        return recovering;
    }

    private static void restoreFrozenPartitions(ClusterMetadata metadata, int brokerId) {
        for (TopicEntry topic : metadata.topics.values()) {
            for (PartitionEntry partition : topic.partitions) {
                if (partition.frozen && partition.leaderBrokerId == brokerId) {
                    boolean requireActivation = partition.activationRequired;
                    partition.frozen = false;
                    partition.activationRequired = false;
                    partition.leaderReady = !requireActivation;
                    partition.isrBrokerIds = new ArrayList<>(List.of(brokerId));
                    invalidateWatermarkSnapshot(partition);
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
            return ErrorCode.UNKNOWN_BROKER;
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
            auditStaleEpoch(topicName, partitionId, brokerId, controllerEpoch,
                    metadata.controllerEpoch, "controller epoch rejected for replica progress");
            return ErrorCode.CONTROLLER_FENCED;
        }
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null || !broker.online || broker.brokerEpoch != brokerEpoch) {
            if (broker != null && broker.brokerEpoch != brokerEpoch) {
                auditStaleEpoch(topicName, partitionId, brokerId, brokerEpoch, broker.brokerEpoch,
                        "broker epoch rejected for replica progress");
            }
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

    /** Installs an externally revisioned snapshot without writing it back. */
    public synchronized void installMetadata(VersionedClusterMetadata value) {
        if (value.revision() < metadataRevision) {
            return;
        }
        metadata = copyMetadata(value.metadata());
        metadataRevision = value.revision();
        normalizeMetadata();
    }

    private ErrorCode commit(ClusterMetadata next) {
        return applyCommitResult(
                metadataRepository.compareAndSet(metadataRevision, next));
    }

    private ErrorCode commit(ClusterMetadata next, Long deadlineNanos) {
        if (deadlineNanos == null) {
            return commit(next);
        }
        long remainingNanos = deadlineNanos - nanoTime.getAsLong();
        if (remainingNanos <= 0L) {
            return ErrorCode.CONTROLLER_NOT_READY;
        }
        return applyCommitResult(
                metadataRepository.compareAndSet(
                        metadataRevision, next, Duration.ofNanos(remainingNanos)));
    }

    private ErrorCode applyCommitResult(ClusterMetadataRepository.UpdateResult result) {
        ClusterMetadata previous = metadata;
        if (result.errorCode() != ErrorCode.NONE) {
            return result.errorCode();
        }
        metadata = copyMetadata(result.value().metadata());
        metadataRevision = result.value().revision();
        try {
            auditMetadataTransitions(previous, metadata);
        } catch (RuntimeException ignored) {
            // A malformed diagnostic projection must not report a committed mutation as failed.
        }
        return ErrorCode.NONE;
    }

    private long mutationDeadline(Duration timeout) {
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long now = nanoTime.getAsLong();
        try {
            return Math.addExact(now, timeoutNanos);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Records transitions at the metadata mutation boundary. The audit ring is deliberately
     * best-effort: failures are swallowed by {@link #audit} and never change the committed state.
     */
    private void auditMetadataTransitions(ClusterMetadata previous, ClusterMetadata current) {
        List<String> topicNames = new ArrayList<>(previous.topics.keySet());
        topicNames.retainAll(current.topics.keySet());
        topicNames.sort(String::compareTo);
        for (String topicName : topicNames) {
            TopicEntry oldTopic = previous.topics.get(topicName);
            TopicEntry newTopic = current.topics.get(topicName);
            for (PartitionEntry oldPartition : oldTopic.partitions) {
                PartitionEntry newPartition = newTopic.partitions.stream()
                        .filter(candidate -> candidate.partitionId == oldPartition.partitionId)
                        .findFirst()
                        .orElse(null);
                if (newPartition == null) {
                    continue;
                }
                List<Integer> oldIsr = oldPartition.isrBrokerIds == null
                        ? List.of() : List.copyOf(oldPartition.isrBrokerIds);
                List<Integer> newIsr = newPartition.isrBrokerIds == null
                        ? List.of() : List.copyOf(newPartition.isrBrokerIds);
                if (oldPartition.leaderBrokerId != newPartition.leaderBrokerId) {
                    audit(
                            FailoverAuditEventType.LEADER_CHANGED,
                            newPartition.leaderBrokerId,
                            topicName,
                            oldPartition.partitionId,
                            String.valueOf(oldPartition.leaderBrokerId),
                            String.valueOf(newPartition.leaderBrokerId),
                            "leader epoch " + oldPartition.leaderEpoch + " -> "
                                    + newPartition.leaderEpoch);
                }
                Set<Integer> oldIsrSet = new HashSet<>(oldIsr);
                Set<Integer> newIsrSet = new HashSet<>(newIsr);
                if (newIsr.size() < oldIsr.size() || !newIsrSet.containsAll(oldIsrSet)) {
                    audit(
                            FailoverAuditEventType.ISR_SHRUNK,
                            null,
                            topicName,
                            oldPartition.partitionId,
                            oldIsr.toString(),
                            newIsr.toString(),
                            "ISR membership shrank");
                }
                if (newIsr.size() > oldIsr.size() || !oldIsrSet.containsAll(newIsrSet)) {
                    audit(
                            FailoverAuditEventType.ISR_EXPANDED,
                            null,
                            topicName,
                            oldPartition.partitionId,
                            oldIsr.toString(),
                            newIsr.toString(),
                            "ISR membership expanded");
                }
                if (!oldPartition.leaderReady && newPartition.leaderReady) {
                    audit(
                            FailoverAuditEventType.LEADER_READY,
                            newPartition.leaderBrokerId,
                            topicName,
                            oldPartition.partitionId,
                            "false",
                            "true",
                            "leader readiness handshake completed");
                } else if (oldPartition.leaderReady && !newPartition.leaderReady) {
                    audit(
                            FailoverAuditEventType.LEADER_NOT_READY,
                            newPartition.leaderBrokerId,
                            topicName,
                            oldPartition.partitionId,
                            "true",
                            "false",
                            "leader readiness revoked");
                } else if (!oldPartition.frozen && newPartition.frozen) {
                    audit(
                            FailoverAuditEventType.LEADER_NOT_READY,
                            newPartition.leaderBrokerId,
                            topicName,
                            oldPartition.partitionId,
                            "active",
                            "frozen",
                            "leader activation timed out");
                }
            }
        }
    }

    private void audit(
            FailoverAuditEventType eventType,
            Integer brokerId,
            String topicName,
            Integer partitionId,
            String oldValue,
            String newValue,
            String detail) {
        if (auditLog == null) {
            return;
        }
        try {
            auditLog.append(eventType, brokerId, topicName, partitionId, oldValue, newValue, detail);
        } catch (RuntimeException ignored) {
            // Audit is operational evidence and must never break the control-plane transition.
        }
    }

    private void auditStaleEpoch(
            String topicName,
            int partitionId,
            int brokerId,
            long suppliedEpoch,
            long currentEpoch,
            String detail) {
        audit(
                FailoverAuditEventType.STALE_EPOCH_REJECTED,
                brokerId,
                topicName,
                partitionId,
                String.valueOf(suppliedEpoch),
                String.valueOf(currentEpoch),
                detail);
    }

    /**
     * Accepts a V4 replica progress report.  Legacy LEO state is kept in sync for
     * V3 metadata consumers, while visible watermarks are only published after
     * every current ISR member has reported for the current controller/leader epoch.
     */
    public synchronized ErrorCode updateReplicaStateV2(
            String topicName,
            int partitionId,
            int brokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
        BrokerEntry broker = metadata.brokers.get(brokerId);
        if (broker == null) {
            return ErrorCode.UNKNOWN_BROKER;
        }
        PartitionEntry currentPartition = getPartitionEntry(topicName, partitionId);
        if (currentPartition == null) {
            return metadata.topics.containsKey(topicName)
                    ? ErrorCode.UNKNOWN_PARTITION
                    : ErrorCode.UNKNOWN_TOPIC;
        }
        if (!currentPartition.replicaBrokerIds.contains(brokerId)) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (!broker.online || broker.brokerEpoch != brokerEpoch) {
            if (broker.brokerEpoch != brokerEpoch) {
                auditStaleEpoch(topicName, partitionId, brokerId, brokerEpoch, broker.brokerEpoch,
                        "broker epoch rejected for V2 replica progress");
            }
            return ErrorCode.BROKER_FENCED;
        }
        if (controllerEpoch != metadata.controllerEpoch) {
            auditStaleEpoch(topicName, partitionId, brokerId, controllerEpoch,
                    metadata.controllerEpoch, "controller epoch rejected for V2 replica progress");
            return ErrorCode.CONTROLLER_FENCED;
        }
        if (currentPartition.frozen || !currentPartition.leaderReady) {
            return ErrorCode.LEADER_NOT_READY;
        }
        if (leaderEpoch != currentPartition.leaderEpoch) {
            auditStaleEpoch(topicName, partitionId, brokerId, leaderEpoch,
                    currentPartition.leaderEpoch, "leader epoch rejected for V2 replica progress");
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        if (physicalLogEndOffset < 0
                || visibleLogEndOffset < 0
                || visibleLogEndOffset > physicalLogEndOffset
                || (visibleLogEndOffset == physicalLogEndOffset
                        ? nextVisibleAtEpochMs != -1L
                        : nextVisibleAtEpochMs < 0L)) {
            return ErrorCode.INVALID_REQUEST;
        }

        ClusterMetadata next = copyMetadata(metadata);
        PartitionEntry partition = getPartitionEntry(next, topicName, partitionId);
        ensureReplicaProgressState(next, partition);
        ClusterMetadata.ReplicaProgressEntry previous =
                partition.replicaProgress.get(brokerId);
        ClusterMetadata.WatermarkSnapshot previousWatermark =
                copyWatermark(partition.lastCompleteWatermark);
        boolean sameReport = previous != null
                && previous.brokerEpoch == brokerEpoch
                && previous.controllerEpoch == controllerEpoch
                && previous.leaderEpoch == leaderEpoch
                && previous.physicalLogEndOffset == physicalLogEndOffset
                && previous.visibleLogEndOffset == visibleLogEndOffset
                && previous.nextVisibleAtEpochMs == nextVisibleAtEpochMs;

        ClusterMetadata.ReplicaProgressEntry progress =
                new ClusterMetadata.ReplicaProgressEntry();
        progress.brokerId = brokerId;
        progress.physicalLogEndOffset = physicalLogEndOffset;
        progress.visibleLogEndOffset = visibleLogEndOffset;
        progress.nextVisibleAtEpochMs = nextVisibleAtEpochMs;
        progress.brokerEpoch = brokerEpoch;
        progress.controllerEpoch = controllerEpoch;
        progress.leaderEpoch = leaderEpoch;
        partition.replicaProgress.put(brokerId, progress);
        partition.replicaLogEndOffsets.put(brokerId, physicalLogEndOffset);
        // A physical progress report can move a replica into or out of the ISR.
        // Reconcile before evaluating completeness so the watermark uses the
        // authoritative current-ISR set from this same metadata snapshot.
        reconcileIsrByLeo(next, partition);

        if (hasCompleteCurrentIsrReport(next, partition)) {
            updateCompleteWatermarkSnapshot(next, partition);
        } else {
            invalidateWatermarkSnapshot(partition);
        }
        if (sameReport && watermarkEquals(previousWatermark, partition.lastCompleteWatermark)) {
            return ErrorCode.NONE;
        }
        return commit(next);
    }

    private boolean hasPortableTopicNameCollision(String candidate) {
        String identity = TopicNames.portableStorageIdentity(candidate);
        return metadata.topics.keySet().stream()
                .anyMatch(existing -> TopicNames.portableStorageIdentity(existing).equals(identity))
                || metadata.topicTombstones.keySet().stream()
                .anyMatch(existing -> TopicNames.portableStorageIdentity(existing).equals(identity));
    }

    public synchronized ErrorCode validateReplicaFetchV2(
            String topicName,
            int partitionId,
            int leaderBrokerId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch) {
        ErrorCode identityError = validateReplicaFetchV2Identity(
                topicName,
                partitionId,
                replicaBrokerId,
                brokerEpoch,
                controllerEpoch);
        if (identityError != ErrorCode.NONE) {
            return identityError;
        }
        PartitionEntry partition = getPartitionEntry(topicName, partitionId);
        if (partition == null) {
            return metadata.topics.containsKey(topicName)
                    ? ErrorCode.UNKNOWN_PARTITION
                    : ErrorCode.UNKNOWN_TOPIC;
        }
        if (partition.leaderBrokerId != leaderBrokerId) {
            return ErrorCode.NOT_LEADER;
        }
        if (replicaBrokerId == leaderBrokerId
                || !partition.replicaBrokerIds.contains(replicaBrokerId)) {
            return ErrorCode.INVALID_REQUEST;
        }
        if (partition.frozen || !partition.leaderReady) {
            return ErrorCode.LEADER_NOT_READY;
        }
        if (leaderEpoch != partition.leaderEpoch) {
            auditStaleEpoch(
                    topicName,
                    partitionId,
                    replicaBrokerId,
                    leaderEpoch,
                    partition.leaderEpoch,
                    "leader epoch rejected for V2 replica fetch");
            return ErrorCode.STALE_LEADER_EPOCH;
        }
        return ErrorCode.NONE;
    }

    public synchronized ErrorCode validateReplicaFetchV2Identity(
            String topicName,
            int partitionId,
            int replicaBrokerId,
            long brokerEpoch,
            long controllerEpoch) {
        BrokerEntry broker = metadata.brokers.get(replicaBrokerId);
        if (broker == null) {
            return ErrorCode.UNKNOWN_BROKER;
        }
        if (!broker.online || broker.brokerEpoch != brokerEpoch) {
            if (broker.brokerEpoch != brokerEpoch) {
                auditStaleEpoch(
                        topicName,
                        partitionId,
                        replicaBrokerId,
                        brokerEpoch,
                        broker.brokerEpoch,
                        "broker epoch rejected for V2 replica fetch");
            }
            return ErrorCode.BROKER_FENCED;
        }
        if (controllerEpoch != metadata.controllerEpoch) {
            auditStaleEpoch(
                    topicName,
                    partitionId,
                    replicaBrokerId,
                    controllerEpoch,
                    metadata.controllerEpoch,
                    "controller epoch rejected for V2 replica fetch");
            return ErrorCode.CONTROLLER_FENCED;
        }
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
        if (partition.leaderEpoch == requestLeaderEpoch) {
            return ErrorCode.NONE;
        }
        auditStaleEpoch(topicName, partitionId, partition.leaderBrokerId,
                requestLeaderEpoch, partition.leaderEpoch,
                "data-plane request used a stale leader epoch");
        return ErrorCode.STALE_LEADER_EPOCH;
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
        ensureReplicaProgressState(source, partition);
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

    private static void ensureReplicaProgressState(
            ClusterMetadata source, PartitionEntry partition) {
        if (partition.replicaProgress == null) {
            partition.replicaProgress = new java.util.HashMap<>();
        }
        if (partition.lastCompleteWatermark == null) {
            partition.lastCompleteWatermark = new ClusterMetadata.WatermarkSnapshot();
        }
        for (int replicaId : partition.replicaBrokerIds) {
            partition.replicaLogEndOffsets.putIfAbsent(replicaId, 0L);
            partition.replicaProgress.computeIfAbsent(replicaId, id -> {
                ClusterMetadata.ReplicaProgressEntry progress =
                        new ClusterMetadata.ReplicaProgressEntry();
                progress.brokerId = id;
                progress.physicalLogEndOffset =
                        partition.replicaLogEndOffsets.getOrDefault(id, 0L);
                progress.visibleLogEndOffset = -1L;
                progress.nextVisibleAtEpochMs = -1L;
                progress.brokerEpoch = 0L;
                progress.controllerEpoch = source.controllerEpoch;
                progress.leaderEpoch = partition.leaderEpoch;
                return progress;
            });
        }
    }

    private static boolean hasCompleteCurrentIsrReport(
            ClusterMetadata source, PartitionEntry partition) {
        if (partition.isrBrokerIds == null || partition.isrBrokerIds.isEmpty()) {
            return false;
        }
        for (int brokerId : partition.isrBrokerIds) {
            BrokerEntry broker = source.brokers.get(brokerId);
            ClusterMetadata.ReplicaProgressEntry progress =
                    partition.replicaProgress.get(brokerId);
            if (broker == null
                    || !broker.online
                    || progress == null
                    || progress.brokerEpoch != broker.brokerEpoch
                    || progress.controllerEpoch != source.controllerEpoch
                    || progress.leaderEpoch != partition.leaderEpoch
                    || progress.visibleLogEndOffset < 0) {
                return false;
            }
        }
        return true;
    }

    private static void updateCompleteWatermarkSnapshot(
            ClusterMetadata source, PartitionEntry partition) {
        long physicalHighWatermark = Long.MAX_VALUE;
        long visibleHighWatermark = Long.MAX_VALUE;
        for (int brokerId : partition.isrBrokerIds) {
            ClusterMetadata.ReplicaProgressEntry progress =
                    partition.replicaProgress.get(brokerId);
            physicalHighWatermark = Math.min(
                    physicalHighWatermark, progress.physicalLogEndOffset);
            visibleHighWatermark = Math.min(
                    visibleHighWatermark, progress.visibleLogEndOffset);
        }
        visibleHighWatermark = Math.min(visibleHighWatermark, physicalHighWatermark);
        long nextVisibleAtEpochMs = -1L;
        if (visibleHighWatermark < physicalHighWatermark) {
            for (int brokerId : partition.isrBrokerIds) {
                ClusterMetadata.ReplicaProgressEntry progress =
                        partition.replicaProgress.get(brokerId);
                if (progress.visibleLogEndOffset == visibleHighWatermark) {
                    nextVisibleAtEpochMs = Math.max(
                            nextVisibleAtEpochMs, progress.nextVisibleAtEpochMs);
                }
            }
        }
        ClusterMetadata.WatermarkSnapshot snapshot =
                new ClusterMetadata.WatermarkSnapshot();
        snapshot.physicalHighWatermark = physicalHighWatermark;
        snapshot.visibleHighWatermark = visibleHighWatermark;
        snapshot.nextVisibleAtEpochMs = nextVisibleAtEpochMs;
        snapshot.controllerEpoch = source.controllerEpoch;
        snapshot.leaderEpoch = partition.leaderEpoch;
        partition.lastCompleteWatermark = snapshot;
    }

    private static void invalidateWatermarkSnapshot(PartitionEntry partition) {
        if (partition.lastCompleteWatermark == null) {
            partition.lastCompleteWatermark = new ClusterMetadata.WatermarkSnapshot();
            return;
        }
        partition.lastCompleteWatermark.controllerEpoch = -1L;
        partition.lastCompleteWatermark.leaderEpoch = -1L;
    }

    private static ClusterMetadata.WatermarkSnapshot copyWatermark(
            ClusterMetadata.WatermarkSnapshot source) {
        ClusterMetadata.WatermarkSnapshot copy = new ClusterMetadata.WatermarkSnapshot();
        if (source == null) {
            return copy;
        }
        copy.physicalHighWatermark = source.physicalHighWatermark;
        copy.visibleHighWatermark = source.visibleHighWatermark;
        copy.nextVisibleAtEpochMs = source.nextVisibleAtEpochMs;
        copy.controllerEpoch = source.controllerEpoch;
        copy.leaderEpoch = source.leaderEpoch;
        return copy;
    }

    private static boolean watermarkEquals(
            ClusterMetadata.WatermarkSnapshot left,
            ClusterMetadata.WatermarkSnapshot right) {
        return left.physicalHighWatermark == right.physicalHighWatermark
                && left.visibleHighWatermark == right.visibleHighWatermark
                && left.nextVisibleAtEpochMs == right.nextVisibleAtEpochMs
                && left.controllerEpoch == right.controllerEpoch
                && left.leaderEpoch == right.leaderEpoch;
    }

    // Return deep copies so callers cannot mutate controller-owned state directly.
    private static ClusterMetadata copyMetadata(ClusterMetadata source) {
        ClusterMetadata copy = new ClusterMetadata();
        copy.metadataFormatVersion = source.metadataFormatVersion;
        copy.controllerEpoch = source.controllerEpoch;
        copy.metadataVersion = source.metadataVersion;
        source.brokers.forEach((id, broker) -> copy.brokers.put(id, copyBroker(broker)));
        source.topics.forEach((name, topic) -> copy.topics.put(name, copyTopic(topic)));
        source.topicTombstones.forEach((name, tombstone) ->
                copy.topicTombstones.put(name, copyTombstone(tombstone)));
        copy.retryTopicBySource.putAll(source.retryTopicBySource);
        copy.deadLetterTopicBySource.putAll(source.deadLetterTopicBySource);
        source.consumerGroups.forEach((id, group) -> copy.consumerGroups.put(id, copyConsumerGroup(group)));
        return copy;
    }

    private static ClusterMetadata.TopicTombstone copyTombstone(
            ClusterMetadata.TopicTombstone source) {
        ClusterMetadata.TopicTombstone copy = new ClusterMetadata.TopicTombstone();
        copy.topicName = source.topicName;
        copy.state = source.state;
        copy.tombstoneVersion = source.tombstoneVersion;
        copy.deletedAtEpochMs = source.deletedAtEpochMs;
        copy.requiredBrokerIds.addAll(source.requiredBrokerIds);
        copy.acknowledgedBrokerIds.addAll(source.acknowledgedBrokerIds);
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
            partitionCopy.replicaProgress = new java.util.HashMap<>();
            partition.replicaProgress.forEach((brokerId, progress) -> {
                ClusterMetadata.ReplicaProgressEntry progressCopy =
                        new ClusterMetadata.ReplicaProgressEntry();
                progressCopy.brokerId = progress.brokerId;
                progressCopy.physicalLogEndOffset = progress.physicalLogEndOffset;
                progressCopy.visibleLogEndOffset = progress.visibleLogEndOffset;
                progressCopy.nextVisibleAtEpochMs = progress.nextVisibleAtEpochMs;
                progressCopy.brokerEpoch = progress.brokerEpoch;
                progressCopy.controllerEpoch = progress.controllerEpoch;
                progressCopy.leaderEpoch = progress.leaderEpoch;
                partitionCopy.replicaProgress.put(brokerId, progressCopy);
            });
            partitionCopy.lastCompleteWatermark = new ClusterMetadata.WatermarkSnapshot();
            partitionCopy.lastCompleteWatermark.physicalHighWatermark =
                    partition.lastCompleteWatermark.physicalHighWatermark;
            partitionCopy.lastCompleteWatermark.visibleHighWatermark =
                    partition.lastCompleteWatermark.visibleHighWatermark;
            partitionCopy.lastCompleteWatermark.nextVisibleAtEpochMs =
                    partition.lastCompleteWatermark.nextVisibleAtEpochMs;
            partitionCopy.lastCompleteWatermark.controllerEpoch =
                    partition.lastCompleteWatermark.controllerEpoch;
            partitionCopy.lastCompleteWatermark.leaderEpoch =
                    partition.lastCompleteWatermark.leaderEpoch;
            partitionCopy.frozen = partition.frozen;
            partitionCopy.activationRequired = partition.activationRequired;
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

package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.common.assignor.AssignmentSerde;
import com.minimq.common.assignor.RangeAssignor;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 同步消费者客户端，负责加入消费组、拉取分配分区、维护本地 offset 并提交消费进度。
 */
public class Consumer implements AutoCloseable {
    public static final long DEFAULT_METADATA_CACHE_MAX_AGE_MS = 30_000L;
    private final ControllerAwareNetworkClient controllerClient;
    private final Function<String, NetworkClient> brokerClientFactory;
    private final String groupId;
    private final Map<Integer, NetworkClient> brokerClients = new HashMap<>();
    private final Map<String, CachedMetadata> metadataCache = new HashMap<>();
    private String topic;
    private String memberId = "";
    private int generationId;
    private String leaderMemberId;
    private List<Integer> assignedPartitions = List.of();
    private final Map<Integer, Long> offsets = new HashMap<>();
    private ScheduledExecutorService heartbeatExecutor;

    public Consumer(String bootstrapServers, String groupId, String clientId) {
        this(bootstrapServers, groupId, clientId, NetworkClient::new);
    }

    Consumer(
            String bootstrapServers,
            String groupId,
            String clientId,
            Function<String, NetworkClient> brokerClientFactory) {
        this.groupId = groupId;
        this.brokerClientFactory = Objects.requireNonNull(
                brokerClientFactory, "brokerClientFactory");
        this.controllerClient = new ControllerAwareNetworkClient(bootstrapServers, clientId);
        try {
            this.controllerClient.connect();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public void subscribe(String topic) throws InterruptedException {
        this.topic = topic;
        joinGroup();
        startHeartbeat();
    }

    // 加入消费组后只有 leader 成员负责计算完整分配，其余成员等待同步结果。
    private void joinGroup() throws InterruptedException {
        MqDecoder.DecodedResponse joinResponse = controllerClient.send(
                ApiKeys.JOIN_GROUP, new JoinGroupRequest(groupId, memberId, topic));
        controllerClient.ensureSuccess(joinResponse);
        JoinGroupResponse join = (JoinGroupResponse) joinResponse.body();
        memberId = join.memberId();
        generationId = join.generationId();
        leaderMemberId = join.leaderMemberId();

        byte[] assignmentBytes = new byte[0];
        if (memberId.equals(leaderMemberId)) {
            assignmentBytes = encodeAssignment(join.members());
        }
        MqDecoder.DecodedResponse syncResponse = controllerClient.send(
                ApiKeys.SYNC_GROUP, new SyncGroupRequest(groupId, generationId, memberId, assignmentBytes));
        controllerClient.ensureSuccess(syncResponse);
        SyncGroupResponse sync = (SyncGroupResponse) syncResponse.body();
        assignedPartitions = AssignmentSerde.decodePartitions(sync.assignment());
        for (int partition : assignedPartitions) {
            MqDecoder.DecodedResponse offsetResponse = controllerClient.send(
                    ApiKeys.OFFSET_FETCH, new OffsetFetchRequest(groupId, topic, partition));
            controllerClient.ensureSuccess(offsetResponse);
            OffsetFetchResponse offset = (OffsetFetchResponse) offsetResponse.body();
            offsets.put(partition, offset.offset() >= 0 ? offset.offset() : 0L);
        }
    }

    private byte[] encodeAssignment(List<String> members) throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(topic);
        TopicMetadata topicMetadata = metadata.topics().stream()
                .filter(t -> t.topicName().equals(topic))
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException("Topic not found: " + topic));
        return AssignmentSerde.encodeFullAssignment(RangeAssignor.assign(
                members, topicMetadata.partitions().size()));
    }

    // poll 会根据当前分配逐分区拉取，并在本地推进下一次读取 offset。
    public List<ConsumerRecord> poll(Duration timeout) throws InterruptedException {
        if (timeout == null) {
            throw new NullPointerException("timeout");
        }
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long timeoutNanos = Math.max(0L, NetworkClient.durationToNanos(timeout));
        long deadlineNanos = NetworkClient.deadlineNanos(currentTimeNanos(),
                Duration.ofNanos(timeoutNanos));
        List<ConsumerRecord> result = new ArrayList<>();
        MetadataResponse metadata = cachedMetadata(topic, deadlineNanos);
        do {
            long nextVisibleAtEpochMs = -1L;
            for (int partition : assignedPartitions) {
                if (!hasPollTimeRemaining(deadlineNanos)) {
                    return result;
                }
                FetchPartitionResult fetched = fetchPartition(
                        partition, metadata, result, deadlineNanos);
                metadata = fetched.metadata();
                if (fetched.nextVisibleAtEpochMs() >= 0
                        && (nextVisibleAtEpochMs < 0
                        || fetched.nextVisibleAtEpochMs() < nextVisibleAtEpochMs)) {
                    nextVisibleAtEpochMs = fetched.nextVisibleAtEpochMs();
                }
            }
            if (!result.isEmpty() || timeoutNanos == 0L) {
                return result;
            }
            long now = currentTimeMillis();
            long remainingNanos = deadlineNanos - currentTimeNanos();
            if (remainingNanos <= 0L) {
                return result;
            }
            long remainingMs = Math.max(
                    1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            long sleepMs = remainingMs;
            if (nextVisibleAtEpochMs >= 0L) {
                sleepMs = Math.min(remainingMs, Math.max(1L, nextVisibleAtEpochMs - now));
            }
            sleepForPoll(sleepMs);
            if (deadlineNanos - currentTimeNanos() <= 0L) {
                return result;
            }
        } while (result.isEmpty());
        return result;
    }

    private MetadataResponse cachedMetadata(String topicName) throws InterruptedException {
        return cachedMetadata(topicName, null);
    }

    private MetadataResponse cachedMetadata(String topicName, Long deadlineNanos)
            throws InterruptedException {
        CachedMetadata cached = metadataCache.get(topicName);
        if (cached != null
                && currentTimeMillis() - cached.cachedAtEpochMs() <= DEFAULT_METADATA_CACHE_MAX_AGE_MS) {
            return cached.response();
        }
        metadataCache.remove(topicName);
        return refreshMetadata(topicName, deadlineNanos);
    }

    private MetadataResponse refreshMetadata(String topicName) throws InterruptedException {
        return refreshMetadata(topicName, null);
    }

    private MetadataResponse refreshMetadata(String topicName, Long deadlineNanos)
            throws InterruptedException {
        MetadataResponse metadata = deadlineNanos == null
                ? fetchMetadata(topicName)
                : fetchMetadata(topicName, deadlineNanos);
        CachedMetadata current = metadataCache.get(topicName);
        if (current == null || metadata.metadataVersion() >= current.response().metadataVersion()) {
            metadataCache.put(topicName, new CachedMetadata(metadata, currentTimeMillis()));
            return metadata;
        }
        return current.response();
    }

    private void invalidateMetadata(String topicName) {
        metadataCache.remove(topicName);
    }

    protected MetadataResponse fetchMetadata(String topicName) throws InterruptedException {
        MqDecoder.DecodedResponse metadataResponse = controllerClient.send(
                ApiKeys.METADATA, new MetadataRequest(topicName));
        controllerClient.ensureSuccess(metadataResponse, "METADATA topic=" + topicName);
        return (MetadataResponse) metadataResponse.body();
    }

    protected MetadataResponse fetchMetadata(String topicName, long deadlineNanos)
            throws InterruptedException {
        requirePollTimeRemaining(deadlineNanos, "METADATA");
        MqDecoder.DecodedResponse metadataResponse = controllerClient.sendUntil(
                ApiKeys.METADATA, new MetadataRequest(topicName), deadlineNanos);
        controllerClient.ensureSuccess(metadataResponse, "METADATA topic=" + topicName);
        return (MetadataResponse) metadataResponse.body();
    }

    private FetchPartitionResult fetchPartition(
            int partition,
            MetadataResponse initialMetadata,
            List<ConsumerRecord> result,
            long deadlineNanos) throws InterruptedException {
        MetadataResponse metadata = initialMetadata;
        for (int attempt = 0; attempt < 2; attempt++) {
            PartitionRoute route = routeWithRefresh(
                    partition, metadata, attempt, deadlineNanos);
            long offset = offsets.getOrDefault(partition, 0L);
            MqDecoder.DecodedResponse fetchResponse;
            try {
                fetchResponse = sendFetchV2(
                        route.leader(),
                        new FetchV2Request(
                                topic, partition, route.leaderEpoch(), offset, 1024 * 1024),
                        deadlineNanos);
            } catch (MiniMqRequestTimeoutException failure) {
                invalidateBrokerClient(route.leader().brokerId(), deadlineNanos);
                throw failure;
            } catch (MiniMqConnectionException failure) {
                invalidateMetadata(topic);
                invalidateBrokerClient(route.leader().brokerId(), deadlineNanos);
                if (attempt == 0 && hasPollTimeRemaining(deadlineNanos)) {
                    metadata = refreshMetadata(topic, deadlineNanos);
                    continue;
                }
                throw failure;
            }
            MqResponseHeader header = fetchResponse.header();
            if (isStaleLeaderError(header.errorCode())
                    && attempt == 0
                    && hasPollTimeRemaining(deadlineNanos)) {
                invalidateMetadata(topic);
                invalidateBrokerClient(route.leader().brokerId(), deadlineNanos);
                metadata = refreshMetadata(topic, deadlineNanos);
                continue;
            }
            controllerClient.ensureSuccess(fetchResponse, "FETCH topic=" + topic
                    + " partition=" + partition
                    + " broker=" + route.leader().brokerId()
                    + " attempt=" + (attempt + 1) + "/2");
            FetchV2Response fetch = (FetchV2Response) fetchResponse.body();
            for (FetchedRecord record : fetch.records().items()) {
                result.add(new ConsumerRecord(topic, partition, record.offset(), record.key(), record.value()));
                offsets.put(partition, record.offset() + 1);
            }
            return new FetchPartitionResult(metadata, fetch.nextVisibleAtEpochMs());
        }
        return new FetchPartitionResult(metadata, -1L);
    }

    private static boolean isStaleLeaderError(ErrorCode errorCode) {
        return errorCode == ErrorCode.NOT_LEADER
                || errorCode == ErrorCode.BROKER_FENCED
                || errorCode == ErrorCode.STALE_LEADER_EPOCH
                || errorCode == ErrorCode.LEADER_NOT_READY
                || errorCode == ErrorCode.UNKNOWN_TOPIC
                || errorCode == ErrorCode.UNKNOWN_PARTITION;
    }

    private PartitionRoute routeWithRefresh(
            int partition,
            MetadataResponse metadata,
            int attempt,
            long deadlineNanos) throws InterruptedException {
        try {
            return route(partition, metadata);
        } catch (MiniMqMetadataException e) {
            if (attempt == 0) {
                invalidateMetadata(topic);
                return route(partition, refreshMetadata(topic, deadlineNanos));
            }
            throw e;
        }
    }

    private PartitionRoute route(int partition, MetadataResponse metadata) {
        TopicMetadata topicMetadata = metadata.topics().stream()
                .filter(t -> t.topicName().equals(topic))
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException("Topic not found: " + topic));
        PartitionMetadata partitionMetadata = topicMetadata.partitions().stream()
                .filter(p -> p.partitionId() == partition)
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException(
                        "Partition " + partition + " not found for topic " + topic));
        BrokerMetadata leader = metadata.brokers().stream()
                .filter(b -> b.brokerId() == partitionMetadata.leaderBrokerId())
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException(
                        "Leader broker " + partitionMetadata.leaderBrokerId() + " not found"));
        return new PartitionRoute(partitionMetadata.leaderEpoch(), leader);
    }

    public void commitSync(String topic, int partition, long offset) throws InterruptedException {
        MqDecoder.DecodedResponse response = controllerClient.send(
                ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest(groupId, topic, partition, offset));
        controllerClient.ensureSuccess(response, "OFFSET_COMMIT topic=" + topic
                + " partition=" + partition);
        offsets.put(partition, offset);
    }

    private NetworkClient brokerClient(BrokerMetadata leader) throws InterruptedException {
        return brokerClient(leader, null);
    }

    private NetworkClient brokerClient(BrokerMetadata leader, Long deadlineNanos)
            throws InterruptedException {
        NetworkClient current = brokerClients.get(leader.brokerId());
        if (current != null) {
            return current;
        }
        NetworkClient candidate = brokerClientFactory.apply(
                "consumer-" + groupId + "-" + leader.brokerId());
        try {
            if (deadlineNanos == null) {
                candidate.connect(leader.host(), leader.port());
            } else {
                candidate.connect(
                        leader.host(),
                        leader.port(),
                        requirePollTimeRemaining(deadlineNanos, "CONNECT_BROKER"));
            }
        } catch (InterruptedException | RuntimeException | Error failure) {
            boolean cleanupInterrupted = deadlineNanos == null
                    ? NetworkClientCleanup.suppressCloseFailure(candidate, failure)
                    : NetworkClientCleanup.suppressCloseFailure(
                            candidate, failure, remainingPollTimeout(deadlineNanos));
            if (cleanupInterrupted) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        }
        brokerClients.put(leader.brokerId(), candidate);
        return candidate;
    }

    protected MqDecoder.DecodedResponse sendFetch(
            BrokerMetadata leader,
            FetchRequest request) throws InterruptedException {
        return brokerClient(leader).send(ApiKeys.FETCH, request);
    }

    protected MqDecoder.DecodedResponse sendFetchV2(
            BrokerMetadata leader,
            FetchV2Request request) throws InterruptedException {
        return brokerClient(leader).send(ApiKeys.FETCH_V2, request);
    }

    protected MqDecoder.DecodedResponse sendFetchV2(
            BrokerMetadata leader,
            FetchV2Request request,
            long deadlineNanos) throws InterruptedException {
        NetworkClient client = brokerClient(leader, deadlineNanos);
        return client.send(
                ApiKeys.FETCH_V2,
                request,
                requirePollTimeRemaining(deadlineNanos, "FETCH_V2"));
    }

    private boolean hasPollTimeRemaining(long deadlineNanos) {
        return deadlineNanos - currentTimeNanos() > 0L;
    }

    private Duration requirePollTimeRemaining(long deadlineNanos, String operation) {
        Duration remaining = remainingPollTimeout(deadlineNanos);
        if (remaining.isZero()) {
            throw new MiniMqRequestTimeoutException(operation, remaining);
        }
        return remaining;
    }

    private Duration remainingPollTimeout(long deadlineNanos) {
        long remainingNanos = deadlineNanos - currentTimeNanos();
        return remainingNanos <= 0L
                ? Duration.ZERO
                : Duration.ofNanos(remainingNanos);
    }

    protected void invalidateBrokerClient(int brokerId) throws InterruptedException {
        NetworkClient staleClient = brokerClients.remove(brokerId);
        if (staleClient != null) {
            staleClient.close();
        }
    }

    protected void invalidateBrokerClient(int brokerId, long deadlineNanos)
            throws InterruptedException {
        NetworkClient staleClient = brokerClients.remove(brokerId);
        if (staleClient != null) {
            staleClient.abortCandidate(remainingPollTimeout(deadlineNanos));
        }
    }

    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    protected long currentTimeNanos() {
        return System.nanoTime();
    }

    protected void sleepForPoll(long sleepMs) throws InterruptedException {
        Thread.sleep(sleepMs);
    }

    // 简化版心跳只维持成员活跃状态，后续版本可在这里承载 rebalance 触发逻辑。
    private void startHeartbeat() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                controllerClient.send(ApiKeys.HEARTBEAT, new HeartbeatRequest(groupId, memberId));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException ignored) {
                // A transient heartbeat failure must not cancel future scheduled executions.
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        controllerClient.close();
        for (NetworkClient client : brokerClients.values()) {
            client.close();
        }
    }

    private record PartitionRoute(long leaderEpoch, BrokerMetadata leader) {
    }

    private record CachedMetadata(MetadataResponse response, long cachedAtEpochMs) {
    }

    private record FetchPartitionResult(MetadataResponse metadata, long nextVisibleAtEpochMs) {
    }
}

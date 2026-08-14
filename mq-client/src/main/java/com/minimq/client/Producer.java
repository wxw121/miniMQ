package com.minimq.client;

import com.minimq.client.exception.MiniMqClientException;
import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.acks.ProduceAcks;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.util.Murmur2Partitioner;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.*;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 生产者客户端：{@link #send(ProducerRecord)} 先入队并立即返回；同一分区上未立刻
 * 读取 {@link ProduceResult#offset()} 的连续发送会攒成一次 Produce。
 * {@link #send(List)}、{@link #flush()}、{@link ProduceResult#offset()} 与 linger
 * 到期都会把待发送批次发出去。
 */
public class Producer implements AutoCloseable {
    public static final long DEFAULT_METADATA_CACHE_MAX_AGE_MS = 30_000L;
    public static final long DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS = 30_000L;
    public static final int DEFAULT_RELIABILITY_LINK_CACHE_MAX_ENTRIES = 1_024;
    private static final long RELIABILITY_LINK_CACHE_MAX_AGE_NANOS =
            TimeUnit.MILLISECONDS.toNanos(DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS);
    private final ControllerAwareNetworkClient client;
    private final Function<String, NetworkClient> brokerClientFactory;
    private final ProducerConfig producerConfig;
    private final Map<Integer, NetworkClient> brokerClients = new ConcurrentHashMap<>();
    private final Map<String, CachedMetadata> metadataCache = new ConcurrentHashMap<>();
    private final Map<String, CachedReliabilityMetadata> reliabilityMetadataCache =
            Collections.synchronizedMap(new BoundedLruCache<>(DEFAULT_RELIABILITY_LINK_CACHE_MAX_ENTRIES));
    private final Object batchLock = new Object();
    private final Map<BatchKey, PendingBatch> pending = new LinkedHashMap<>();
    private final ScheduledExecutorService lingerExecutor;
    private boolean closed;

    public Producer(String bootstrapServers, String clientId) {
        this(bootstrapServers, clientId, ProducerConfig.defaults());
    }

    public Producer(String bootstrapServers, String clientId, ProducerConfig producerConfig) {
        this(bootstrapServers, clientId, NetworkClient::new, producerConfig);
    }

    Producer(
            String bootstrapServers,
            String clientId,
            Function<String, NetworkClient> brokerClientFactory) {
        this(bootstrapServers, clientId, brokerClientFactory, ProducerConfig.defaults());
    }

    Producer(
            String bootstrapServers,
            String clientId,
            Function<String, NetworkClient> brokerClientFactory,
            ProducerConfig producerConfig) {
        this.client = new ControllerAwareNetworkClient(bootstrapServers, clientId);
        this.brokerClientFactory = Objects.requireNonNull(
                brokerClientFactory, "brokerClientFactory");
        this.producerConfig = Objects.requireNonNull(producerConfig, "producerConfig");
        this.lingerExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "minimq-producer-linger-" + clientId);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() throws InterruptedException {
        client.connect();
    }

    // 每次发送前读取最新元数据。若 leader 过期，V1.3 会刷新元数据并重试一次。
    public ProduceResult send(ProducerRecord record) throws InterruptedException {
        return send(record, ProduceAcks.LEADER, ProduceAcks.DEFAULT_TIMEOUT_MS);
    }

    public ProduceResult send(ProducerRecord record, ProduceAcks acks) throws InterruptedException {
        return send(record, acks, ProduceAcks.DEFAULT_TIMEOUT_MS);
    }

    public ProduceResult send(ProducerRecord record, ProduceAcks acks, int ackTimeoutMs)
            throws InterruptedException {
        ProduceAcks.requireTimeoutMs(ackTimeoutMs);
        Objects.requireNonNull(record, "record");
        synchronized (batchLock) {
            ensureOpen();
        }
        MetadataResponse metadata = cachedMetadata(record.topic());
        ProduceRoute route = routeWithRefresh(record, metadata, 0);
        CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        ProduceResult result = new ProduceResult(
                record.topic(), route.partitionId(), offsetFuture, this);
        List<PendingDispatch> ready = enqueue(
                new BatchKey(record.topic(), route.partitionId()),
                new PendingRecord(record, offsetFuture),
                acks,
                ackTimeoutMs);
        dispatchAll(ready, true);
        return result;
    }

    private ProduceResult send(ProducerRecord record, boolean retryConnectionFailure)
            throws InterruptedException {
        return send(record, retryConnectionFailure, ProduceAcks.LEADER, ProduceAcks.DEFAULT_TIMEOUT_MS);
    }

    private ProduceResult send(
            ProducerRecord record,
            boolean retryConnectionFailure,
            ProduceAcks acks,
            int ackTimeoutMs)
            throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(record.topic());
        for (int attempt = 0; attempt < 2; attempt++) {
            ProduceRoute route = routeWithRefresh(record, metadata, attempt);
            ProduceRequest request = new ProduceRequest(
                    record.topic(),
                    route.partitionId(),
                    route.leaderEpoch(),
                    List.of(new ProduceRecord(record.key(), record.value())),
                    acks.code(),
                    ackTimeoutMs);
            MqDecoder.DecodedResponse response;
            try {
                response = sendProduce(route.leader(), request);
            } catch (MiniMqConnectionException failure) {
                invalidateMetadata(record.topic());
                invalidateBrokerClient(route.leader().brokerId());
                if (retryConnectionFailure && attempt == 0) {
                    metadata = refreshMetadata(record.topic());
                    continue;
                }
                throw failure;
            }
            MqResponseHeader header = response.header();
            if (isStaleLeaderError(header.errorCode()) && attempt == 0) {
                invalidateMetadata(record.topic());
                invalidateBrokerClient(route.leader().brokerId());
                metadata = refreshMetadata(record.topic());
                continue;
            }
            client.ensureSuccess(response, "PRODUCE topic=" + record.topic()
                    + " partition=" + route.partitionId()
                    + " broker=" + route.leader().brokerId()
                    + " attempt=" + (attempt + 1) + "/2");
            ProduceResponse produceResponse = (ProduceResponse) response.body();
            return new ProduceResult(
                    produceResponse.topicName(),
                    produceResponse.partitionId(),
                    produceResponse.baseOffset());
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Sends every record in one round-trip per destination partition.
     * Results are aligned with {@code records} so {@code results.get(i)} is the
     * assigned offset of {@code records.get(i)}.
     */
    public List<ProduceResult> send(List<ProducerRecord> records) throws InterruptedException {
        return send(records, ProduceAcks.LEADER, ProduceAcks.DEFAULT_TIMEOUT_MS);
    }

    public List<ProduceResult> send(List<ProducerRecord> records, ProduceAcks acks)
            throws InterruptedException {
        return send(records, acks, ProduceAcks.DEFAULT_TIMEOUT_MS);
    }

    public List<ProduceResult> send(
            List<ProducerRecord> records, ProduceAcks acks, int ackTimeoutMs)
            throws InterruptedException {
        ProduceAcks.requireTimeoutMs(ackTimeoutMs);
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }
        flush();
        ProduceResult[] results = new ProduceResult[records.size()];
        Map<String, List<IndexedRecord>> byTopic = new LinkedHashMap<>();
        for (int i = 0; i < records.size(); i++) {
            ProducerRecord record = records.get(i);
            if (record == null) {
                throw new IllegalArgumentException("records must not contain null");
            }
            byTopic.computeIfAbsent(record.topic(), ignored -> new ArrayList<>())
                    .add(new IndexedRecord(i, record));
        }
        for (Map.Entry<String, List<IndexedRecord>> topicEntry : byTopic.entrySet()) {
            sendTopicBatch(topicEntry.getKey(), topicEntry.getValue(), acks, ackTimeoutMs, results);
        }
        return List.of(results);
    }

    private void sendTopicBatch(
            String topic,
            List<IndexedRecord> topicRecords,
            ProduceAcks acks,
            int ackTimeoutMs,
            ProduceResult[] results) throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(topic);
        Map<Integer, List<IndexedRecord>> byPartition = new LinkedHashMap<>();
        for (IndexedRecord indexed : topicRecords) {
            ProduceRoute route = routeWithRefresh(indexed.record(), metadata, 0);
            byPartition.computeIfAbsent(route.partitionId(), ignored -> new ArrayList<>())
                    .add(indexed);
            metadata = cachedMetadata(topic);
        }
        for (List<IndexedRecord> partitionRecords : byPartition.values()) {
            sendPartitionBatch(topic, partitionRecords, acks, ackTimeoutMs, true, results);
        }
    }

    private void sendPartitionBatch(
            String topic,
            List<IndexedRecord> partitionRecords,
            ProduceAcks acks,
            int ackTimeoutMs,
            boolean retryConnectionFailure,
            ProduceResult[] results) throws InterruptedException {
        ProducerRecord first = partitionRecords.get(0).record();
        MetadataResponse metadata = cachedMetadata(topic);
        for (int attempt = 0; attempt < 2; attempt++) {
            ProduceRoute route = routeWithRefresh(first, metadata, attempt);
            List<ProduceRecord> payload = new ArrayList<>(partitionRecords.size());
            for (IndexedRecord indexed : partitionRecords) {
                payload.add(new ProduceRecord(indexed.record().key(), indexed.record().value()));
            }
            ProduceRequest request = new ProduceRequest(
                    topic,
                    route.partitionId(),
                    route.leaderEpoch(),
                    payload,
                    acks.code(),
                    ackTimeoutMs);
            MqDecoder.DecodedResponse response;
            try {
                response = sendProduce(route.leader(), request);
            } catch (MiniMqConnectionException failure) {
                invalidateMetadata(topic);
                invalidateBrokerClient(route.leader().brokerId());
                if (retryConnectionFailure && attempt == 0) {
                    metadata = refreshMetadata(topic);
                    continue;
                }
                throw failure;
            }
            MqResponseHeader header = response.header();
            if (isStaleLeaderError(header.errorCode()) && attempt == 0) {
                invalidateMetadata(topic);
                invalidateBrokerClient(route.leader().brokerId());
                metadata = refreshMetadata(topic);
                continue;
            }
            client.ensureSuccess(response, "PRODUCE topic=" + topic
                    + " partition=" + route.partitionId()
                    + " broker=" + route.leader().brokerId()
                    + " attempt=" + (attempt + 1) + "/2");
            ProduceResponse produceResponse = (ProduceResponse) response.body();
            long baseOffset = produceResponse.baseOffset();
            for (int i = 0; i < partitionRecords.size(); i++) {
                IndexedRecord indexed = partitionRecords.get(i);
                results[indexed.index()] = new ProduceResult(
                        produceResponse.topicName(),
                        produceResponse.partitionId(),
                        baseOffset + i);
            }
            return;
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Sends a failed source record to the source topic's configured DLQ link.
     * The target topic is resolved through METADATA_V2, then routed through the
     * normal PRODUCE API so existing consumers can read it immediately.
     */
    public ProduceResult sendToDeadLetter(String originalTopic, ProducerRecord record)
            throws InterruptedException {
        return sendReliability(originalTopic, record, ReliabilityTarget.DEAD_LETTER, null);
    }

    /**
     * Sends a failed source record to the source topic's configured retry link
     * with a delayed visibility timestamp using PRODUCE_V2.
     */
    public ProduceResult sendToRetry(
            String originalTopic,
            ProducerRecord record,
            long deliverAtEpochMs) throws InterruptedException {
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("deliverAtEpochMs must be non-negative");
        }
        return sendReliability(originalTopic, record, ReliabilityTarget.RETRY, deliverAtEpochMs);
    }

    public ProduceResult sendDelayed(ProducerRecord record, long deliverAtEpochMs)
            throws InterruptedException {
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("deliverAtEpochMs must be non-negative");
        }
        return sendScheduled(record, deliverAtEpochMs);
    }

    private static boolean isStaleLeaderError(ErrorCode errorCode) {
        return errorCode == ErrorCode.NOT_LEADER
                || errorCode == ErrorCode.BROKER_FENCED
                || errorCode == ErrorCode.STALE_LEADER_EPOCH
                || errorCode == ErrorCode.LEADER_NOT_READY
                || errorCode == ErrorCode.UNKNOWN_TOPIC
                || errorCode == ErrorCode.UNKNOWN_PARTITION;
    }

    private MetadataResponse cachedMetadata(String topicName) throws InterruptedException {
        CachedMetadata cached = metadataCache.get(topicName);
        if (cached != null
                && currentTimeMillis() - cached.cachedAtEpochMs() <= DEFAULT_METADATA_CACHE_MAX_AGE_MS) {
            return cached.response();
        }
        metadataCache.remove(topicName);
        return refreshMetadata(topicName);
    }

    private MetadataResponse refreshMetadata(String topicName) throws InterruptedException {
        MetadataResponse metadata = fetchMetadata(topicName);
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
        MqDecoder.DecodedResponse metadataResponse = client.send(
                ApiKeys.METADATA, new MetadataRequest(topicName));
        client.ensureSuccess(metadataResponse, "METADATA topic=" + topicName);
        return (MetadataResponse) metadataResponse.body();
    }

    protected MetadataV2Response fetchMetadataV2(String topicName) throws InterruptedException {
        MqDecoder.DecodedResponse metadataResponse = client.send(
                ApiKeys.METADATA_V2, new MetadataV2Request(topicName, 1, 1));
        client.ensureSuccess(metadataResponse, "METADATA_V2 topic=" + topicName);
        return (MetadataV2Response) metadataResponse.body();
    }

    private ProduceRoute routeWithRefresh(
            ProducerRecord record,
            MetadataResponse metadata,
            int attempt) throws InterruptedException {
        try {
            return route(record, metadata);
        } catch (MiniMqMetadataException e) {
            if (attempt == 0) {
                invalidateMetadata(record.topic());
                return route(record, refreshMetadata(record.topic()));
            }
            throw e;
        }
    }

    private ProduceRoute route(ProducerRecord record, MetadataResponse metadata) {
        TopicMetadata topic = metadata.topics().stream()
                .filter(t -> t.topicName().equals(record.topic()))
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException("Topic not found: " + record.topic()));
        if (topic.partitions().isEmpty()) {
            throw new MiniMqMetadataException("Topic has no partitions: " + record.topic());
        }
        if (record.partitionId() != null && record.partitionId() < 0) {
            throw new IllegalArgumentException("Partition id must be non-negative");
        }

        // 指定 partition 时直接使用；否则有 key 按 Murmur2 稳定分区，无 key 默认写入 0 号分区。
        int partitionId = record.partitionId() != null
                ? record.partitionId()
                : (record.key() != null
                ? Murmur2Partitioner.partition(record.key(), topic.partitions().size())
                : 0);

        PartitionMetadata partition = topic.partitions().stream()
                .filter(p -> p.partitionId() == partitionId)
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException(
                        "Partition " + partitionId + " not found for topic " + record.topic()));

        BrokerMetadata leader = metadata.brokers().stream()
                .filter(b -> b.brokerId() == partition.leaderBrokerId())
                .findFirst()
                .orElseThrow(() -> new MiniMqMetadataException(
                        "Leader broker " + partition.leaderBrokerId() + " not found"));

        return new ProduceRoute(partitionId, partition.leaderEpoch(), leader);
    }

    protected MqDecoder.DecodedResponse sendProduce(
            BrokerMetadata leader,
            ProduceRequest request) throws InterruptedException {
        return brokerClient(leader).send(ApiKeys.PRODUCE, request);
    }

    protected MqDecoder.DecodedResponse sendProduceV2(
            BrokerMetadata leader,
            ProduceV2Request request) throws InterruptedException {
        return brokerClient(leader).send(ApiKeys.PRODUCE_V2, request);
    }

    private ProduceResult sendScheduled(ProducerRecord record, long deliverAtEpochMs)
            throws InterruptedException {
        return sendScheduled(record, deliverAtEpochMs, true);
    }

    private ProduceResult sendScheduled(
            ProducerRecord record,
            long deliverAtEpochMs,
            boolean retryConnectionFailure) throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(record.topic());
        for (int attempt = 0; attempt < 2; attempt++) {
            ProduceRoute route = routeWithRefresh(record, metadata, attempt);
            ProduceV2Request request = new ProduceV2Request(
                    record.topic(),
                    route.partitionId(),
                    route.leaderEpoch(),
                    new V4BoundedList<>(
                            1,
                            false,
                            List.of(new ScheduledProduceRecord(
                                    record.key(), record.value(), deliverAtEpochMs))));
            MqDecoder.DecodedResponse response;
            try {
                response = sendProduceV2(route.leader(), request);
            } catch (MiniMqConnectionException failure) {
                invalidateMetadata(record.topic());
                invalidateBrokerClient(route.leader().brokerId());
                if (retryConnectionFailure && attempt == 0) {
                    metadata = refreshMetadata(record.topic());
                    continue;
                }
                throw failure;
            }
            MqResponseHeader header = response.header();
            if (isStaleLeaderError(header.errorCode()) && attempt == 0) {
                invalidateMetadata(record.topic());
                invalidateBrokerClient(route.leader().brokerId());
                metadata = refreshMetadata(record.topic());
                continue;
            }
            client.ensureSuccess(response, "PRODUCE_V2 topic=" + record.topic()
                    + " partition=" + route.partitionId()
                    + " broker=" + route.leader().brokerId()
                    + " attempt=" + (attempt + 1) + "/2");
            ProduceV2Response produceResponse = (ProduceV2Response) response.body();
            return new ProduceResult(
                    produceResponse.topicName(),
                    produceResponse.partitionId(),
                    produceResponse.baseOffset());
        }
        throw new IllegalStateException("unreachable");
    }

    private String reliabilityTarget(String originalTopic, ReliabilityTarget target)
            throws InterruptedException {
        if (originalTopic == null || originalTopic.isBlank()) {
            throw new IllegalArgumentException("originalTopic is required");
        }
        MetadataV2Response metadata = cachedReliabilityMetadata(originalTopic);
        TopicSummary summary = metadata.topics().items().stream()
                .filter(topic -> originalTopic.equals(topic.topicName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No METADATA_V2 topic summary for " + originalTopic));
        String targetTopic = switch (target) {
            case RETRY -> summary.retryTopic();
            case DEAD_LETTER -> summary.deadLetterTopic();
        };
        if (targetTopic == null || targetTopic.isBlank()) {
            throw new IllegalArgumentException(
                    "No " + target.description + " topic configured for " + originalTopic);
        }
        return targetTopic;
    }

    private ProduceResult sendReliability(
            String originalTopic,
            ProducerRecord record,
            ReliabilityTarget target,
            Long deliverAtEpochMs) throws InterruptedException {
        for (int attempt = 0; attempt < 2; attempt++) {
            String targetTopic = reliabilityTarget(originalTopic, target);
            try {
                ProducerRecord rerouted = reroute(record, targetTopic);
                return deliverAtEpochMs == null
                        ? send(rerouted, false)
                        : sendScheduled(rerouted, deliverAtEpochMs, false);
            } catch (MiniMqMetadataException failure) {
                if (attempt == 0) {
                    invalidateReliabilityRouting(originalTopic, targetTopic);
                    continue;
                }
                throw failure;
            } catch (MiniMqServerException failure) {
                if (attempt == 0 && isStaleLeaderError(failure.errorCode())) {
                    invalidateReliabilityRouting(originalTopic, targetTopic);
                    continue;
                }
                throw failure;
            } catch (MiniMqConnectionException failure) {
                invalidateReliabilityRouting(originalTopic, targetTopic);
                if (attempt == 0) {
                    continue;
                }
                throw failure;
            } catch (MiniMqRequestTimeoutException failure) {
                invalidateReliabilityRouting(originalTopic, targetTopic);
                throw failure;
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private MetadataV2Response cachedReliabilityMetadata(String originalTopic)
            throws InterruptedException {
        CachedReliabilityMetadata cached = reliabilityMetadataCache.get(originalTopic);
        if (cached != null
                && currentTimeNanos() - cached.cachedAtNanos()
                <= RELIABILITY_LINK_CACHE_MAX_AGE_NANOS) {
            return cached.response();
        }
        reliabilityMetadataCache.remove(originalTopic);
        MetadataV2Response metadata = fetchMetadataV2(originalTopic);
        CachedReliabilityMetadata current = reliabilityMetadataCache.get(originalTopic);
        if (current == null || metadata.metadataVersion() >= current.response().metadataVersion()) {
            reliabilityMetadataCache.put(
                    originalTopic, new CachedReliabilityMetadata(metadata, currentTimeNanos()));
            return metadata;
        }
        return current.response();
    }

    private void invalidateReliabilityRouting(String originalTopic, String targetTopic) {
        reliabilityMetadataCache.remove(originalTopic);
        invalidateMetadata(targetTopic);
    }

    private static ProducerRecord reroute(ProducerRecord record, String targetTopic) {
        if (record == null) {
            throw new IllegalArgumentException("record is required");
        }
        return new ProducerRecord(targetTopic, null, record.key(), record.value());
    }

    private NetworkClient brokerClient(BrokerMetadata leader) throws InterruptedException {
        NetworkClient current = brokerClients.get(leader.brokerId());
        if (current != null) {
            return current;
        }
        NetworkClient candidate = brokerClientFactory.apply(
                "producer-leader-" + leader.brokerId());
        try {
            candidate.connect(leader.host(), leader.port());
        } catch (InterruptedException | RuntimeException | Error failure) {
            if (NetworkClientCleanup.suppressCloseFailure(candidate, failure)) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        }
        NetworkClient existing = brokerClients.putIfAbsent(leader.brokerId(), candidate);
        if (existing != null) {
            discardUnusedBrokerClient(candidate);
            return existing;
        }
        return candidate;
    }

    private static void discardUnusedBrokerClient(NetworkClient candidate) {
        try {
            candidate.close();
        } catch (InterruptedException closeFailure) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException | Error ignored) {
            // The winner's client is already cached; losing the extra connection is best-effort.
        }
    }

    protected void invalidateBrokerClient(int brokerId) throws InterruptedException {
        NetworkClient staleClient = brokerClients.remove(brokerId);
        if (staleClient != null) {
            staleClient.close();
        }
    }

    public void flush() throws InterruptedException {
        dispatchAll(drainPending(), true);
    }

    public void flushQuietly() {
        try {
            flush();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MiniMqClientException("Interrupted while flushing producer", interrupted);
        }
    }

    @Override
    public void close() throws InterruptedException {
        List<PendingDispatch> remaining;
        synchronized (batchLock) {
            closed = true;
            remaining = drainPendingLocked();
        }
        try {
            dispatchAll(remaining, true);
        } finally {
            lingerExecutor.shutdownNow();
            client.close();
            for (NetworkClient c : brokerClients.values()) {
                c.close();
            }
        }
    }

    private List<PendingDispatch> enqueue(
            BatchKey key,
            PendingRecord pendingRecord,
            ProduceAcks acks,
            int ackTimeoutMs) {
        List<PendingDispatch> ready = new ArrayList<>();
        synchronized (batchLock) {
            ensureOpen();
            PendingBatch batch = pending.get(key);
            if (batch != null && (batch.acks != acks || batch.ackTimeoutMs != ackTimeoutMs)) {
                pending.remove(key);
                cancelLinger(batch);
                ready.add(new PendingDispatch(key, batch));
                batch = null;
            }
            if (batch == null) {
                batch = new PendingBatch(acks, ackTimeoutMs);
                pending.put(key, batch);
            }
            batch.records.add(pendingRecord);
            if (batch.records.size() >= producerConfig.batchSize() || producerConfig.lingerMs() == 0) {
                pending.remove(key);
                cancelLinger(batch);
                ready.add(new PendingDispatch(key, batch));
            } else if (batch.records.size() == 1) {
                scheduleLinger(key, batch);
            }
        }
        return ready;
    }

    private List<PendingDispatch> drainPending() {
        synchronized (batchLock) {
            return drainPendingLocked();
        }
    }

    private List<PendingDispatch> drainPendingLocked() {
        List<PendingDispatch> ready = new ArrayList<>(pending.size());
        for (Map.Entry<BatchKey, PendingBatch> entry : pending.entrySet()) {
            cancelLinger(entry.getValue());
            ready.add(new PendingDispatch(entry.getKey(), entry.getValue()));
        }
        pending.clear();
        return ready;
    }

    private void scheduleLinger(BatchKey key, PendingBatch batch) {
        batch.lingerTask = lingerExecutor.schedule(
                () -> lingerDispatch(key),
                producerConfig.lingerMs(),
                TimeUnit.MILLISECONDS);
    }

    private void lingerDispatch(BatchKey key) {
        PendingBatch batch;
        synchronized (batchLock) {
            batch = pending.remove(key);
            if (batch == null) {
                return;
            }
            batch.lingerTask = null;
        }
        try {
            dispatchAll(List.of(new PendingDispatch(key, batch)), false);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void cancelLinger(PendingBatch batch) {
        if (batch.lingerTask != null) {
            batch.lingerTask.cancel(false);
            batch.lingerTask = null;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
    }

    private void dispatchAll(List<PendingDispatch> ready, boolean rethrow)
            throws InterruptedException {
        InterruptedException interrupted = null;
        RuntimeException failure = null;
        for (PendingDispatch dispatch : ready) {
            try {
                dispatchBatch(dispatch.key(), dispatch.batch());
            } catch (InterruptedException e) {
                interrupted = e;
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        if (!rethrow) {
            return;
        }
        if (interrupted != null) {
            throw interrupted;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void dispatchBatch(BatchKey key, PendingBatch batch) throws InterruptedException {
        ProduceResult[] results = new ProduceResult[batch.records.size()];
        List<IndexedRecord> indexed = new ArrayList<>(batch.records.size());
        for (int i = 0; i < batch.records.size(); i++) {
            indexed.add(new IndexedRecord(i, batch.records.get(i).record()));
        }
        try {
            sendPartitionBatch(
                    key.topic(),
                    indexed,
                    batch.acks,
                    batch.ackTimeoutMs,
                    true,
                    results);
            for (int i = 0; i < batch.records.size(); i++) {
                batch.records.get(i).future().complete(results[i].offset());
            }
        } catch (InterruptedException interrupted) {
            failBatch(batch, new MiniMqClientException(
                    "Interrupted while sending produce batch", interrupted));
            throw interrupted;
        } catch (RuntimeException failure) {
            failBatch(batch, failure);
            throw failure;
        }
    }

    private static void failBatch(PendingBatch batch, Throwable failure) {
        for (PendingRecord record : batch.records) {
            record.future().completeExceptionally(failure);
        }
    }

    private record ProduceRoute(int partitionId, long leaderEpoch, BrokerMetadata leader) {
    }

    private record BatchKey(String topic, int partitionId) {
    }

    private record PendingRecord(ProducerRecord record, CompletableFuture<Long> future) {
    }

    private record PendingDispatch(BatchKey key, PendingBatch batch) {
    }

    private static final class PendingBatch {
        private final ProduceAcks acks;
        private final int ackTimeoutMs;
        private final List<PendingRecord> records = new ArrayList<>();
        private ScheduledFuture<?> lingerTask;

        private PendingBatch(ProduceAcks acks, int ackTimeoutMs) {
            this.acks = acks;
            this.ackTimeoutMs = ackTimeoutMs;
        }
    }

    private record IndexedRecord(int index, ProducerRecord record) {
    }

    private enum ReliabilityTarget {
        RETRY("retry"),
        DEAD_LETTER("dead-letter");

        private final String description;

        ReliabilityTarget(String description) {
            this.description = description;
        }
    }

    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    protected long currentTimeNanos() {
        return System.nanoTime();
    }

    private record CachedMetadata(MetadataResponse response, long cachedAtEpochMs) {
    }

    private record CachedReliabilityMetadata(
            MetadataV2Response response, long cachedAtNanos) {
    }

    private static final class BoundedLruCache<K, V> extends LinkedHashMap<K, V> {
        private final int maxEntries;

        private BoundedLruCache(int maxEntries) {
            super(16, 0.75f, true);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxEntries;
        }
    }
}

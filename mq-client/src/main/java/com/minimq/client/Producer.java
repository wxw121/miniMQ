package com.minimq.client;

import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqMetadataException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.util.Murmur2Partitioner;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.*;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 同步生产者客户端，负责获取元数据、选择目标分区并把消息发送到 leader Broker。
 */
public class Producer implements AutoCloseable {
    public static final long DEFAULT_METADATA_CACHE_MAX_AGE_MS = 30_000L;
    public static final long DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS = 30_000L;
    public static final int DEFAULT_RELIABILITY_LINK_CACHE_MAX_ENTRIES = 1_024;
    private static final long RELIABILITY_LINK_CACHE_MAX_AGE_NANOS =
            TimeUnit.MILLISECONDS.toNanos(DEFAULT_RELIABILITY_LINK_CACHE_MAX_AGE_MS);
    private final ControllerAwareNetworkClient client;
    private final Function<String, NetworkClient> brokerClientFactory;
    private final Map<Integer, NetworkClient> brokerClients = new HashMap<>();
    private final Map<String, CachedMetadata> metadataCache = new HashMap<>();
    private final Map<String, CachedReliabilityMetadata> reliabilityMetadataCache =
            new BoundedLruCache<>(DEFAULT_RELIABILITY_LINK_CACHE_MAX_ENTRIES);

    public Producer(String bootstrapServers, String clientId) {
        this(bootstrapServers, clientId, NetworkClient::new);
    }

    Producer(
            String bootstrapServers,
            String clientId,
            Function<String, NetworkClient> brokerClientFactory) {
        this.client = new ControllerAwareNetworkClient(bootstrapServers, clientId);
        this.brokerClientFactory = Objects.requireNonNull(
                brokerClientFactory, "brokerClientFactory");
    }

    public void start() throws InterruptedException {
        client.connect();
    }

    // 每次发送前读取最新元数据。若 leader 过期，V1.3 会刷新元数据并重试一次。
    public ProduceResult send(ProducerRecord record) throws InterruptedException {
        return send(record, true);
    }

    private ProduceResult send(ProducerRecord record, boolean retryConnectionFailure)
            throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(record.topic());
        for (int attempt = 0; attempt < 2; attempt++) {
            ProduceRoute route = routeWithRefresh(record, metadata, attempt);
            ProduceRequest request = new ProduceRequest(
                    record.topic(),
                    route.partitionId(),
                    route.leaderEpoch(),
                    List.of(new ProduceRecord(record.key(), record.value())));
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
        brokerClients.put(leader.brokerId(), candidate);
        return candidate;
    }

    protected void invalidateBrokerClient(int brokerId) throws InterruptedException {
        NetworkClient staleClient = brokerClients.remove(brokerId);
        if (staleClient != null) {
            staleClient.close();
        }
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
        for (NetworkClient c : brokerClients.values()) {
            c.close();
        }
    }

    private record ProduceRoute(int partitionId, long leaderEpoch, BrokerMetadata leader) {
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

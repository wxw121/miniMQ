package com.minimq.client;

import com.minimq.client.exception.MiniMqMetadataException;
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
import java.util.List;
import java.util.Map;

/**
 * 同步生产者客户端，负责获取元数据、选择目标分区并把消息发送到 leader Broker。
 */
public class Producer implements AutoCloseable {
    private final NetworkClient client;
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final Map<Integer, NetworkClient> brokerClients = new HashMap<>();
    private final Map<String, MetadataResponse> metadataCache = new HashMap<>();

    public Producer(String bootstrapServers, String clientId) {
        String[] parts = parseBootstrapServer(bootstrapServers);
        this.bootstrapHost = parts[0];
        this.bootstrapPort = Integer.parseInt(parts[1]);
        this.client = new NetworkClient(clientId);
    }

    public void start() throws InterruptedException {
        client.connect(bootstrapHost, bootstrapPort);
    }

    // 每次发送前读取最新元数据。若 leader 过期，V1.3 会刷新元数据并重试一次。
    public ProduceResult send(ProducerRecord record) throws InterruptedException {
        MetadataResponse metadata = cachedMetadata(record.topic());
        for (int attempt = 0; attempt < 2; attempt++) {
            ProduceRoute route = routeWithRefresh(record, metadata, attempt);
            ProduceRequest request = new ProduceRequest(
                    record.topic(),
                    route.partitionId(),
                    List.of(new ProduceRecord(record.key(), record.value())));
            MqDecoder.DecodedResponse response = sendProduce(route.leader(), request);
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

    private static boolean isStaleLeaderError(ErrorCode errorCode) {
        return errorCode == ErrorCode.NOT_LEADER || errorCode == ErrorCode.BROKER_FENCED;
    }

    private MetadataResponse cachedMetadata(String topicName) throws InterruptedException {
        MetadataResponse cached = metadataCache.get(topicName);
        if (cached != null) {
            return cached;
        }
        return refreshMetadata(topicName);
    }

    private MetadataResponse refreshMetadata(String topicName) throws InterruptedException {
        MetadataResponse metadata = fetchMetadata(topicName);
        metadataCache.put(topicName, metadata);
        return metadata;
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

        return new ProduceRoute(partitionId, leader);
    }

    protected MqDecoder.DecodedResponse sendProduce(
            BrokerMetadata leader,
            ProduceRequest request) throws InterruptedException {
        return brokerClient(leader).send(ApiKeys.PRODUCE, request);
    }

    private NetworkClient brokerClient(BrokerMetadata leader) throws InterruptedException {
        return brokerClients.computeIfAbsent(leader.brokerId(), id -> {
            try {
                NetworkClient c = new NetworkClient("producer-leader-" + id);
                c.connect(leader.host(), leader.port());
                return c;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        });
    }

    protected void invalidateBrokerClient(int brokerId) throws InterruptedException {
        NetworkClient staleClient = brokerClients.remove(brokerId);
        if (staleClient != null) {
            staleClient.close();
        }
    }

    private static String[] parseBootstrapServer(String bootstrapServers) {
        String[] parts = bootstrapServers.split(",", 2)[0].split(":", 2);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must contain host:port");
        }
        return parts;
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
        for (NetworkClient c : brokerClients.values()) {
            c.close();
        }
    }

    private record ProduceRoute(int partitionId, BrokerMetadata leader) {
    }
}

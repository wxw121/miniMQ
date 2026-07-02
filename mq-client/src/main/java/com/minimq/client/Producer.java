package com.minimq.client;

import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.util.Murmur2Partitioner;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
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

    public Producer(String bootstrapServers, String clientId) {
        String[] parts = parseBootstrapServer(bootstrapServers);
        this.bootstrapHost = parts[0];
        this.bootstrapPort = Integer.parseInt(parts[1]);
        this.client = new NetworkClient(clientId);
    }

    public void start() throws InterruptedException {
        client.connect(bootstrapHost, bootstrapPort);
    }

    // 每次发送前读取最新元数据，当前版本优先保证 leader 路由正确而不是缓存极致性能。
    public ProduceResult send(ProducerRecord record) throws InterruptedException {
        MqDecoder.DecodedResponse metadataResponse = client.send(
                ApiKeys.METADATA, new MetadataRequest(record.topic()));
        client.ensureSuccess(metadataResponse);
        MetadataResponse metadata = (MetadataResponse) metadataResponse.body();
        TopicMetadata topic = metadata.topics().stream()
                .filter(t -> t.topicName().equals(record.topic()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Topic not found: " + record.topic()));
        if (topic.partitions().isEmpty()) {
            throw new IllegalStateException("Topic has no partitions: " + record.topic());
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
                .orElseThrow(() -> new IllegalArgumentException(
                        "Partition " + partitionId + " not found for topic " + record.topic()));

        BrokerMetadata leader = metadata.brokers().stream()
                .filter(b -> b.brokerId() == partition.leaderBrokerId())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Leader broker " + partition.leaderBrokerId() + " not found"));

        NetworkClient leaderClient = brokerClient(leader);
        ProduceRequest request = new ProduceRequest(
                record.topic(),
                partitionId,
                List.of(new ProduceRecord(record.key(), record.value())));
        MqDecoder.DecodedResponse response = leaderClient.send(ApiKeys.PRODUCE, request);
        leaderClient.ensureSuccess(response);
        ProduceResponse produceResponse = (ProduceResponse) response.body();
        return new ProduceResult(produceResponse.topicName(), produceResponse.partitionId(), produceResponse.baseOffset());
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
}

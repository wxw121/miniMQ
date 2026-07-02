package com.minimq.client;

import com.minimq.client.model.ConsumerRecord;
import com.minimq.common.assignor.AssignmentSerde;
import com.minimq.common.assignor.RangeAssignor;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 同步消费者客户端，负责加入消费组、拉取分配分区、维护本地 offset 并提交消费进度。
 */
public class Consumer implements AutoCloseable {
    private final NetworkClient controllerClient;
    private final String groupId;
    private final Map<Integer, NetworkClient> brokerClients = new HashMap<>();
    private String topic;
    private String memberId = "";
    private int generationId;
    private String leaderMemberId;
    private List<Integer> assignedPartitions = List.of();
    private final Map<Integer, Long> offsets = new HashMap<>();
    private ScheduledExecutorService heartbeatExecutor;

    public Consumer(String bootstrapServers, String groupId, String clientId) {
        String[] parts = parseBootstrapServer(bootstrapServers);
        this.groupId = groupId;
        this.controllerClient = new NetworkClient(clientId);
        try {
            this.controllerClient.connect(parts[0], Integer.parseInt(parts[1]));
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
        MqDecoder.DecodedResponse metadataResponse = controllerClient.send(
                ApiKeys.METADATA, new MetadataRequest(topic));
        controllerClient.ensureSuccess(metadataResponse);
        MetadataResponse metadata = (MetadataResponse) metadataResponse.body();
        TopicMetadata topicMetadata = metadata.topics().stream()
                .filter(t -> t.topicName().equals(topic))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Topic not found: " + topic));
        return AssignmentSerde.encodeFullAssignment(RangeAssignor.assign(
                members, topicMetadata.partitions().size()));
    }

    // poll 会根据当前分配逐分区拉取，并在本地推进下一次读取 offset。
    public List<ConsumerRecord> poll(Duration timeout) throws InterruptedException {
        List<ConsumerRecord> result = new ArrayList<>();
        MqDecoder.DecodedResponse metadataResponse = controllerClient.send(
                ApiKeys.METADATA, new MetadataRequest(topic));
        controllerClient.ensureSuccess(metadataResponse);
        MetadataResponse metadata = (MetadataResponse) metadataResponse.body();
        TopicMetadata topicMetadata = metadata.topics().stream()
                .filter(t -> t.topicName().equals(topic))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Topic not found: " + topic));

        for (int partition : assignedPartitions) {
            PartitionMetadata partitionMetadata = topicMetadata.partitions().stream()
                    .filter(p -> p.partitionId() == partition)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Partition " + partition + " not found for topic " + topic));
            BrokerMetadata leader = metadata.brokers().stream()
                    .filter(b -> b.brokerId() == partitionMetadata.leaderBrokerId())
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Leader broker " + partitionMetadata.leaderBrokerId() + " not found"));
            NetworkClient leaderClient = brokerClient(leader);
            long offset = offsets.getOrDefault(partition, 0L);
            MqDecoder.DecodedResponse fetchResponse = leaderClient.send(
                    ApiKeys.FETCH, new FetchRequest(topic, partition, offset, 1024 * 1024));
            leaderClient.ensureSuccess(fetchResponse);
            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            for (FetchedRecord record : fetch.records()) {
                result.add(new ConsumerRecord(topic, partition, record.offset(), record.key(), record.value()));
                offsets.put(partition, record.offset() + 1);
            }
        }
        if (result.isEmpty()) {
            Thread.sleep(timeout.toMillis());
        }
        return result;
    }

    public void commitSync(String topic, int partition, long offset) throws InterruptedException {
        MqDecoder.DecodedResponse response = controllerClient.send(
                ApiKeys.OFFSET_COMMIT, new OffsetCommitRequest(groupId, topic, partition, offset));
        controllerClient.ensureSuccess(response);
        offsets.put(partition, offset);
    }

    private NetworkClient brokerClient(BrokerMetadata leader) throws InterruptedException {
        return brokerClients.computeIfAbsent(leader.brokerId(), id -> {
            try {
                NetworkClient c = new NetworkClient("consumer-" + groupId + "-" + id);
                c.connect(leader.host(), leader.port());
                return c;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        });
    }

    // 简化版心跳只维持成员活跃状态，后续版本可在这里承载 rebalance 触发逻辑。
    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                controllerClient.send(ApiKeys.HEARTBEAT, new HeartbeatRequest(groupId, memberId));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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

    private static String[] parseBootstrapServer(String bootstrapServers) {
        String[] parts = bootstrapServers.split(",", 2)[0].split(":", 2);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must contain host:port");
        }
        return parts;
    }
}

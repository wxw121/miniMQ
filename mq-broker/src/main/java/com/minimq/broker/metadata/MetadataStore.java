package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.minimq.common.error.ErrorCode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.format.DateTimeFormatter;

/**
 * 元数据文件存储器，负责把 ClusterMetadata 序列化到本地 JSON 文件并在启动时加载。
 */
public class MetadataStore implements ClusterMetadataRepository {
    private final Path metadataFile;
    private final boolean recoverCorruptMetadata;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    public MetadataStore(Path dataDir) {
        this(dataDir, false);
    }

    public MetadataStore(Path dataDir, boolean recoverCorruptMetadata) {
        this(dataDir, recoverCorruptMetadata, Clock.systemUTC());
    }

    MetadataStore(Path dataDir, boolean recoverCorruptMetadata, Clock clock) {
        this.metadataFile = dataDir.resolve("cluster-metadata.json");
        this.recoverCorruptMetadata = recoverCorruptMetadata;
        this.clock = clock;
    }

    public synchronized ClusterMetadata load() {
        if (!Files.exists(metadataFile)) {
            return new ClusterMetadata();
        }
        try {
            if (Files.size(metadataFile) == 0) {
                throw new IOException("Metadata file is empty: " + metadataFile);
            }
            JsonNode root = mapper.readTree(metadataFile.toFile());
            validateDocument(root);
            migrateOrValidateFormat((ObjectNode) root);
            ClusterMetadata metadata = mapper.treeToValue(root, ClusterMetadata.class);
            validate(metadata);
            return metadata;
        } catch (IOException e) {
            if (recoverCorruptMetadata) {
                quarantineCorruptMetadata();
                return new ClusterMetadata();
            }
            throw new IllegalStateException(
                    "Failed to load metadata from " + metadataFile
                            + "; enable corrupt metadata recovery to quarantine this file and start empty",
                    e);
        }
    }

    public synchronized void save(ClusterMetadata metadata) {
        try {
            validate(metadata);
            Path parent = metadataFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempFile = metadataFile.resolveSibling(metadataFile.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), metadata);
            forceFile(tempFile);
            try {
                Files.move(tempFile, metadataFile,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tempFile, metadataFile, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(parent);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to save metadata", e);
        }
    }

    @Override
    public synchronized VersionedClusterMetadata read() {
        ClusterMetadata metadata = load();
        return new VersionedClusterMetadata(metadata, metadata.metadataVersion);
    }

    @Override
    public synchronized UpdateResult initialize(ClusterMetadata metadata) {
        ClusterMetadata candidate = mapper.convertValue(metadata, ClusterMetadata.class);
        candidate.metadataVersion = 0L;
        if (!Files.exists(metadataFile)) {
            save(candidate);
            return new UpdateResult(ErrorCode.NONE, new VersionedClusterMetadata(candidate, 0L));
        }
        VersionedClusterMetadata current = read();
        candidate.metadataVersion = current.metadata().metadataVersion;
        if (mapper.valueToTree(candidate).equals(mapper.valueToTree(current.metadata()))) {
            return new UpdateResult(ErrorCode.NONE, current);
        }
        return new UpdateResult(ErrorCode.INVALID_REQUEST, current);
    }

    @Override
    public synchronized UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
        VersionedClusterMetadata current = read();
        if (current.revision() != expectedRevision) {
            return new UpdateResult(ErrorCode.CONTROLLER_FENCED, current);
        }
        ClusterMetadata next = mapper.convertValue(metadata, ClusterMetadata.class);
        next.metadataVersion = Math.addExact(expectedRevision, 1L);
        save(next);
        return new UpdateResult(ErrorCode.NONE, new VersionedClusterMetadata(next, next.metadataVersion));
    }

    private void validate(ClusterMetadata metadata) throws IOException {
        if (metadata == null) {
            throw new IOException("Metadata document is null");
        }
        if (metadata.brokers == null || metadata.topics == null || metadata.consumerGroups == null) {
            throw new IOException("Metadata document has missing top-level maps");
        }
        if (metadata.metadataFormatVersion != 3) {
            throw new IOException("Unsupported metadataFormatVersion " + metadata.metadataFormatVersion);
        }
        if (metadata.controllerEpoch < 0 || metadata.metadataVersion < 0) {
            throw new IOException("Metadata epochs and version must be non-negative");
        }
        for (var entry : metadata.brokers.entrySet()) {
            Integer brokerId = entry.getKey();
            ClusterMetadata.BrokerEntry broker = entry.getValue();
            if (broker == null) {
                throw new IOException("Metadata broker " + brokerId + " is null");
            }
            if (brokerId == null || broker.brokerId != brokerId) {
                throw new IOException("Metadata broker key " + brokerId
                        + " does not match brokerId " + broker.brokerId);
            }
            if (broker.host == null || broker.host.isBlank()) {
                throw new IOException("Metadata broker " + brokerId + " has blank host");
            }
            if (broker.port <= 0 || broker.port > 65535) {
                throw new IOException("Metadata broker " + brokerId + " has invalid port " + broker.port);
            }
            if (broker.brokerEpoch < 0) {
                throw new IOException("Metadata broker " + brokerId
                        + " has invalid brokerEpoch " + broker.brokerEpoch);
            }
        }
        for (var entry : metadata.topics.entrySet()) {
            String topicName = entry.getKey();
            ClusterMetadata.TopicEntry topic = entry.getValue();
            if (topicName == null || topicName.isBlank()) {
                throw new IOException("Metadata topic key is blank");
            }
            if (topic == null) {
                throw new IOException("Metadata topic " + topicName + " is null");
            }
            if (topic.topicName == null || topic.topicName.isBlank()) {
                throw new IOException("Metadata topic key " + topicName + " has blank topicName");
            }
            if (!topicName.equals(topic.topicName)) {
                throw new IOException("Metadata topic key " + topicName
                        + " does not match topicName " + topic.topicName);
            }
            if (topic.partitions == null) {
                throw new IOException("Metadata topic " + topicName + " has null partitions");
            }
            for (ClusterMetadata.PartitionEntry partition : topic.partitions) {
                if (partition == null) {
                    throw new IOException("Metadata topic " + topicName + " has null partition");
                }
                if (partition.partitionId < 0) {
                    throw new IOException("Metadata topic " + topicName
                            + " has invalid partitionId " + partition.partitionId);
                }
                if (partition.replicaBrokerIds == null || partition.isrBrokerIds == null) {
                    throw new IOException("Metadata topic " + topicName + " has null replica or ISR list");
                }
                if (partition.frozen && !partition.isrBrokerIds.isEmpty()) {
                    throw new IOException("Metadata topic " + topicName
                            + " frozen partition must have an empty ISR");
                }
                if (partition.frozen
                        && !partition.replicaBrokerIds.contains(partition.leaderBrokerId)) {
                    throw new IOException("Metadata topic " + topicName
                            + " frozen partition Leader must be a configured replica");
                }
                if (partition.replicaLogEndOffsets == null) {
                    throw new IOException("Metadata topic " + topicName + " has null replica LEO map");
                }
                if (partition.leaderEpoch < 0 || partition.leaderEpochStartOffset < 0) {
                    throw new IOException("Metadata topic " + topicName + " has invalid Leader epoch state");
                }
                if (partition.frozen && partition.leaderReady) {
                    throw new IOException("Metadata topic " + topicName + " frozen partition cannot be ready");
                }
                for (var leoEntry : partition.replicaLogEndOffsets.entrySet()) {
                    Integer brokerId = leoEntry.getKey();
                    Long logEndOffset = leoEntry.getValue();
                    if (brokerId == null || logEndOffset == null || logEndOffset < 0
                            || partition.replicaBrokerIds == null
                            || !partition.replicaBrokerIds.contains(brokerId)) {
                        throw new IOException("Metadata topic " + topicName
                                + " has invalid replica LEO for broker " + brokerId);
                    }
                }
            }
        }
        for (var entry : metadata.consumerGroups.entrySet()) {
            String groupId = entry.getKey();
            ClusterMetadata.ConsumerGroupEntry group = entry.getValue();
            if (groupId == null || groupId.isBlank()) {
                throw new IOException("Metadata consumer group key is blank");
            }
            if (group == null) {
                throw new IOException("Metadata consumer group " + groupId + " is null");
            }
            if (group.groupId == null || group.groupId.isBlank()) {
                throw new IOException("Metadata consumer group key " + groupId + " has blank groupId");
            }
            if (!groupId.equals(group.groupId)) {
                throw new IOException("Metadata consumer group key " + groupId
                        + " does not match groupId " + group.groupId);
            }
            if (group.topicName == null || group.topicName.isBlank()) {
                throw new IOException("Metadata consumer group " + groupId + " has blank topicName");
            }
            if (group.generationId < 0) {
                throw new IOException("Metadata consumer group " + groupId
                        + " has invalid generationId " + group.generationId);
            }
            if (group.members == null || group.committedOffsets == null) {
                throw new IOException("Metadata consumer group " + groupId
                        + " has missing member or offset maps");
            }
            for (var memberEntry : group.members.entrySet()) {
                String memberId = memberEntry.getKey();
                ClusterMetadata.MemberEntry member = memberEntry.getValue();
                if (memberId == null || memberId.isBlank()) {
                    throw new IOException("Metadata member key is blank");
                }
                if (member == null) {
                    throw new IOException("Metadata member " + memberId + " is null");
                }
                if (member.memberId == null || member.memberId.isBlank()) {
                    throw new IOException("Metadata member key " + memberId + " has blank memberId");
                }
                if (!memberId.equals(member.memberId)) {
                    throw new IOException("Metadata member key " + memberId
                            + " does not match memberId " + member.memberId);
                }
                if (member.lastHeartbeatMs < 0) {
                    throw new IOException("Metadata member " + memberId
                            + " has invalid lastHeartbeatMs " + member.lastHeartbeatMs);
                }
                if (member.assignedPartitions == null) {
                    throw new IOException("Metadata member " + memberId + " has null assignedPartitions");
                }
            }
            for (var offsetEntry : group.committedOffsets.entrySet()) {
                String topicPartition = offsetEntry.getKey();
                Long offset = offsetEntry.getValue();
                if (topicPartition == null || topicPartition.isBlank()) {
                    throw new IOException("Metadata group " + groupId + " has blank committed offset key");
                }
                if (offset == null || offset < 0) {
                    throw new IOException("Metadata group " + groupId
                            + " has invalid committed offset for " + topicPartition);
                }
            }
        }
    }

    private void validateDocument(JsonNode root) throws IOException {
        if (root == null || root.isNull() || !root.isObject()) {
            throw new IOException("Metadata document must be a JSON object");
        }
        requireObject(root, "brokers");
        requireObject(root, "topics");
        requireObject(root, "consumerGroups");
    }

    private void migrateOrValidateFormat(ObjectNode root) throws IOException {
        JsonNode format = root.get("metadataFormatVersion");
        if (format == null) {
            root.put("metadataFormatVersion", 3);
            root.put("controllerEpoch", 0L);
            root.put("metadataVersion", 0L);
            JsonNode topics = root.get("topics");
            if (topics != null && topics.isObject()) {
                topics.elements().forEachRemaining(topic -> {
                    JsonNode partitions = topic.get("partitions");
                    if (partitions != null && partitions.isArray()) {
                        partitions.elements().forEachRemaining(partition -> {
                            if (partition instanceof ObjectNode object) {
                                object.put("leaderEpoch", 0L);
                                object.put("leaderEpochStartOffset", 0L);
                                object.put("leaderReady", !object.path("frozen").asBoolean(false));
                            }
                        });
                    }
                });
            }
            return;
        }
        if (!format.canConvertToInt() || format.intValue() != 3) {
            throw new IOException("Unsupported metadataFormatVersion " + format);
        }
    }

    private void requireObject(JsonNode root, String fieldName) throws IOException {
        JsonNode node = root.get(fieldName);
        if (node == null || !node.isObject()) {
            throw new IOException("Metadata document field " + fieldName + " must be an object");
        }
    }

    private void quarantineCorruptMetadata() {
        try {
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
                    .withZone(clock.getZone())
                    .format(clock.instant());
            Path corruptFile = metadataFile.resolveSibling(metadataFile.getFileName() + "." + timestamp + ".corrupt");
            Files.move(metadataFile, corruptFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException quarantineFailure) {
            throw new IllegalStateException("Failed to quarantine corrupt metadata file " + metadataFile,
                    quarantineFailure);
        }
    }

    private void forceFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private void forceDirectory(Path directory) {
        if (directory == null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignored) {
            // Some platforms do not support forcing a directory channel.
        }
    }
}

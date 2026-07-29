package com.minimq.broker.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.minimq.common.error.ErrorCode;
import com.minimq.common.validation.TopicNames;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

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
            migrateAndValidate(metadata);
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
            migrateAndValidate(metadata);
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
    public synchronized VersionedClusterMetadata read(Duration timeout) {
        requirePositiveTimeout(timeout);
        // Local file operations have no interruptible remote stage; validation still
        // makes the caller's deadline contract explicit at this boundary.
        return read();
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
    public synchronized UpdateResult initialize(ClusterMetadata metadata, Duration timeout) {
        requirePositiveTimeout(timeout);
        return initialize(metadata);
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

    @Override
    public synchronized UpdateResult compareAndSet(
            long expectedRevision, ClusterMetadata metadata, Duration timeout) {
        requirePositiveTimeout(timeout);
        return compareAndSet(expectedRevision, metadata);
    }

    private static void requirePositiveTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    private static void validate(ClusterMetadata metadata) throws IOException {
        if (metadata == null) {
            throw new IOException("Metadata document is null");
        }
        if (metadata.brokers == null || metadata.topics == null
                || metadata.topicTombstones == null
                || metadata.retryTopicBySource == null
                || metadata.deadLetterTopicBySource == null
                || metadata.consumerGroups == null) {
            throw new IOException("Metadata document has missing top-level maps");
        }
        if (metadata.metadataFormatVersion != ClusterMetadata.CURRENT_FORMAT_VERSION) {
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
        Map<String, String> topicStorageIdentities = new HashMap<>();
        for (var entry : metadata.topics.entrySet()) {
            String topicName = entry.getKey();
            ClusterMetadata.TopicEntry topic = entry.getValue();
            if (topicName == null || topicName.isBlank()) {
                throw new IOException("Metadata topic key is blank");
            }
            if (!TopicNames.isValidReference(topicName)) {
                throw new IOException("Metadata topic key is invalid");
            }
            registerTopicStorageIdentity(topicStorageIdentities, topicName);
            if (topic == null) {
                throw new IOException("Metadata topic " + topicName + " is null");
            }
            if (topic.topicName == null || topic.topicName.isBlank()) {
                throw new IOException("Metadata topic key " + topicName + " has blank topicName");
            }
            if (!TopicNames.isValidReference(topic.topicName)) {
                throw new IOException("Metadata topic key " + topicName + " has invalid topicName");
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
                if (partition.replicaProgress == null) {
                    throw new IOException("Metadata topic " + topicName + " has null replica progress map");
                }
                if (partition.lastCompleteWatermark == null) {
                    throw new IOException("Metadata topic " + topicName + " has null watermark snapshot");
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
                for (var progressEntry : partition.replicaProgress.entrySet()) {
                    Integer brokerId = progressEntry.getKey();
                    ClusterMetadata.ReplicaProgressEntry progress = progressEntry.getValue();
                    if (brokerId == null || progress == null
                            || progress.brokerId != brokerId
                            || partition.replicaBrokerIds == null
                            || !partition.replicaBrokerIds.contains(brokerId)) {
                        throw new IOException("Metadata topic " + topicName
                                + " has invalid replica progress for broker " + brokerId);
                    }
                    validateReplicaProgress(topicName, brokerId, progress);
                }
                validateWatermarkSnapshot(topicName, partition.lastCompleteWatermark);
            }
        }
        for (var entry : metadata.topicTombstones.entrySet()) {
            String topicName = entry.getKey();
            ClusterMetadata.TopicTombstone tombstone = entry.getValue();
            if (topicName == null || topicName.isBlank()) {
                throw new IOException("Metadata tombstone key is blank");
            }
            if (!TopicNames.isValidReference(topicName)) {
                throw new IOException("Metadata tombstone key is invalid");
            }
            registerTopicStorageIdentity(topicStorageIdentities, topicName);
            if (tombstone == null) {
                throw new IOException("Metadata tombstone " + topicName + " is null");
            }
            if (tombstone.topicName == null || tombstone.topicName.isBlank()) {
                throw new IOException("Metadata tombstone key " + topicName + " has blank topicName");
            }
            if (!TopicNames.isValidReference(tombstone.topicName)) {
                throw new IOException("Metadata tombstone key " + topicName + " has invalid topicName");
            }
            if (!topicName.equals(tombstone.topicName)) {
                throw new IOException("Metadata tombstone key " + topicName
                        + " does not match topicName " + tombstone.topicName);
            }
            if (!"DELETING".equals(tombstone.state) && !"DELETED".equals(tombstone.state)) {
                throw new IOException("Metadata tombstone " + topicName + " has invalid state");
            }
            if (tombstone.tombstoneVersion <= 0 || tombstone.deletedAtEpochMs < 0) {
                throw new IOException("Metadata tombstone " + topicName + " has invalid version or timestamp");
            }
            if (tombstone.requiredBrokerIds == null || tombstone.acknowledgedBrokerIds == null) {
                throw new IOException("Metadata tombstone " + topicName + " has null broker sets");
            }
            for (Integer brokerId : tombstone.requiredBrokerIds) {
                if (brokerId == null || brokerId < 0) {
                    throw new IOException("Metadata tombstone " + topicName + " has invalid required broker");
                }
            }
            for (Integer brokerId : tombstone.acknowledgedBrokerIds) {
                if (brokerId == null || brokerId < 0) {
                    throw new IOException("Metadata tombstone " + topicName + " has invalid acknowledged broker");
                }
            }
            if (metadata.topics.containsKey(topicName)) {
                throw new IOException("Metadata topic " + topicName + " is active and tombstoned");
            }
            if ("DELETED".equals(tombstone.state)
                    && !tombstone.acknowledgedBrokerIds.containsAll(tombstone.requiredBrokerIds)) {
                throw new IOException("Metadata deleted tombstone " + topicName
                        + " is missing required cleanup acknowledgements");
            }
        }
        validateReliabilityLinks(metadata, metadata.retryTopicBySource, "retry");
        validateReliabilityLinks(metadata, metadata.deadLetterTopicBySource, "dead-letter");
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
            if (!TopicNames.isValidReference(group.topicName)) {
                throw new IOException("Metadata consumer group " + groupId + " has invalid topicName");
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
            root.put("metadataFormatVersion", ClusterMetadata.CURRENT_FORMAT_VERSION);
            root.put("controllerEpoch", 0L);
            root.put("metadataVersion", 0L);
            migratePartitionDefaults(root);
            return;
        }
        if (!format.canConvertToInt()) {
            throw new IOException("Unsupported metadataFormatVersion " + format);
        }
        int version = format.intValue();
        if (version == 3) {
            root.put("metadataFormatVersion", ClusterMetadata.CURRENT_FORMAT_VERSION);
            migratePartitionDefaults(root);
            return;
        }
        if (version == ClusterMetadata.CURRENT_FORMAT_VERSION) {
            migratePartitionDefaults(root);
            return;
        }
        throw new IOException("Unsupported metadataFormatVersion " + format);
    }

    private void migratePartitionDefaults(ObjectNode root) {
        putIfMissing(root, "controllerEpoch", 0L);
        putIfMissing(root, "metadataVersion", 0L);
        if (!root.has("topicTombstones")) {
            root.putObject("topicTombstones");
        }
        if (!root.has("retryTopicBySource")) {
            root.putObject("retryTopicBySource");
        }
        if (!root.has("deadLetterTopicBySource")) {
            root.putObject("deadLetterTopicBySource");
        }
        JsonNode topics = root.get("topics");
        if (topics == null || !topics.isObject()) {
            return;
        }
        topics.elements().forEachRemaining(topic -> {
            JsonNode partitions = topic.get("partitions");
            if (partitions == null || !partitions.isArray()) {
                return;
            }
            partitions.elements().forEachRemaining(partition -> {
                if (partition instanceof ObjectNode object) {
                    putIfMissing(object, "leaderEpoch", 0L);
                    putIfMissing(object, "leaderEpochStartOffset", 0L);
                    if (!object.has("leaderReady")) {
                        object.put("leaderReady", !object.path("frozen").asBoolean(false));
                    }
                    if (!object.has("replicaLogEndOffsets")) {
                        object.putObject("replicaLogEndOffsets");
                    }
                    JsonNode progressNode = object.get("replicaProgress");
                    ObjectNode progress = null;
                    if (progressNode == null) {
                        progress = object.putObject("replicaProgress");
                    } else if (progressNode.isObject()) {
                        progress = (ObjectNode) progressNode;
                    }
                    JsonNode replicaLeos = object.get("replicaLogEndOffsets");
                    if (progress != null && replicaLeos != null && replicaLeos.isObject()) {
                        ObjectNode progressMap = progress;
                        replicaLeos.fields().forEachRemaining(entry -> {
                            String brokerId = entry.getKey();
                            JsonNode brokerProgressNode = progressMap.get(brokerId);
                            ObjectNode progressObject = brokerProgressNode != null && brokerProgressNode.isObject()
                                    ? (ObjectNode) brokerProgressNode
                                    : progressMap.putObject(brokerId);
                            if (!progressObject.has("brokerId")) {
                                try {
                                    progressObject.put("brokerId", Integer.parseInt(brokerId));
                                } catch (NumberFormatException ignored) {
                                    progressObject.put("brokerId", -1);
                                }
                            }
                            if (!progressObject.has("physicalLogEndOffset")) {
                                progressObject.put("physicalLogEndOffset", entry.getValue().asLong(0L));
                            }
                            putIfMissing(progressObject, "visibleLogEndOffset", -1L);
                            putIfMissing(progressObject, "nextVisibleAtEpochMs", -1L);
                            putIfMissing(progressObject, "brokerEpoch", 0L);
                            putIfMissing(progressObject, "controllerEpoch", 0L);
                            putIfMissing(progressObject, "leaderEpoch", 0L);
                        });
                    }
                    JsonNode watermarkNode = object.get("lastCompleteWatermark");
                    ObjectNode watermark = null;
                    if (watermarkNode == null) {
                        watermark = object.putObject("lastCompleteWatermark");
                    } else if (watermarkNode.isObject()) {
                        watermark = (ObjectNode) watermarkNode;
                    }
                    if (watermark != null) {
                        putIfMissing(watermark, "physicalHighWatermark", -1L);
                        putIfMissing(watermark, "visibleHighWatermark", -1L);
                        putIfMissing(watermark, "nextVisibleAtEpochMs", -1L);
                        putIfMissing(watermark, "controllerEpoch", -1L);
                        putIfMissing(watermark, "leaderEpoch", -1L);
                    }
                }
            });
        });
    }

    static void migrateAndNormalize(ClusterMetadata metadata) throws IOException {
        if (metadata == null) {
            return;
        }
        if (metadata.metadataFormatVersion == 3) {
            metadata.metadataFormatVersion = ClusterMetadata.CURRENT_FORMAT_VERSION;
        }
        if (metadata.metadataFormatVersion != ClusterMetadata.CURRENT_FORMAT_VERSION) {
            return;
        }
        if (metadata.topics == null) {
            return;
        }
        for (ClusterMetadata.TopicEntry topic : metadata.topics.values()) {
            if (topic == null || topic.partitions == null) {
                continue;
            }
            for (ClusterMetadata.PartitionEntry partition : topic.partitions) {
                if (partition == null) {
                    continue;
                }
                if (partition.replicaLogEndOffsets == null) {
                    continue;
                }
                if (partition.replicaProgress == null) {
                    continue;
                }
                for (var leoEntry : partition.replicaLogEndOffsets.entrySet()) {
                    Integer brokerId = leoEntry.getKey();
                    Long physicalLeo = leoEntry.getValue();
                    if (brokerId == null || physicalLeo == null || physicalLeo < 0) {
                        continue;
                    }
                    partition.replicaProgress.computeIfAbsent(brokerId, id -> {
                        ClusterMetadata.ReplicaProgressEntry progress =
                                new ClusterMetadata.ReplicaProgressEntry();
                        progress.brokerId = id;
                        progress.physicalLogEndOffset = physicalLeo;
                        progress.visibleLogEndOffset = -1L;
                        progress.nextVisibleAtEpochMs = -1L;
                        progress.brokerEpoch = 0L;
                        progress.controllerEpoch = 0L;
                        progress.leaderEpoch = 0L;
                        return progress;
                    });
                }
                if (partition.lastCompleteWatermark == null) {
                    partition.lastCompleteWatermark = new ClusterMetadata.WatermarkSnapshot();
                }
            }
        }
    }

    static void migrateAndValidate(ClusterMetadata metadata) throws IOException {
        migrateAndNormalize(metadata);
        validate(metadata);
    }

    private static void putIfMissing(ObjectNode object, String fieldName, long value) {
        if (!object.has(fieldName)) {
            object.put(fieldName, value);
        }
    }

    private static void validateReliabilityLinks(
            ClusterMetadata metadata, java.util.Map<String, String> links, String linkType)
            throws IOException {
        for (var entry : links.entrySet()) {
            String source = entry.getKey();
            String target = entry.getValue();
            if (source == null || source.isBlank() || target == null || target.isBlank()) {
                throw new IOException("Metadata " + linkType + " link has blank source or target");
            }
            if (!TopicNames.isValidReference(source)
                    || !TopicNames.isValidReference(target)) {
                throw new IOException("Metadata " + linkType + " link has invalid source or target");
            }
            if (!metadata.topics.containsKey(source) || !metadata.topics.containsKey(target)) {
                throw new IOException("Metadata " + linkType + " link references inactive topic");
            }
        }
    }

    private static void registerTopicStorageIdentity(
            Map<String, String> identities, String topicName) throws IOException {
        String identity = TopicNames.portableStorageIdentity(topicName);
        String existing = identities.putIfAbsent(identity, topicName);
        if (existing != null && !existing.equals(topicName)) {
            throw new IOException("Metadata topics '" + existing + "' and '" + topicName
                    + "' share the same portable storage identity");
        }
    }

    private static void validateReplicaProgress(
            String topicName, Integer brokerId, ClusterMetadata.ReplicaProgressEntry progress) throws IOException {
        if (progress.physicalLogEndOffset < 0
                || progress.visibleLogEndOffset < -1
                || progress.nextVisibleAtEpochMs < -1
                || progress.brokerEpoch < 0
                || progress.controllerEpoch < 0
                || progress.leaderEpoch < 0) {
            throw new IOException("Metadata topic " + topicName
                    + " has invalid replica progress for broker " + brokerId);
        }
        if (progress.visibleLogEndOffset > progress.physicalLogEndOffset) {
            throw new IOException("Metadata topic " + topicName
                    + " has replica visible LEO beyond physical LEO for broker " + brokerId);
        }
        if (progress.visibleLogEndOffset == -1L && progress.nextVisibleAtEpochMs != -1L) {
            throw new IOException("Metadata topic " + topicName
                    + " has visible-unknown replica progress with a next-visible timestamp for broker " + brokerId);
        }
        if (progress.visibleLogEndOffset == progress.physicalLogEndOffset
                && progress.nextVisibleAtEpochMs != -1L) {
            throw new IOException("Metadata topic " + topicName
                    + " has fully visible replica progress with a next-visible timestamp for broker " + brokerId);
        }
        if (progress.visibleLogEndOffset >= 0
                && progress.visibleLogEndOffset < progress.physicalLogEndOffset
                && progress.nextVisibleAtEpochMs < 0) {
            throw new IOException("Metadata topic " + topicName
                    + " has partially visible replica progress without a next-visible timestamp for broker "
                    + brokerId);
        }
    }

    private static void validateWatermarkSnapshot(
            String topicName, ClusterMetadata.WatermarkSnapshot watermark) throws IOException {
        if (watermark.physicalHighWatermark < -1
                || watermark.visibleHighWatermark < -1
                || watermark.nextVisibleAtEpochMs < -1
                || watermark.controllerEpoch < -1
                || watermark.leaderEpoch < -1) {
            throw new IOException("Metadata topic " + topicName + " has invalid watermark snapshot");
        }
        if (watermark.physicalHighWatermark == -1L && watermark.visibleHighWatermark != -1L) {
            throw new IOException("Metadata topic " + topicName
                    + " has visible watermark without physical watermark");
        }
        if (watermark.visibleHighWatermark > watermark.physicalHighWatermark) {
            throw new IOException("Metadata topic " + topicName
                    + " has visible watermark beyond physical watermark");
        }
        if (watermark.visibleHighWatermark == -1L && watermark.nextVisibleAtEpochMs != -1L) {
            throw new IOException("Metadata topic " + topicName
                    + " has visible-unknown watermark with a next-visible timestamp");
        }
        if (watermark.visibleHighWatermark == watermark.physicalHighWatermark
                && watermark.nextVisibleAtEpochMs != -1L) {
            throw new IOException("Metadata topic " + topicName
                    + " has fully visible watermark with a next-visible timestamp");
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

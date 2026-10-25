package com.minimq.broker.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataStoreTest {

    @Test
    void loadReturnsEmptyMetadataWhenFileIsMissing(@TempDir Path dir) {
        ClusterMetadata metadata = new MetadataStore(dir).load();

        assertTrue(metadata.brokers.isEmpty());
        assertTrue(metadata.topics.isEmpty());
        assertTrue(metadata.consumerGroups.isEmpty());
    }

    @Test
    void loadFailsClearlyWhenMetadataIsCorruptAndRecoveryDisabled(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, "{", StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getMessage().contains("enable corrupt metadata recovery"));
        assertTrue(Files.exists(metadataFile));
    }

    @Test
    void loadQuarantinesCorruptMetadataWhenRecoveryEnabled(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, "{", StandardCharsets.UTF_8);
        Clock clock = Clock.fixed(Instant.parse("2026-07-09T00:00:00Z"), ZoneOffset.UTC);

        ClusterMetadata metadata = new MetadataStore(dir, true, clock).load();

        assertTrue(metadata.brokers.isEmpty());
        assertTrue(metadata.topics.isEmpty());
        assertTrue(metadata.consumerGroups.isEmpty());
        assertFalse(Files.exists(metadataFile));
        Path corruptFile = dir.resolve("cluster-metadata.json.20260709000000000.corrupt");
        assertTrue(Files.exists(corruptFile));
        assertEquals("{", Files.readString(corruptFile, StandardCharsets.UTF_8));
    }

    @Test
    void loadTreatsEmptyMetadataFileAsCorrupt(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, "", StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getMessage().contains("Failed to load metadata"));
        assertTrue(Files.exists(metadataFile));
    }

    @Test
    void loadRejectsSchemaIncompatibleMetadata(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, """
                {
                  "brokers": [],
                  "topics": {},
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getCause().getMessage().contains("brokers"));
    }

    @Test
    void loadRejectsNonObjectOrMissingTopLevelMetadata(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, "[]", "must be a JSON object");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {}
                }
                """, "consumerGroups");
    }

    @Test
    void loadRejectsBrokerValidationErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "brokers": {"1": null},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "broker 1 is null");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {"1": {"brokerId": 2, "host": "127.0.0.1", "port": 9092}},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "does not match brokerId");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {"1": {"brokerId": 1, "host": " ", "port": 9092}},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "blank host");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {"1": {"brokerId": 1, "host": "127.0.0.1", "port": 70000}},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "invalid port");
    }

    @Test
    void loadRejectsTopicKeyMismatch(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "payments",
                      "partitions": []
                    }
                  },
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getCause().getMessage().contains("does not match topicName"));
    }

    @Test
    void loadPreservesPortableLegacyTopicNames(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("cluster-metadata.json"), """
                {
                  "brokers": {},
                  "topics": {
                    "sales east": {"topicName": "sales east", "partitions": []},
                    "订单": {"topicName": "订单", "partitions": []}
                  },
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);

        ClusterMetadata metadata = new MetadataStore(dir).load();

        assertTrue(metadata.topics.containsKey("sales east"));
        assertTrue(metadata.topics.containsKey("订单"));
    }

    @Test
    void loadRejectsTopicNamesWithTheSamePortableStorageIdentity(@TempDir Path dir)
            throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {"topicName": "orders", "partitions": []},
                    "Orders": {"topicName": "Orders", "partitions": []}
                  },
                  "consumerGroups": {}
                }
                """, "portable storage identity");
    }

    @Test
    void loadRejectsTopicValidationErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"": {"topicName": "orders", "partitions": []}},
                  "consumerGroups": {}
                }
                """, "topic key is blank");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"../orders": {"topicName": "orders", "partitions": []}},
                  "consumerGroups": {}
                }
                """, "topic key is invalid");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"orders": null},
                  "consumerGroups": {}
                }
                """, "topic orders is null");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"orders": {"topicName": " ", "partitions": []}},
                  "consumerGroups": {}
                }
                """, "blank topicName");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"orders": {"topicName": "orders/bad", "partitions": []}},
                  "consumerGroups": {}
                }
                """, "invalid topicName");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {"orders": {"topicName": "orders", "partitions": [null]}},
                  "consumerGroups": {}
                }
                """, "null partition");
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "orders",
                      "partitions": [{"partitionId": -1, "leaderBrokerId": 0}]
                    }
                  },
                  "consumerGroups": {}
                }
                """, "invalid partitionId");
    }

    @Test
    void loadRejectsTopicTombstoneNameValidationErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, tombstoneJson("orders", "orders/bad"), "invalid topicName");
        assertInvalidMetadata(dir, tombstoneJson("", "orders"), "tombstone key is blank");
        assertInvalidMetadata(dir, tombstoneJson("../orders", "orders"), "tombstone key is invalid");
        assertInvalidMetadata(dir, tombstoneJson("orders", " "), "blank topicName");
    }

    @Test
    void loadRejectsReliabilityLinkNameValidationErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, reliabilityLinkJson("retryTopicBySource", "", "orders.retry"),
                "retry link has blank source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("retryTopicBySource", "orders", ""),
                "retry link has blank source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("retryTopicBySource", "../orders", "orders.retry"),
                "retry link has invalid source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("retryTopicBySource", "orders", "orders/bad"),
                "retry link has invalid source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("deadLetterTopicBySource", "", "orders.retry"),
                "dead-letter link has blank source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("deadLetterTopicBySource", "orders", ""),
                "dead-letter link has blank source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("deadLetterTopicBySource", "../orders", "orders.retry"),
                "dead-letter link has invalid source or target");
        assertInvalidMetadata(dir, reliabilityLinkJson("deadLetterTopicBySource", "orders", "orders/bad"),
                "dead-letter link has invalid source or target");
    }

    @Test
    void loadRejectsConsumerGroupValidationErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, groupJson("", """
                {"groupId": "g1", "topicName": "orders", "generationId": 0, "members": {}, "committedOffsets": {}}
                """), "consumer group key is blank");
        assertInvalidMetadata(dir, groupJson("g1", "null"), "consumer group g1 is null");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": " ", "topicName": "orders", "generationId": 0, "members": {}, "committedOffsets": {}}
                """), "blank groupId");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": "g2", "topicName": "orders", "generationId": 0, "members": {}, "committedOffsets": {}}
                """), "does not match groupId");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": "g1", "topicName": " ", "generationId": 0, "members": {}, "committedOffsets": {}}
                """), "blank topicName");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": "g1", "topicName": "../orders", "generationId": 0, "members": {}, "committedOffsets": {}}
                """), "invalid topicName");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": "g1", "topicName": "orders", "generationId": -1, "members": {}, "committedOffsets": {}}
                """), "invalid generationId");
        assertInvalidMetadata(dir, groupJson("g1", """
                {"groupId": "g1", "topicName": "orders", "generationId": 0, "members": null, "committedOffsets": {}}
                """), "missing member or offset maps");
    }

    @Test
    void loadRejectsConsumerGroupMemberAndOffsetErrors(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"": {"memberId": "m1", "lastHeartbeatMs": 1, "assignedPartitions": []}},
                  "committedOffsets": {}
                }
                """), "member key is blank");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"m1": null},
                  "committedOffsets": {}
                }
                """), "member m1 is null");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"m1": {"memberId": " ", "lastHeartbeatMs": 1, "assignedPartitions": []}},
                  "committedOffsets": {}
                }
                """), "blank memberId");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"m1": {"memberId": "m2", "lastHeartbeatMs": 1, "assignedPartitions": []}},
                  "committedOffsets": {}
                }
                """), "does not match memberId");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"m1": {"memberId": "m1", "lastHeartbeatMs": -1, "assignedPartitions": []}},
                  "committedOffsets": {}
                }
                """), "invalid lastHeartbeatMs");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {"m1": {"memberId": "m1", "lastHeartbeatMs": 1, "assignedPartitions": null}},
                  "committedOffsets": {}
                }
                """), "null assignedPartitions");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {},
                  "committedOffsets": {"": 1}
                }
                """), "blank committed offset key");
        assertInvalidMetadata(dir, groupJson("g1", """
                {
                  "groupId": "g1",
                  "topicName": "orders",
                  "generationId": 0,
                  "members": {},
                  "committedOffsets": {"orders:0": -1}
                }
                """), "invalid committed offset");
    }

    @Test
    void loadQuarantinesSchemaIncompatibleMetadataWhenRecoveryEnabled(@TempDir Path dir) throws IOException {
        Path metadataFile = dir.resolve("cluster-metadata.json");
        Files.writeString(metadataFile, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "orders",
                      "partitions": null
                    }
                  },
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);
        Clock clock = Clock.fixed(Instant.parse("2026-07-09T01:02:03.004Z"), ZoneOffset.UTC);

        ClusterMetadata metadata = new MetadataStore(dir, true, clock).load();

        assertTrue(metadata.topics.isEmpty());
        assertFalse(Files.exists(metadataFile));
        assertTrue(Files.exists(dir.resolve("cluster-metadata.json.20260709010203004.corrupt")));
    }

    @Test
    void saveWritesLoadableMetadataWithoutLeavingTempFile(@TempDir Path dir) {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.BrokerEntry broker = new ClusterMetadata.BrokerEntry();
        broker.brokerId = 1;
        broker.host = "127.0.0.1";
        broker.port = 9092;
        broker.online = true;
        metadata.brokers.put(1, broker);

        MetadataStore store = new MetadataStore(dir);
        store.save(metadata);

        assertFalse(Files.exists(dir.resolve("cluster-metadata.json.tmp")));
        ClusterMetadata loaded = store.load();
        assertEquals("127.0.0.1", loaded.brokers.get(1).host);
    }

    @Test
    void saveAndLoadPreservesReplicaLogEndOffsets(@TempDir Path dir) {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        ClusterMetadata.PartitionEntry partition = new ClusterMetadata.PartitionEntry();
        partition.partitionId = 0;
        partition.leaderBrokerId = 0;
        partition.replicaBrokerIds = List.of(0, 1);
        partition.isrBrokerIds = List.of(0, 1);
        partition.replicaLogEndOffsets.put(0, 12L);
        partition.replicaLogEndOffsets.put(1, 9L);
        topic.partitions.add(partition);
        metadata.topics.put(topic.topicName, topic);

        MetadataStore store = new MetadataStore(dir);
        store.save(metadata);

        assertEquals(Map.of(0, 12L, 1, 9L),
                store.load().topics.get("orders").partitions.get(0).replicaLogEndOffsets);
    }

    @Test
    void newMetadataDefaultsToFormat4WithUnknownWatermarkSnapshot() {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.PartitionEntry partition = new ClusterMetadata.PartitionEntry();

        assertEquals(4, metadata.metadataFormatVersion);
        assertEquals(-1L, partition.lastCompleteWatermark.physicalHighWatermark);
        assertEquals(-1L, partition.lastCompleteWatermark.visibleHighWatermark);
        assertEquals(-1L, partition.lastCompleteWatermark.nextVisibleAtEpochMs);
        assertEquals(-1L, partition.lastCompleteWatermark.controllerEpoch);
        assertEquals(-1L, partition.lastCompleteWatermark.leaderEpoch);
    }

    @Test
    void saveAndLoadPreservesReplicaProgressAndWatermarkSnapshot(@TempDir Path dir) {
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.controllerEpoch = 3L;
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        ClusterMetadata.PartitionEntry partition = new ClusterMetadata.PartitionEntry();
        partition.partitionId = 0;
        partition.leaderBrokerId = 0;
        partition.replicaBrokerIds = List.of(0, 1);
        partition.isrBrokerIds = List.of(0, 1);
        partition.replicaLogEndOffsets.put(0, 12L);
        partition.replicaLogEndOffsets.put(1, 9L);
        partition.replicaProgress.put(0, replicaProgress(0, 12L, 10L, 1_000L, 4L, 3L, 2L));
        partition.replicaProgress.put(1, replicaProgress(1, 9L, 9L, -1L, 5L, 3L, 2L));
        partition.lastCompleteWatermark = new ClusterMetadata.WatermarkSnapshot();
        partition.lastCompleteWatermark.physicalHighWatermark = 9L;
        partition.lastCompleteWatermark.visibleHighWatermark = 8L;
        partition.lastCompleteWatermark.nextVisibleAtEpochMs = 1_000L;
        partition.lastCompleteWatermark.controllerEpoch = 3L;
        partition.lastCompleteWatermark.leaderEpoch = 2L;
        topic.partitions.add(partition);
        metadata.topics.put(topic.topicName, topic);

        MetadataStore store = new MetadataStore(dir);
        store.save(metadata);
        ClusterMetadata.PartitionEntry loaded =
                store.load().topics.get("orders").partitions.get(0);

        assertEquals(4, store.load().metadataFormatVersion);
        assertEquals(12L, loaded.replicaProgress.get(0).physicalLogEndOffset);
        assertEquals(10L, loaded.replicaProgress.get(0).visibleLogEndOffset);
        assertEquals(1_000L, loaded.replicaProgress.get(0).nextVisibleAtEpochMs);
        assertEquals(4L, loaded.replicaProgress.get(0).brokerEpoch);
        assertEquals(3L, loaded.replicaProgress.get(0).controllerEpoch);
        assertEquals(2L, loaded.replicaProgress.get(0).leaderEpoch);
        assertEquals(9L, loaded.lastCompleteWatermark.physicalHighWatermark);
        assertEquals(8L, loaded.lastCompleteWatermark.visibleHighWatermark);
        assertEquals(1_000L, loaded.lastCompleteWatermark.nextVisibleAtEpochMs);
    }

    @Test
    void saveAndLoadPreservesBrokerEpochAndFrozenPartition(@TempDir Path dir) {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.BrokerEntry broker = new ClusterMetadata.BrokerEntry();
        broker.brokerId = 0;
        broker.host = "localhost";
        broker.port = 9092;
        broker.online = false;
        broker.brokerEpoch = 4L;
        metadata.brokers.put(0, broker);

        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        ClusterMetadata.PartitionEntry partition = new ClusterMetadata.PartitionEntry();
        partition.partitionId = 0;
        partition.leaderBrokerId = 0;
        partition.replicaBrokerIds = List.of(0);
        partition.isrBrokerIds = List.of();
        partition.replicaLogEndOffsets.put(0, 9L);
        partition.frozen = true;
        partition.activationRequired = true;
        partition.leaderReady = false;
        topic.partitions.add(partition);
        metadata.topics.put("orders", topic);

        MetadataStore store = new MetadataStore(dir);
        store.save(metadata);
        ClusterMetadata loaded = store.load();

        assertEquals(4L, loaded.brokers.get(0).brokerEpoch);
        assertTrue(loaded.topics.get("orders").partitions.get(0).frozen);
        assertTrue(loaded.topics.get("orders").partitions.get(0).activationRequired);
        assertTrue(loaded.topics.get("orders").partitions.get(0).isrBrokerIds.isEmpty());
    }

    @Test
    void compareAndSetAdvancesRevisionAndFencesStaleWriter(@TempDir Path dir) {
        MetadataStore repository = new MetadataStore(dir);
        VersionedClusterMetadata initial = repository.read();
        ClusterMetadata next = initial.metadata();
        next.controllerEpoch = 3L;

        ClusterMetadataRepository.UpdateResult committed =
                repository.compareAndSet(initial.revision(), next);
        ClusterMetadataRepository.UpdateResult stale =
                repository.compareAndSet(initial.revision(), new ClusterMetadata());

        assertEquals(com.minimq.common.error.ErrorCode.NONE, committed.errorCode());
        assertEquals(1L, committed.value().revision());
        assertEquals(1L, committed.value().metadata().metadataVersion);
        assertEquals(com.minimq.common.error.ErrorCode.CONTROLLER_FENCED, stale.errorCode());
        assertEquals(1L, stale.value().revision());
        assertEquals(3L, repository.read().metadata().controllerEpoch);
    }

    @Test
    void deadlineOverloadsAreExplicitAndValidateTheirBudgets(@TempDir Path dir) {
        MetadataStore repository = new MetadataStore(dir);
        ClusterMetadata initial = new ClusterMetadata();

        assertEquals(0L, repository.read(Duration.ofMillis(25L)).revision());
        assertEquals(
                com.minimq.common.error.ErrorCode.NONE,
                repository.initialize(initial, Duration.ofMillis(25L)).errorCode());
        assertEquals(
                com.minimq.common.error.ErrorCode.NONE,
                repository.compareAndSet(
                        0L, new ClusterMetadata(), Duration.ofMillis(25L)).errorCode());

        assertThrows(IllegalArgumentException.class,
                () -> repository.read(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> repository.initialize(initial, Duration.ofMillis(-1L)));
        assertThrows(IllegalArgumentException.class,
                () -> repository.compareAndSet(1L, initial, null));
    }

    @Test
    void initializeIsIdempotentForSameSnapshotAndRejectsConflicts(@TempDir Path dir) {
        MetadataStore repository = new MetadataStore(dir);
        ClusterMetadata initial = new ClusterMetadata();
        initial.controllerEpoch = 2L;

        ClusterMetadataRepository.UpdateResult created = repository.initialize(initial);
        ClusterMetadataRepository.UpdateResult repeated = repository.initialize(initial);
        ClusterMetadata conflicting = new ClusterMetadata();
        conflicting.controllerEpoch = 3L;
        ClusterMetadataRepository.UpdateResult conflict = repository.initialize(conflicting);

        assertEquals(com.minimq.common.error.ErrorCode.NONE, created.errorCode());
        assertEquals(com.minimq.common.error.ErrorCode.NONE, repeated.errorCode());
        assertEquals(com.minimq.common.error.ErrorCode.INVALID_REQUEST, conflict.errorCode());
        assertEquals(2L, repository.read().metadata().controllerEpoch);
    }

    @Test
    void loadMigratesLegacy25MetadataAndInitializesEpochFields(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("cluster-metadata.json"), """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "orders",
                      "partitions": [{
                        "partitionId": 0,
                        "leaderBrokerId": 0,
                        "replicaBrokerIds": [0],
                        "isrBrokerIds": [0],
                        "replicaLogEndOffsets": {"0": 7},
                        "frozen": false
                      }]
                    }
                  },
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);

        ClusterMetadata migrated = new MetadataStore(dir).load();
        ClusterMetadata.PartitionEntry partition = migrated.topics.get("orders").partitions.get(0);

        assertEquals(4, migrated.metadataFormatVersion);
        assertEquals(0L, migrated.controllerEpoch);
        assertEquals(0L, migrated.metadataVersion);
        assertEquals(0L, partition.leaderEpoch);
        assertEquals(0L, partition.leaderEpochStartOffset);
        assertTrue(partition.leaderReady);
        assertFalse(partition.activationRequired);
        assertEquals(7L, partition.replicaProgress.get(0).physicalLogEndOffset);
        assertEquals(-1L, partition.replicaProgress.get(0).visibleLogEndOffset);
        assertEquals(-1L, partition.replicaProgress.get(0).nextVisibleAtEpochMs);
        assertEquals(0L, partition.replicaProgress.get(0).brokerEpoch);
        assertEquals(0L, partition.replicaProgress.get(0).controllerEpoch);
        assertEquals(0L, partition.replicaProgress.get(0).leaderEpoch);
        assertEquals(-1L, partition.lastCompleteWatermark.visibleHighWatermark);
    }

    @Test
    void loadRejectsUnknownMetadataFormatVersion(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "metadataFormatVersion": 99,
                  "controllerEpoch": 0,
                  "metadataVersion": 0,
                  "brokers": {},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "metadataFormatVersion");
    }

    @Test
    void loadRejectsFrozenPartitionWithNonEmptyIsr(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "orders",
                      "partitions": [{
                        "partitionId": 0,
                        "leaderBrokerId": 0,
                        "replicaBrokerIds": [0],
                        "isrBrokerIds": [0],
                        "replicaLogEndOffsets": {"0": 0},
                        "frozen": true
                      }]
                    }
                  },
                  "consumerGroups": {}
                }
                """, "frozen partition must have an empty ISR");
    }

    @Test
    void loadRejectsInvalidReplicaLogEndOffset(@TempDir Path dir) throws IOException {
        assertInvalidMetadata(dir, """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {
                      "topicName": "orders",
                      "partitions": [{
                        "partitionId": 0,
                        "leaderBrokerId": 0,
                        "replicaBrokerIds": [0, 1],
                        "isrBrokerIds": [0, 1],
                        "replicaLogEndOffsets": {"1": -1}
                      }]
                    }
                  },
                  "consumerGroups": {}
                }
                """, "invalid replica LEO");
    }

    @Test
    void migrateAndValidateRejectsInvalidControllerBrokerAndPartitionState() throws IOException {
        assertInvalidMetadata((ClusterMetadata) null, "document is null");

        ClusterMetadata unsupported = new ClusterMetadata();
        unsupported.metadataFormatVersion = 99;
        assertInvalidMetadata(unsupported, "Unsupported metadataFormatVersion");

        ClusterMetadata negativeEpoch = new ClusterMetadata();
        negativeEpoch.controllerEpoch = -1L;
        assertInvalidMetadata(negativeEpoch, "must be non-negative");

        ClusterMetadata invalidBroker = new ClusterMetadata();
        ClusterMetadata.BrokerEntry broker = new ClusterMetadata.BrokerEntry();
        broker.brokerId = 0;
        broker.host = "localhost";
        broker.port = 9092;
        broker.brokerEpoch = -1L;
        invalidBroker.brokers.put(0, broker);
        assertInvalidMetadata(invalidBroker, "invalid brokerEpoch");

        ClusterMetadata nullReplicaLists = metadataWithPartition();
        partition(nullReplicaLists).replicaBrokerIds = null;
        assertInvalidMetadata(nullReplicaLists, "null replica or ISR list");

        ClusterMetadata frozenLeaderOutsideReplicas = metadataWithPartition();
        partition(frozenLeaderOutsideReplicas).frozen = true;
        partition(frozenLeaderOutsideReplicas).leaderReady = false;
        partition(frozenLeaderOutsideReplicas).isrBrokerIds = List.of();
        partition(frozenLeaderOutsideReplicas).leaderBrokerId = 1;
        assertInvalidMetadata(frozenLeaderOutsideReplicas, "Leader must be a configured replica");

        ClusterMetadata nullLeo = metadataWithPartition();
        partition(nullLeo).replicaLogEndOffsets = null;
        assertInvalidMetadata(nullLeo, "null replica LEO map");

        ClusterMetadata nullProgress = metadataWithPartition();
        partition(nullProgress).replicaProgress = null;
        assertInvalidMetadata(nullProgress, "null replica progress map");

        ClusterMetadata missingWatermark = metadataWithPartition();
        missingWatermark.metadataFormatVersion = 3;
        partition(missingWatermark).lastCompleteWatermark = null;
        MetadataStore.migrateAndValidate(missingWatermark);
        assertTrue(partition(missingWatermark).lastCompleteWatermark != null);

        ClusterMetadata invalidLeaderEpoch = metadataWithPartition();
        partition(invalidLeaderEpoch).leaderEpoch = -1L;
        assertInvalidMetadata(invalidLeaderEpoch, "invalid Leader epoch state");

        ClusterMetadata frozenReady = metadataWithPartition();
        partition(frozenReady).frozen = true;
        partition(frozenReady).isrBrokerIds = List.of();
        assertInvalidMetadata(frozenReady, "frozen partition cannot be ready");

        ClusterMetadata invalidProgressKey = metadataWithPartition();
        partition(invalidProgressKey).replicaProgress.put(
                0, replicaProgress(1, 0L, 0L, -1L, 0L, 0L, 0L));
        assertInvalidMetadata(invalidProgressKey, "invalid replica progress");
    }

    @Test
    void migrateAndValidateRejectsInvalidTombstoneState() throws IOException {
        ClusterMetadata nullTombstone = new ClusterMetadata();
        nullTombstone.topicTombstones.put("orders", null);
        assertInvalidMetadata(nullTombstone, "tombstone orders is null");

        ClusterMetadata mismatchedName = metadataWithTombstone("DELETING");
        mismatchedName.topicTombstones.get("orders").topicName = "payments";
        assertInvalidMetadata(mismatchedName, "does not match topicName");

        ClusterMetadata invalidState = metadataWithTombstone("PENDING");
        assertInvalidMetadata(invalidState, "invalid state");

        ClusterMetadata invalidVersion = metadataWithTombstone("DELETING");
        invalidVersion.topicTombstones.get("orders").tombstoneVersion = 0L;
        assertInvalidMetadata(invalidVersion, "invalid version or timestamp");

        ClusterMetadata nullBrokerSets = metadataWithTombstone("DELETING");
        nullBrokerSets.topicTombstones.get("orders").requiredBrokerIds = null;
        assertInvalidMetadata(nullBrokerSets, "null broker sets");

        ClusterMetadata invalidRequiredBroker = metadataWithTombstone("DELETING");
        invalidRequiredBroker.topicTombstones.get("orders").requiredBrokerIds.add(-1);
        assertInvalidMetadata(invalidRequiredBroker, "invalid required broker");

        ClusterMetadata invalidAcknowledgedBroker = metadataWithTombstone("DELETING");
        invalidAcknowledgedBroker.topicTombstones.get("orders").acknowledgedBrokerIds.add(-1);
        assertInvalidMetadata(invalidAcknowledgedBroker, "invalid acknowledged broker");

        ClusterMetadata activeAndDeleted = metadataWithTombstone("DELETING");
        ClusterMetadata.TopicEntry activeTopic = new ClusterMetadata.TopicEntry();
        activeTopic.topicName = "orders";
        activeAndDeleted.topics.put("orders", activeTopic);
        assertInvalidMetadata(activeAndDeleted, "is active and tombstoned");

        ClusterMetadata missingAcknowledgement = metadataWithTombstone("DELETED");
        missingAcknowledgement.topicTombstones.get("orders").requiredBrokerIds.add(0);
        assertInvalidMetadata(missingAcknowledgement, "missing required cleanup acknowledgements");

        ClusterMetadata inactiveReliabilityTarget = new ClusterMetadata();
        inactiveReliabilityTarget.retryTopicBySource.put("orders", "orders.retry");
        assertInvalidMetadata(inactiveReliabilityTarget, "references inactive topic");
    }

    @Test
    void migrateAndValidateRejectsInvalidReplicaProgressAndWatermarks() throws IOException {
        ClusterMetadata negativeProgress = metadataWithReplicaProgress(-1L, -1L, -1L);
        assertInvalidMetadata(negativeProgress, "invalid replica progress");

        ClusterMetadata visibleBeyondPhysical = metadataWithReplicaProgress(1L, 2L, -1L);
        assertInvalidMetadata(visibleBeyondPhysical, "visible LEO beyond physical LEO");

        ClusterMetadata unknownVisibleWithTimestamp = metadataWithReplicaProgress(1L, -1L, 10L);
        assertInvalidMetadata(unknownVisibleWithTimestamp, "visible-unknown replica progress");

        ClusterMetadata fullyVisibleWithTimestamp = metadataWithReplicaProgress(1L, 1L, 10L);
        assertInvalidMetadata(fullyVisibleWithTimestamp, "fully visible replica progress");

        ClusterMetadata partiallyVisibleWithoutTimestamp = metadataWithReplicaProgress(1L, 0L, -1L);
        assertInvalidMetadata(partiallyVisibleWithoutTimestamp, "partially visible replica progress");

        ClusterMetadata negativeWatermark = metadataWithPartition();
        partition(negativeWatermark).lastCompleteWatermark.physicalHighWatermark = -2L;
        assertInvalidMetadata(negativeWatermark, "invalid watermark snapshot");

        ClusterMetadata visibleWithoutPhysical = metadataWithPartition();
        partition(visibleWithoutPhysical).lastCompleteWatermark.visibleHighWatermark = 0L;
        assertInvalidMetadata(visibleWithoutPhysical, "visible watermark without physical watermark");

        ClusterMetadata visibleWatermarkBeyondPhysical = metadataWithPartition();
        partition(visibleWatermarkBeyondPhysical).lastCompleteWatermark.physicalHighWatermark = 0L;
        partition(visibleWatermarkBeyondPhysical).lastCompleteWatermark.visibleHighWatermark = 1L;
        assertInvalidMetadata(visibleWatermarkBeyondPhysical, "visible watermark beyond physical watermark");

        ClusterMetadata unknownVisibleWatermarkWithTimestamp = metadataWithPartition();
        partition(unknownVisibleWatermarkWithTimestamp).lastCompleteWatermark.physicalHighWatermark = 0L;
        partition(unknownVisibleWatermarkWithTimestamp).lastCompleteWatermark.nextVisibleAtEpochMs = 10L;
        assertInvalidMetadata(unknownVisibleWatermarkWithTimestamp, "visible-unknown watermark");

        ClusterMetadata fullyVisibleWatermarkWithTimestamp = metadataWithPartition();
        partition(fullyVisibleWatermarkWithTimestamp).lastCompleteWatermark.physicalHighWatermark = 0L;
        partition(fullyVisibleWatermarkWithTimestamp).lastCompleteWatermark.visibleHighWatermark = 0L;
        partition(fullyVisibleWatermarkWithTimestamp).lastCompleteWatermark.nextVisibleAtEpochMs = 10L;
        assertInvalidMetadata(fullyVisibleWatermarkWithTimestamp, "fully visible watermark");
    }

    @Test
    void migrateAndNormalizeToleratesIncompletePreValidationState() throws IOException {
        MetadataStore.migrateAndNormalize(null);

        ClusterMetadata unsupported = new ClusterMetadata();
        unsupported.metadataFormatVersion = 99;
        MetadataStore.migrateAndNormalize(unsupported);

        ClusterMetadata sparse = new ClusterMetadata();
        sparse.topics.put("null-topic", null);
        ClusterMetadata.TopicEntry nullPartitions = new ClusterMetadata.TopicEntry();
        nullPartitions.partitions = null;
        sparse.topics.put("null-partitions", nullPartitions);
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        topic.partitions.add(null);
        ClusterMetadata.PartitionEntry nullLeo = new ClusterMetadata.PartitionEntry();
        nullLeo.replicaLogEndOffsets = null;
        topic.partitions.add(nullLeo);
        ClusterMetadata.PartitionEntry nullProgress = new ClusterMetadata.PartitionEntry();
        nullProgress.replicaProgress = null;
        topic.partitions.add(nullProgress);
        ClusterMetadata.PartitionEntry missingWatermark = new ClusterMetadata.PartitionEntry();
        missingWatermark.replicaBrokerIds = List.of(0);
        missingWatermark.replicaLogEndOffsets.put(0, 1L);
        missingWatermark.lastCompleteWatermark = null;
        topic.partitions.add(missingWatermark);
        sparse.topics.put("orders", topic);

        MetadataStore.migrateAndNormalize(sparse);

        assertTrue(missingWatermark.replicaProgress.containsKey(0));
        assertEquals(1L, missingWatermark.replicaProgress.get(0).physicalLogEndOffset);
        assertTrue(missingWatermark.lastCompleteWatermark != null);
    }

    @Test
    void loadMigratesVersionThreeAndRejectsNonNumericFormat(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("cluster-metadata.json"), """
                {
                  "metadataFormatVersion": 3,
                  "brokers": {},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, StandardCharsets.UTF_8);
        assertEquals(ClusterMetadata.CURRENT_FORMAT_VERSION, new MetadataStore(dir).load().metadataFormatVersion);

        assertInvalidMetadata(dir, """
                {
                  "metadataFormatVersion": "bad",
                  "brokers": {},
                  "topics": {},
                  "consumerGroups": {}
                }
                """, "Unsupported metadataFormatVersion");
    }

    private static void assertInvalidMetadata(Path dir, String json, String expectedCause) throws IOException {
        Files.writeString(dir.resolve("cluster-metadata.json"), json, StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getCause().getMessage().contains(expectedCause), failure.getCause().getMessage());
    }

    private static void assertInvalidMetadata(ClusterMetadata metadata, String expectedMessage) {
        IOException failure = assertThrows(
                IOException.class,
                () -> MetadataStore.migrateAndValidate(metadata));
        assertTrue(failure.getMessage().contains(expectedMessage), failure.getMessage());
    }

    private static ClusterMetadata metadataWithPartition() {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        ClusterMetadata.PartitionEntry partition = new ClusterMetadata.PartitionEntry();
        partition.partitionId = 0;
        partition.leaderBrokerId = 0;
        partition.replicaBrokerIds = List.of(0);
        partition.isrBrokerIds = List.of(0);
        topic.partitions.add(partition);
        metadata.topics.put(topic.topicName, topic);
        return metadata;
    }

    private static ClusterMetadata.PartitionEntry partition(ClusterMetadata metadata) {
        return metadata.topics.get("orders").partitions.get(0);
    }

    private static ClusterMetadata metadataWithReplicaProgress(
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs) {
        ClusterMetadata metadata = metadataWithPartition();
        partition(metadata).replicaProgress.put(
                0,
                replicaProgress(
                        0,
                        physicalLogEndOffset,
                        visibleLogEndOffset,
                        nextVisibleAtEpochMs,
                        0L,
                        0L,
                        0L));
        return metadata;
    }

    private static ClusterMetadata metadataWithTombstone(String state) {
        ClusterMetadata metadata = new ClusterMetadata();
        ClusterMetadata.TopicTombstone tombstone = new ClusterMetadata.TopicTombstone();
        tombstone.topicName = "orders";
        tombstone.state = state;
        tombstone.tombstoneVersion = 1L;
        tombstone.deletedAtEpochMs = 0L;
        metadata.topicTombstones.put(tombstone.topicName, tombstone);
        return metadata;
    }

    private static ClusterMetadata.ReplicaProgressEntry replicaProgress(
            int brokerId,
            long physicalLogEndOffset,
            long visibleLogEndOffset,
            long nextVisibleAtEpochMs,
            long brokerEpoch,
            long controllerEpoch,
            long leaderEpoch) {
        ClusterMetadata.ReplicaProgressEntry progress = new ClusterMetadata.ReplicaProgressEntry();
        progress.brokerId = brokerId;
        progress.physicalLogEndOffset = physicalLogEndOffset;
        progress.visibleLogEndOffset = visibleLogEndOffset;
        progress.nextVisibleAtEpochMs = nextVisibleAtEpochMs;
        progress.brokerEpoch = brokerEpoch;
        progress.controllerEpoch = controllerEpoch;
        progress.leaderEpoch = leaderEpoch;
        return progress;
    }

    private static String groupJson(String groupId, String groupBody) {
        return """
                {
                  "brokers": {},
                  "topics": {},
                  "consumerGroups": {
                    "%s": %s
                  }
                }
                """.formatted(groupId, groupBody);
    }

    private static String tombstoneJson(String key, String topicName) {
        return """
                {
                  "brokers": {},
                  "topics": {},
                  "topicTombstones": {
                    "%s": {
                      "topicName": "%s",
                      "state": "DELETING",
                      "tombstoneVersion": 1,
                      "deletedAtEpochMs": 0,
                      "requiredBrokerIds": [],
                      "acknowledgedBrokerIds": []
                    }
                  },
                  "consumerGroups": {}
                }
                """.formatted(key, topicName);
    }

    private static String reliabilityLinkJson(String mapName, String source, String target) {
        return """
                {
                  "brokers": {},
                  "topics": {
                    "orders": {"topicName": "orders", "partitions": []},
                    "orders.retry": {"topicName": "orders.retry", "partitions": []}
                  },
                  "%s": {"%s": "%s"},
                  "consumerGroups": {}
                }
                """.formatted(mapName, source, target);
    }
}

package com.minimq.broker.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
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

    private static void assertInvalidMetadata(Path dir, String json, String expectedCause) throws IOException {
        Files.writeString(dir.resolve("cluster-metadata.json"), json, StandardCharsets.UTF_8);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new MetadataStore(dir).load());

        assertTrue(failure.getCause().getMessage().contains(expectedCause), failure.getCause().getMessage());
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
}

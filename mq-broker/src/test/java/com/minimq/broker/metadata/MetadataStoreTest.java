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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataStoreTest {

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
}

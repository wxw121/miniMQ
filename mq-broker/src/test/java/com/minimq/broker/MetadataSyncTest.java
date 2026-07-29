package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import com.minimq.protocol.message.ProtocolMessages.ControllerMetadata;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Request;
import com.minimq.protocol.message.ProtocolMessages.MetadataV2Response;
import com.minimq.protocol.message.ProtocolMessages.OpsBrokerInfo;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneInfo;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneSummary;
import com.minimq.protocol.message.ProtocolMessages.TopicMetadata;
import com.minimq.protocol.message.ProtocolMessages.TopicSummary;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataSyncTest {

    @TempDir
    Path tempDir;

    @Test
    void applyMetadataCopiesReplicaLogEndOffsetsAndFrozenState() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse response = new MetadataResponse(
                List.of(
                        new BrokerMetadata(0, "localhost", 9092),
                        new BrokerMetadata(1, "localhost", 9093)),
                List.of(new TopicMetadata(
                        "orders",
                        List.of(new PartitionMetadata(
                                0,
                                0,
                                List.of(0, 1),
                                List.of(),
                                Map.of(0, 12L, 1, 9L),
                                true)))));

        MetadataSync.applyMetadata(controller, response);

        assertEquals(
                Map.of(0, 12L, 1, 9L),
                controller.snapshot().topics.get("orders").partitions.get(0).replicaLogEndOffsets);
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).frozen);
        assertTrue(controller.snapshot().topics.get("orders").partitions.get(0).isrBrokerIds.isEmpty());
    }

    @Test
    void applyMetadataV2PreservesBrokerEpochAndOfflineState() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse response = new MetadataResponse(
                List.of(new BrokerMetadata(0, "localhost", 9092)),
                List.of(),
                null,
                11L);
        MetadataV2Response responseV2 = new MetadataV2Response(
                new V4BoundedList<>(
                        2,
                        false,
                        List.of(
                                new OpsBrokerInfo(0, "localhost", 9092, "ACTIVE", 7L, 0L),
                                new OpsBrokerInfo(1, "localhost", 9093, "OFFLINE", 9L, 0L))),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                new ControllerMetadata(0, "localhost", 9092, 3L),
                11L,
                4);

        MetadataSync.applyMetadata(controller, response, responseV2);

        assertEquals(7L, controller.getBroker(0).brokerEpoch);
        assertTrue(controller.getBroker(0).online);
        assertEquals(9L, controller.getBroker(1).brokerEpoch);
        assertFalse(controller.getBroker(1).online);
        assertEquals(3L, controller.snapshot().controllerEpoch);
    }

    @Test
    void applyMetadataV2CopiesTopicTombstones() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse response = new MetadataResponse(
                List.of(new BrokerMetadata(0, "localhost", 9092)),
                List.of(),
                null,
                11L);
        MetadataV2Response responseV2 = new MetadataV2Response(
                new V4BoundedList<>(
                        1,
                        false,
                        List.of(new OpsBrokerInfo(0, "localhost", 9092, "ACTIVE", 7L, 0L))),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(
                        1,
                        false,
                        List.of(new TopicTombstoneSummary(
                                "orders",
                                new TopicTombstoneInfo(
                                        "DELETING",
                                        22L,
                                        1234L,
                                        new V4BoundedList<>(2, false, List.of(0, 1)),
                                        new V4BoundedList<>(1, false, List.of(0)),
                                        new V4BoundedList<>(1, false, List.of(1)))))),
                new ControllerMetadata(0, "localhost", 9092, 3L),
                11L,
                4);

        MetadataSync.applyMetadata(controller, response, responseV2);

        var tombstone = controller.snapshot().topicTombstones.get("orders");
        assertEquals("orders", tombstone.topicName);
        assertEquals("DELETING", tombstone.state);
        assertEquals(22L, tombstone.tombstoneVersion);
        assertEquals(1234L, tombstone.deletedAtEpochMs);
        assertEquals(Set.of(0, 1), tombstone.requiredBrokerIds);
        assertEquals(Set.of(0), tombstone.acknowledgedBrokerIds);
    }

    @Test
    void applyMetadataRejectsMixedRevisionsWithoutModifyingSnapshot() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        installVersion(controller, 7L);
        MetadataResponse legacy = new MetadataResponse(
                List.of(new BrokerMetadata(0, "legacy", 9092)),
                List.of(),
                null,
                10L);
        MetadataV2Response v2 = metadataV2(11L, "v2");

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.applyMetadata(controller, legacy, v2));

        assertTrue(failure.getMessage().contains("10"), failure.getMessage());
        assertTrue(failure.getMessage().contains("11"), failure.getMessage());
        assertEquals(7L, controller.snapshot().metadataVersion);
        assertTrue(controller.snapshot().brokers.isEmpty());
    }

    @Test
    void refreshRetriesMixedRevisionAndInstallsAConsistentPair() throws Exception {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        FakeControllerClient client = new FakeControllerClient(
                List.of(metadata(10L, "legacy-10"), metadata(11L, "legacy-11")),
                List.of(metadataV2(11L, "v2-11"), metadataV2(11L, "v2-11")));

        MetadataSync.refresh(controller, client);

        assertEquals(2, client.legacyFetches);
        assertEquals(2, client.v2Fetches);
        assertEquals(11L, controller.snapshot().metadataVersion);
        assertEquals("v2-11", controller.getBroker(0).host);
    }

    @Test
    void refreshKeepsOldSnapshotAfterBoundedRevisionMismatch() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        installVersion(controller, 7L);
        FakeControllerClient client = new FakeControllerClient(
                List.of(
                        metadata(10L, "legacy-10-a"),
                        metadata(10L, "legacy-10-b"),
                        metadata(10L, "legacy-10-c")),
                List.of(
                        metadataV2(11L, "v2-11-a"),
                        metadataV2(11L, "v2-11-b"),
                        metadataV2(11L, "v2-11-c")));

        assertThrows(IllegalStateException.class, () -> MetadataSync.refresh(controller, client));

        assertEquals(3, client.legacyFetches);
        assertEquals(3, client.v2Fetches);
        assertEquals(7L, controller.snapshot().metadataVersion);
        assertTrue(controller.snapshot().brokers.isEmpty());
    }

    @Test
    void refreshWithDeadlineInstallsAConsistentPairUsingRemainingBudget() throws Exception {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        FakeControllerClient client = new FakeControllerClient(
                List.of(metadata(12L, "legacy")),
                List.of(metadataV2(12L, "v2")));

        MetadataSync.refresh(
                controller,
                client,
                TimeUnit.MILLISECONDS.toNanos(50L),
                () -> 0L);

        assertEquals(List.of(50L), client.legacyTimeouts);
        assertEquals(List.of(50L), client.v2Timeouts);
        assertEquals(12L, controller.snapshot().metadataVersion);
        assertEquals("v2", controller.getBroker(0).host);
    }

    @Test
    void refreshPaginatesMetadataV2AndFetchesLargeTopicSetByName() throws Exception {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        int itemCount = 10_001;
        List<TopicSummary> topics = new ArrayList<>(itemCount);
        List<TopicTombstoneSummary> tombstones = new ArrayList<>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            topics.add(new TopicSummary(topicName("active", i), 1, 1, null, null));
            tombstones.add(new TopicTombstoneSummary(
                    topicName("deleted", i),
                    new TopicTombstoneInfo(
                            "DELETED",
                            i + 1L,
                            1_000L + i,
                            new V4BoundedList<>(1, false, List.of(0)),
                            new V4BoundedList<>(1, false, List.of(0)),
                            new V4BoundedList<>(0, false, List.of()))));
        }
        PagingControllerClient client = new PagingControllerClient(31L, topics, tombstones);

        MetadataSync.refresh(controller, client);

        assertEquals(itemCount, controller.snapshot().topics.size());
        assertEquals(itemCount, controller.snapshot().topicTombstones.size());
        assertEquals(0, client.fullLegacyFetches);
        assertEquals(itemCount, client.exactTopicFetches);
        assertEquals(2, client.v2Requests.size());
        assertEquals(topicName("active", 9_999), client.v2Requests.get(1).topicStartAfter());
        assertEquals(topicName("deleted", 9_999), client.v2Requests.get(1).tombstoneStartAfter());
        assertEquals(31L, controller.snapshot().metadataVersion);
    }

    @Test
    void refreshWithDeadlineRejectsExpiredAndRepeatedlyInconsistentSnapshots() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        FakeControllerClient expired = new FakeControllerClient(
                List.of(metadata(12L, "legacy")),
                List.of(metadataV2(12L, "v2")));

        IllegalStateException deadlineFailure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.refresh(controller, expired, 10L, () -> 10L));
        assertTrue(deadlineFailure.getMessage().contains("deadline exceeded"));
        assertEquals(0, expired.legacyFetches);

        FakeControllerClient inconsistent = new FakeControllerClient(
                List.of(
                        metadata(20L, "legacy-a"),
                        metadata(20L, "legacy-b"),
                        metadata(20L, "legacy-c")),
                List.of(
                        metadataV2(21L, "v2-a"),
                        metadataV2(21L, "v2-b"),
                        metadataV2(21L, "v2-c")));
        IllegalStateException mismatchFailure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.refresh(
                        controller,
                        inconsistent,
                        TimeUnit.SECONDS.toNanos(1L),
                        () -> 0L));

        assertTrue(mismatchFailure.getMessage().contains("after 3 attempts"));
        assertTrue(mismatchFailure.getCause().getMessage().contains("legacy=20"));
    }

    @Test
    void applyMetadataRejectsTruncatedV2BrokerAndTombstoneLists() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse legacy = metadata(15L, "legacy");
        MetadataV2Response truncatedBrokers = new MetadataV2Response(
                new V4BoundedList<>(1, true, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                null,
                15L,
                4);
        MetadataV2Response truncatedTombstones = new MetadataV2Response(
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(1, true, List.of()),
                null,
                15L,
                4);

        IllegalStateException brokerFailure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.applyMetadata(controller, legacy, truncatedBrokers));
        IllegalStateException tombstoneFailure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.applyMetadata(controller, legacy, truncatedTombstones));

        assertTrue(brokerFailure.getMessage().contains("broker list is truncated"));
        assertTrue(tombstoneFailure.getMessage().contains("tombstone list is truncated"));
    }

    @Test
    void applyMetadataV2ReplacesReliabilityLinksAndRejectsTruncatedTopics() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        var initial = controller.snapshot();
        initial.retryTopicBySource.put("stale", "stale-retry");
        initial.deadLetterTopicBySource.put("stale", "stale-dlq");
        controller.installMetadata(new VersionedClusterMetadata(initial, 14L));

        MetadataResponse legacy = new MetadataResponse(
                List.of(new BrokerMetadata(0, "legacy", 9092)),
                List.of(
                        new TopicMetadata("orders", List.of()),
                        new TopicMetadata("orders-retry", List.of()),
                        new TopicMetadata("orders-dlq", List.of())),
                null,
                15L);
        MetadataV2Response complete = new MetadataV2Response(
                new V4BoundedList<>(1, false,
                        List.of(new OpsBrokerInfo(0, "legacy", 9092, "ACTIVE", 2L, 0L))),
                new V4BoundedList<>(3, false,
                        List.of(
                                new TopicSummary("orders", 0, 0, "orders-retry", "orders-dlq"),
                                new TopicSummary("orders-retry", 0, 0, null, null),
                                new TopicSummary("orders-dlq", 0, 0, null, null))),
                new V4BoundedList<>(0, false, List.of()),
                null,
                15L,
                4);

        MetadataSync.applyMetadata(controller, legacy, complete);

        assertEquals(Map.of("orders", "orders-retry"), controller.snapshot().retryTopicBySource);
        assertEquals(Map.of("orders", "orders-dlq"), controller.snapshot().deadLetterTopicBySource);

        MetadataV2Response truncated = new MetadataV2Response(
                complete.brokers(),
                new V4BoundedList<>(3, true, List.of()),
                complete.tombstones(),
                null,
                16L,
                4);
        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.applyMetadata(
                        controller,
                        new MetadataResponse(List.of(), List.of(), null, 16L),
                        truncated));
        assertTrue(failure.getMessage().contains("topic list is truncated"));
    }

    @Test
    void applyLegacyMetadataUsesLegacyControllerAndBrokers() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        MetadataResponse legacy = new MetadataResponse(
                List.of(new BrokerMetadata(3, "legacy", 9095)),
                List.of(),
                new ControllerMetadata(3, "legacy", 9095, 8L),
                16L);

        MetadataSync.applyMetadata(controller, legacy);

        assertEquals(8L, controller.snapshot().controllerEpoch);
        assertEquals("legacy", controller.getBroker(3).host);
        assertTrue(controller.getBroker(3).online);
    }

    @Test
    void refreshSkipsActiveAndNonLegacyControllerRoles() throws Exception {
        BrokerConfig config = BrokerConfig.parse(new String[] {"--admin-token", "test-token"});
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));

        MetadataSync.refresh(config, controller, new ControllerRole() {
            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public com.minimq.broker.coordination.ControllerIdentity currentLeader() {
                return null;
            }
        });
        MetadataSync.refresh(config, controller, new ControllerRole() {
            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public com.minimq.broker.coordination.ControllerIdentity currentLeader() {
                return null;
            }
        });

        assertTrue(controller.snapshot().brokers.isEmpty());
    }

    @Test
    void refreshDefaultLegacyRoleSkipsActiveController() throws Exception {
        BrokerConfig config = BrokerConfig.parse(new String[] {"--admin-token", "test-token"})
                .withLegacyController(true, "127.0.0.1", 19092);
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));

        MetadataSync.refresh(config, controller);

        assertTrue(controller.snapshot().brokers.isEmpty());
    }

    @Test
    void refreshRejectsLegacyFollowerWithoutLeaderIdentity() {
        BrokerConfig config = BrokerConfig.parse(new String[] {"--admin-token", "test-token"})
                .withLegacyController(false, "127.0.0.1", 19092);
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> MetadataSync.refresh(config, controller, new ControllerRole() {
                    @Override
                    public boolean isActive() {
                        return false;
                    }

                    @Override
                    public com.minimq.broker.coordination.ControllerIdentity currentLeader() {
                        return null;
                    }
                }));

        assertTrue(failure.getMessage().contains("not available"));
    }

    private static MetadataResponse metadata(long metadataVersion, String host) {
        return new MetadataResponse(
                List.of(new BrokerMetadata(0, host, 9092)),
                List.of(),
                null,
                metadataVersion);
    }

    private static void installVersion(ClusterController controller, long metadataVersion) {
        var snapshot = controller.snapshot();
        snapshot.metadataVersion = metadataVersion;
        controller.installMetadata(new VersionedClusterMetadata(snapshot, metadataVersion));
    }

    private static MetadataV2Response metadataV2(long metadataVersion, String host) {
        return new MetadataV2Response(
                new V4BoundedList<>(
                        1,
                        false,
                        List.of(new OpsBrokerInfo(0, host, 9092, "ACTIVE", 7L, 0L))),
                new V4BoundedList<>(0, false, List.of()),
                new V4BoundedList<>(0, false, List.of()),
                new ControllerMetadata(0, host, 9092, 3L),
                metadataVersion,
                4);
    }

    private static final class FakeControllerClient extends ControllerClient {
        private final Deque<MetadataResponse> legacyResponses;
        private final Deque<MetadataV2Response> v2Responses;
        private int legacyFetches;
        private int v2Fetches;
        private final List<Long> legacyTimeouts = new ArrayList<>();
        private final List<Long> v2Timeouts = new ArrayList<>();

        private FakeControllerClient(
                List<MetadataResponse> legacyResponses,
                List<MetadataV2Response> v2Responses) {
            this.legacyResponses = new ArrayDeque<>(legacyResponses);
            this.v2Responses = new ArrayDeque<>(v2Responses);
        }

        @Override
        public synchronized MetadataResponse fetchMetadata() {
            legacyFetches++;
            return legacyResponses.removeFirst();
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2() {
            v2Fetches++;
            return v2Responses.removeFirst();
        }

        @Override
        public synchronized MetadataResponse fetchMetadata(long responseTimeoutMs) {
            legacyTimeouts.add(responseTimeoutMs);
            return fetchMetadata();
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(long responseTimeoutMs) {
            v2Timeouts.add(responseTimeoutMs);
            return fetchMetadataV2();
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(
                MetadataV2Request request, long responseTimeoutMs) {
            v2Timeouts.add(responseTimeoutMs);
            return fetchMetadataV2();
        }
    }

    private static String topicName(String prefix, int index) {
        return String.format("%s-%05d", prefix, index);
    }

    private static final class PagingControllerClient extends ControllerClient {
        private final long metadataVersion;
        private final List<TopicSummary> topics;
        private final List<TopicTombstoneSummary> tombstones;
        private final List<MetadataV2Request> v2Requests = new ArrayList<>();
        private int fullLegacyFetches;
        private int exactTopicFetches;

        private PagingControllerClient(
                long metadataVersion,
                List<TopicSummary> topics,
                List<TopicTombstoneSummary> tombstones) {
            this.metadataVersion = metadataVersion;
            this.topics = topics;
            this.tombstones = tombstones;
        }

        @Override
        public synchronized MetadataResponse fetchMetadata(long responseTimeoutMs) {
            fullLegacyFetches++;
            throw new AssertionError(
                    "large metadata refresh must not request an unbounded legacy snapshot");
        }

        @Override
        public synchronized MetadataResponse fetchMetadata(String topicName, long responseTimeoutMs) {
            exactTopicFetches++;
            return new MetadataResponse(
                    List.of(new BrokerMetadata(0, "localhost", 9092)),
                    List.of(new TopicMetadata(
                            topicName,
                            List.of(new PartitionMetadata(0, 0, List.of(0), List.of(0))))),
                    null,
                    metadataVersion);
        }

        @Override
        public synchronized MetadataV2Response fetchMetadataV2(
                MetadataV2Request request, long responseTimeoutMs) {
            v2Requests.add(request);
            return new MetadataV2Response(
                    new V4BoundedList<>(
                            1,
                            false,
                            List.of(new OpsBrokerInfo(0, "localhost", 9092, "ACTIVE", 7L, 0L))),
                    pageTopics(topics, request.topicStartAfter(), request.topicLimit()),
                    pageTombstones(tombstones, request.tombstoneStartAfter(), request.topicLimit()),
                    new ControllerMetadata(0, "localhost", 9092, 3L),
                    metadataVersion,
                    4);
        }

        private static V4BoundedList<TopicSummary> pageTopics(
                List<TopicSummary> source, String startAfter, int limit) {
            List<TopicSummary> remaining = source.stream()
                    .filter(topic -> startAfter == null || topic.topicName().compareTo(startAfter) > 0)
                    .toList();
            List<TopicSummary> page = remaining.stream().limit(limit).toList();
            return new V4BoundedList<>(remaining.size(), remaining.size() > page.size(), page);
        }

        private static V4BoundedList<TopicTombstoneSummary> pageTombstones(
                List<TopicTombstoneSummary> source, String startAfter, int limit) {
            List<TopicTombstoneSummary> remaining = source.stream()
                    .filter(tombstone -> startAfter == null || tombstone.topicName().compareTo(startAfter) > 0)
                    .toList();
            List<TopicTombstoneSummary> page = remaining.stream().limit(limit).toList();
            return new V4BoundedList<>(remaining.size(), remaining.size() > page.size(), page);
        }
    }
}

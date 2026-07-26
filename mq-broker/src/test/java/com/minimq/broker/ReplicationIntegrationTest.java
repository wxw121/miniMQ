package com.minimq.broker;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.ControllerConnectionProxy;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.FetchRequest;
import com.minimq.protocol.message.ProtocolMessages.FetchResponse;
import com.minimq.protocol.message.ProtocolMessages.MarkBrokerOfflineRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.minimq.protocol.message.ProtocolMessages.PartitionMetadata;
import com.minimq.protocol.message.ProtocolMessages.ProduceRecord;
import com.minimq.protocol.message.ProtocolMessages.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationIntegrationTest {
    private static final String[] FAST_LEASE_ARGS = {
            "--broker-heartbeat-interval-ms", "50",
            "--broker-self-fence-timeout-ms", "150",
            "--controller-broker-lease-timeout-ms", "300"
    };

    @Test
    void clientFetchWaitsForReplicationHighWatermark() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("fetch-before-replication");
        Path dir1 = BrokerTestHarness.newDataDir("fetch-follower");
        Path dir2 = BrokerTestHarness.newDataDir("fetch-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient();
             TestNetworkClient leaderClient = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1, 2);
            createTopic(admin, "orders", 1, 3);
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "unreplicated");

            MqDecoder.DecodedResponse fetchResponse = leaderClient.send(
                    ApiKeys.FETCH, new FetchRequest("orders", 0, 0, 1024 * 1024));
            assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());
            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            assertEquals(0L, fetch.highWatermark());
            assertEquals(0, fetch.records().size());
        }
    }

    @Test
    void followerReplicatesLeaderLog() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("replication-leader");
        Path dir1 = BrokerTestHarness.newDataDir("replication-follower");
        Path dir2 = BrokerTestHarness.newDataDir("replication-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1, 2);
            createTopic(admin, "orders", 1, 3);

            TestNetworkClient leaderClient = new TestNetworkClient();
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "hello");

            waitForFollowerReplication(broker1, "orders", 0, 1);
            waitForFollowerReplication(broker2, "orders", 0, 1);
            waitForControllerLeo(admin, "orders", 0, 1, 1L);
            waitForControllerLeo(admin, "orders", 0, 2, 1L);
            waitForControllerLeo(admin, "orders", 0, 1, 1L);
            waitForControllerLeo(admin, "orders", 0, 2, 1L);

            MetadataResponse metadata = (MetadataResponse) admin.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")).body();
            var replicaLeos = metadata.topics().get(0).partitions().get(0).replicaLogEndOffsets();
            assertEquals(1L, replicaLeos.get(1));
            assertEquals(1L, replicaLeos.get(2));
        }
    }

    @Test
    void clientFetchAfterFailoverWithoutWaitingForFollowerFetch() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("failover-immediate-fetch-leader");
        Path dir1 = BrokerTestHarness.newDataDir("failover-immediate-fetch-follower");
        Path dir2 = BrokerTestHarness.newDataDir("failover-immediate-fetch-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1, 2);
            createTopic(admin, "orders", 1, 3);

            TestNetworkClient leaderClient = new TestNetworkClient();
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "before-failover");
            waitForFollowerReplication(broker1, "orders", 0, 1);
            waitForFollowerReplication(broker2, "orders", 0, 1);
            waitForControllerLeo(admin, "orders", 0, 1, 1L);
            waitForControllerLeo(admin, "orders", 0, 2, 1L);

            MqDecoder.DecodedResponse offline = admin.send(
                    ApiKeys.MARK_BROKER_OFFLINE, new MarkBrokerOfflineRequest(0));
            assertEquals(ErrorCode.NONE, offline.header().errorCode());

            int electedLeader = partition(metadata(admin, "orders"), "orders", 0).leaderBrokerId();
            BrokerTestHarness electedLeaderHarness = electedLeader == 1 ? broker1 : broker2;
            waitForLocalLeader(electedLeaderHarness, "orders", 0, electedLeader);
            PartitionMetadata readyPartition = waitForLeaderReady(admin, "orders", 0, electedLeader);
            TestNetworkClient newLeaderClient = new TestNetworkClient();
            newLeaderClient.connect("127.0.0.1", electedLeader == 1 ? broker1.port() : broker2.port());
            MqDecoder.DecodedResponse fetchResponse = newLeaderClient.send(
                    ApiKeys.FETCH, new FetchRequest(
                            "orders", 0,
                            readyPartition.leaderEpoch(),
                            0, 1024 * 1024));
            assertEquals(ErrorCode.NONE, fetchResponse.header().errorCode());
            FetchResponse fetch = (FetchResponse) fetchResponse.body();
            assertEquals(1, fetch.records().size());
            assertEquals(1L, fetch.highWatermark());
        }
    }

    @Test
    void leaderFailoverAllowsProduceOnPromotedFollower() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("failover-leader");
        Path dir1 = BrokerTestHarness.newDataDir("failover-follower");
        Path dir2 = BrokerTestHarness.newDataDir("failover-follower-2");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(2, dir2, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1, 2);
            createTopic(admin, "orders", 1, 3);

            TestNetworkClient leaderClient = new TestNetworkClient();
            leaderClient.connect("127.0.0.1", controller.port());
            produce(leaderClient, "orders", 0, "before-failover");
            waitForFollowerReplication(broker1, "orders", 0, 1);
            waitForFollowerReplication(broker2, "orders", 0, 1);
            waitForControllerLeo(admin, "orders", 0, 1, 1L);
            waitForControllerLeo(admin, "orders", 0, 2, 1L);

            MqDecoder.DecodedResponse offline = admin.send(
                    ApiKeys.MARK_BROKER_OFFLINE, new MarkBrokerOfflineRequest(0));
            assertEquals(ErrorCode.NONE, offline.header().errorCode());

            int electedLeader = partition(metadata(admin, "orders"), "orders", 0).leaderBrokerId();
            BrokerTestHarness electedLeaderHarness = electedLeader == 1 ? broker1 : broker2;
            waitForLocalLeader(electedLeaderHarness, "orders", 0, electedLeader);
            waitForLeaderReady(admin, "orders", 0, electedLeader);
            TestNetworkClient newLeaderClient = new TestNetworkClient();
            newLeaderClient.connect("127.0.0.1", electedLeader == 1 ? broker1.port() : broker2.port());
            produce(newLeaderClient, "orders", 0, "after-failover");
            assertEquals(2L, electedLeaderHarness.localHighWatermark("orders", 0),
                    "promoted Leader did not append the post-failover record");
            BrokerTestHarness survivingFollower = electedLeader == 1 ? broker2 : broker1;
            int survivingFollowerId = electedLeader == 1 ? 2 : 1;
            waitForLocalLeader(survivingFollower, "orders", 0, electedLeader);
            waitForFollowerReplication(survivingFollower, "orders", 0, 2);
            waitForControllerLeo(admin, "orders", 0, survivingFollowerId, 2L);

            FetchResponse fetch = waitForFetchRecords(
                    newLeaderClient, admin, "orders", 0, 0, 2, 2L);
            assertTrue(fetch.records().size() >= 2,
                    "promoted Leader fetch did not expose both records");
            assertTrue(fetch.highWatermark() >= 2,
                    "promoted Leader fetch high watermark did not include replicated record");
        }
    }

    @Test
    void produceReturnsNotLeaderOnFollowerBroker() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("not-leader-controller");
        Path dir1 = BrokerTestHarness.newDataDir("not-leader-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(dir0);
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(1, dir1, "127.0.0.1", controller.port());
             TestNetworkClient admin = new TestNetworkClient();
             TestNetworkClient followerClient = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1);
            createTopic(admin, "orders", 1, 2);

            followerClient.connect("127.0.0.1", broker1.port());
            MqDecoder.DecodedResponse response = followerClient.send(
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "orders",
                            0,
                            List.of(new ProduceRecord(null, "x".getBytes(StandardCharsets.UTF_8)))));
            assertEquals(ErrorCode.NOT_LEADER, response.header().errorCode());
        }
    }

    @Test
    void controllerIsolationFencesOldLeaderBeforeAutomaticFailover() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("lease-failover-controller");
        Path dir1 = BrokerTestHarness.newDataDir("lease-failover-isolated");
        Path dir2 = BrokerTestHarness.newDataDir("lease-failover-survivor");
        try (BrokerTestHarness controller = BrokerTestHarness.start(
                     BrokerTestHarness.controllerConfig(0, dir0, FAST_LEASE_ARGS));
             ControllerConnectionProxy proxy = ControllerConnectionProxy.start("127.0.0.1", controller.port());
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(
                     1, dir1, "127.0.0.1", proxy.port(), FAST_LEASE_ARGS);
             BrokerTestHarness broker2 = BrokerTestHarness.startFollower(
                     2, dir2, "127.0.0.1", controller.port(), FAST_LEASE_ARGS);
             TestNetworkClient admin = new TestNetworkClient();
             TestNetworkClient broker1Client = new TestNetworkClient();
             TestNetworkClient broker2Client = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            broker1Client.connect("127.0.0.1", broker1.port());
            broker2Client.connect("127.0.0.1", broker2.port());
            waitForOnlineBrokers(admin, 0, 1, 2);
            createTopic(admin, "orders", 2, 3);
            produce(broker1Client, "orders", 1, "before-failover");
            waitForFollowerReplication(controller, "orders", 1, 1);
            waitForFollowerReplication(broker2, "orders", 1, 1);
            waitForControllerLeo(admin, "orders", 1, 0, 1L);
            waitForControllerLeo(admin, "orders", 1, 2, 1L);

            proxy.disconnect();
            assertTrue(proxy.disconnected());

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            long fencedAt = -1L;
            long electedAt = -1L;
            int observedLeader = 1;
            BrokerAvailabilityGuard.State finalState = broker1.availabilityState();
            while (System.nanoTime() < deadline) {
                long observedAt = System.nanoTime();
                finalState = broker1.availabilityState();
                PartitionMetadata partition = partition(metadata(admin, "orders"), "orders", 1);
                observedLeader = partition.leaderBrokerId();
                if (finalState == BrokerAvailabilityGuard.State.FENCED && fencedAt < 0) {
                    fencedAt = observedAt;
                }
                if (observedLeader != 1 && electedAt < 0) {
                    if (fencedAt < 0) {
                        fail("Controller elected leader " + observedLeader
                                + " before broker 1 fenced; state=" + finalState);
                    }
                    electedAt = observedAt;
                }
                if (fencedAt >= 0 && electedAt >= 0) {
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(fencedAt >= 0,
                    "broker 1 was not observed fenced; final state=" + finalState
                            + ", leader=" + observedLeader);
            assertTrue(electedAt >= 0,
                    "replacement leader was not elected; final state=" + finalState
                            + ", leader=" + observedLeader);
            assertTrue(fencedAt <= electedAt,
                    "broker fenced after election: fencedAt=" + fencedAt + ", electedAt=" + electedAt);
            assertEquals(0, observedLeader);

            MqDecoder.DecodedResponse oldLeader = broker1Client.send(
                    ApiKeys.PRODUCE,
                    new ProduceRequest(
                            "orders",
                            1,
                            List.of(new ProduceRecord(null, "blocked".getBytes(StandardCharsets.UTF_8)))));
            assertEquals(ErrorCode.BROKER_FENCED, oldLeader.header().errorCode());
            produce(admin, "orders", 1, "after-failover");
        }
    }

    @Test
    void isolatedSoleLeaderFreezesAndRecoversWithHigherEpoch() throws Exception {
        Path dir0 = BrokerTestHarness.newDataDir("lease-freeze-controller");
        Path dir1 = BrokerTestHarness.newDataDir("lease-freeze-original-leader");
        try (BrokerTestHarness controller = BrokerTestHarness.start(
                     BrokerTestHarness.controllerConfig(0, dir0, FAST_LEASE_ARGS));
             ControllerConnectionProxy proxy = ControllerConnectionProxy.start("127.0.0.1", controller.port());
             BrokerTestHarness broker1 = BrokerTestHarness.startFollower(
                     1, dir1, "127.0.0.1", proxy.port(), FAST_LEASE_ARGS);
             TestNetworkClient admin = new TestNetworkClient()) {
            admin.connect("127.0.0.1", controller.port());
            waitForOnlineBrokers(admin, 0, 1);
            createTopic(admin, "single-replica", 2, 1);
            assertEquals(1, partition(metadata(admin, "single-replica"), "single-replica", 1)
                    .leaderBrokerId());
            long originalEpoch = broker1.brokerEpoch();

            proxy.disconnect();
            MetadataResponse frozenMetadata = waitForFrozenOfflinePartition(
                    admin, "single-replica", 1, 1, broker1);
            PartitionMetadata frozen = partition(frozenMetadata, "single-replica", 1);
            assertTrue(frozen.frozen());
            assertTrue(frozen.isrBrokerIds().isEmpty());
            assertEquals(1, frozen.leaderBrokerId());

            broker1.close();
            proxy.reconnect();
            try (BrokerTestHarness restarted = BrokerTestHarness.startFollower(
                    1, dir1, "127.0.0.1", proxy.port(), FAST_LEASE_ARGS)) {
                assertEquals(BrokerAvailabilityGuard.State.ACTIVE, restarted.availabilityState());
                assertTrue(restarted.brokerEpoch() > originalEpoch,
                        "restarted epoch=" + restarted.brokerEpoch() + ", original=" + originalEpoch);
                PartitionMetadata recovered = waitForRecoveredPartition(
                        admin, "single-replica", 1, 1);
                assertFalse(recovered.frozen());
                assertEquals(List.of(1), recovered.isrBrokerIds());
            }
        }
    }

    private static void waitForOnlineBrokers(TestNetworkClient admin, int... expectedIds)
            throws InterruptedException {
        Set<Integer> expected = Arrays.stream(expectedIds).boxed().collect(Collectors.toSet());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        Set<Integer> actual = Set.of();
        while (System.nanoTime() < deadline) {
            actual = metadata(admin, "").brokers().stream()
                    .map(broker -> broker.brokerId())
                    .collect(Collectors.toSet());
            if (actual.containsAll(expected)) {
                return;
            }
            Thread.sleep(10);
        }
        fail("Brokers did not become online; expected=" + expected + ", actual=" + actual);
    }

    private static MetadataResponse waitForFrozenOfflinePartition(
            TestNetworkClient admin,
            String topic,
            int partitionId,
            int brokerId,
            BrokerTestHarness broker) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        MetadataResponse observed = metadata(admin, topic);
        while (System.nanoTime() < deadline) {
            observed = metadata(admin, topic);
            PartitionMetadata partition = partition(observed, topic, partitionId);
            boolean brokerOffline = observed.brokers().stream()
                    .noneMatch(entry -> entry.brokerId() == brokerId);
            if (partition.frozen() && partition.isrBrokerIds().isEmpty()
                    && partition.leaderBrokerId() == brokerId && brokerOffline) {
                return observed;
            }
            Thread.sleep(10);
        }
        PartitionMetadata partition = partition(observed, topic, partitionId);
        fail("Partition did not freeze after isolation; state=" + broker.availabilityState()
                + ", frozen=" + partition.frozen()
                + ", leader=" + partition.leaderBrokerId()
                + ", isr=" + partition.isrBrokerIds()
                + ", brokers=" + observed.brokers());
        return observed;
    }

    private static PartitionMetadata waitForRecoveredPartition(
            TestNetworkClient admin, String topic, int partitionId, int brokerId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        PartitionMetadata observed = partition(metadata(admin, topic), topic, partitionId);
        while (System.nanoTime() < deadline) {
            observed = partition(metadata(admin, topic), topic, partitionId);
            if (!observed.frozen() && observed.isrBrokerIds().equals(List.of(brokerId))) {
                return observed;
            }
            Thread.sleep(10);
        }
        fail("Partition did not recover; frozen=" + observed.frozen()
                + ", leader=" + observed.leaderBrokerId() + ", isr=" + observed.isrBrokerIds());
        return observed;
    }

    private static MetadataResponse metadata(TestNetworkClient client, String topic)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(ApiKeys.METADATA, new MetadataRequest(topic));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
        return (MetadataResponse) response.body();
    }

    private static PartitionMetadata partition(
            MetadataResponse metadata, String topic, int partitionId) {
        return metadata.topics().stream()
                .filter(entry -> entry.topicName().equals(topic))
                .flatMap(entry -> entry.partitions().stream())
                .filter(entry -> entry.partitionId() == partitionId)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Partition not found: topic=" + topic + ", partition=" + partitionId));
    }

    private static void createTopic(
            TestNetworkClient client, String topic, int partitions, int replicationFactor)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.CREATE_TOPIC, new CreateTopicRequest(topic, partitions, replicationFactor));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private static void produce(TestNetworkClient client, String topic, int partition, String value)
            throws InterruptedException {
        MqDecoder.DecodedResponse discovered = client.send(
                ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
        assertEquals(ErrorCode.NONE, discovered.header().errorCode());
        DiscoverControllerResponse controller = (DiscoverControllerResponse) discovered.body();
        long leaderEpoch;
        try (TestNetworkClient metadataClient = new TestNetworkClient()) {
            metadataClient.connect(controller.controller().host(), controller.controller().port());
            leaderEpoch = partition(metadata(metadataClient, topic), topic, partition).leaderEpoch();
        }
        MqDecoder.DecodedResponse response = client.send(
                ApiKeys.PRODUCE,
                new ProduceRequest(
                        topic,
                        partition,
                        leaderEpoch,
                        List.of(new ProduceRecord(
                                "key".getBytes(StandardCharsets.UTF_8),
                                value.getBytes(StandardCharsets.UTF_8)))));
        assertEquals(ErrorCode.NONE, response.header().errorCode());
    }

    private static void waitForFollowerReplication(
            BrokerTestHarness follower, String topic, int partition, long expectedRecords)
            throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (follower.localHighWatermark(topic, partition) >= expectedRecords) {
                return;
            }
            Thread.sleep(50);
        }
        assertEquals(
                expectedRecords,
                follower.localHighWatermark(topic, partition),
                "follower did not replicate expected records");
    }

    private static void waitForControllerLeo(
            TestNetworkClient client,
            String topic,
            int partition,
            int brokerId,
            long expectedLeo) throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            MetadataResponse metadata = (MetadataResponse) client.send(
                    ApiKeys.METADATA, new MetadataRequest(topic)).body();
            Long actualLeo = metadata.topics().get(0).partitions().get(partition)
                    .replicaLogEndOffsets().get(brokerId);
            if (actualLeo != null && actualLeo >= expectedLeo) {
                return;
            }
            Thread.sleep(50);
        }
        MetadataResponse metadata = (MetadataResponse) client.send(
                ApiKeys.METADATA, new MetadataRequest(topic)).body();
        assertEquals(
                expectedLeo,
                metadata.topics().get(0).partitions().get(partition)
                        .replicaLogEndOffsets().get(brokerId),
                "Controller did not observe confirmed follower LEO");
    }

    private static void waitForLocalLeader(
            BrokerTestHarness broker, String topic, int partitionId, int expectedLeader)
            throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (broker.localLeaderBrokerId(topic, partitionId) == expectedLeader) {
                return;
            }
            Thread.sleep(50);
        }
        assertEquals(expectedLeader, broker.localLeaderBrokerId(topic, partitionId),
                "broker did not refresh the elected Leader");
    }

    private static PartitionMetadata waitForLeaderReady(
            TestNetworkClient admin, String topic, int partitionId, int expectedLeader)
            throws InterruptedException {
        PartitionMetadata observed = partition(metadata(admin, topic), topic, partitionId);
        for (int attempt = 0; attempt < 100; attempt++) {
            observed = partition(metadata(admin, topic), topic, partitionId);
            if (observed.leaderBrokerId() == expectedLeader && observed.leaderReady()) {
                return observed;
            }
            Thread.sleep(50);
        }
        fail("elected Leader did not complete activation; expected=" + expectedLeader
                + ", observedLeader=" + observed.leaderBrokerId()
                + ", leaderReady=" + observed.leaderReady()
                + ", leaderEpoch=" + observed.leaderEpoch()
                + ", leaderEpochStartOffset=" + observed.leaderEpochStartOffset());
        return observed;
    }

    private static FetchResponse waitForFetchRecords(
            TestNetworkClient fetchClient,
            TestNetworkClient admin,
            String topic,
            int partitionId,
            long fetchOffset,
            int expectedRecords,
            long expectedHighWatermark)
            throws InterruptedException {
        ErrorCode lastError = ErrorCode.NONE;
        FetchResponse observed = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            PartitionMetadata partition = partition(metadata(admin, topic), topic, partitionId);
            MqDecoder.DecodedResponse response = fetchClient.send(
                    ApiKeys.FETCH, new FetchRequest(
                            topic, partitionId, partition.leaderEpoch(), fetchOffset, 1024 * 1024));
            lastError = response.header().errorCode();
            if (lastError == ErrorCode.NONE) {
                observed = (FetchResponse) response.body();
                if (observed.records().size() >= expectedRecords
                        && observed.highWatermark() >= expectedHighWatermark) {
                    return observed;
                }
            }
            Thread.sleep(50);
        }
        assertEquals(ErrorCode.NONE, lastError,
                "fetch on promoted Leader did not succeed while waiting for records");
        assertTrue(observed != null && observed.records().size() >= expectedRecords,
                "fetch on promoted Leader returned "
                        + (observed == null ? 0 : observed.records().size())
                        + " records; expected at least " + expectedRecords);
        assertTrue(observed != null && observed.highWatermark() >= expectedHighWatermark,
                "fetch on promoted Leader high watermark="
                        + (observed == null ? -1 : observed.highWatermark())
                        + "; expected at least " + expectedHighWatermark);
        return observed;
    }
}

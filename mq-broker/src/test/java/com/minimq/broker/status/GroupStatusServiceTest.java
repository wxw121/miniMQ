package com.minimq.broker.status;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusResponse;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupStatusServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void statusReportsMembersAssignmentsAndLagRowsWithIndependentBounds() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 3, 1)));
        controller.updateReplicaStateV2("orders", 0, 0, 1L, 0L, 0L, 10L, 7L, 9_000L);
        controller.updateReplicaStateV2("orders", 1, 0, 1L, 0L, 0L, 5L, 5L, -1L);
        GroupCoordinator coordinator = new GroupCoordinator(controller);
        coordinator.joinGroup(new JoinGroupRequest("g1", "member-b", "orders"));
        var join = coordinator.joinGroup(new JoinGroupRequest("g1", "member-a", "orders"));
        controller.updateMetadata(metadata -> {
            metadata.consumerGroups.get("g1").members.get("member-b").assignedPartitions = List.of(2);
            metadata.consumerGroups.get("g1").members.get("member-a").assignedPartitions = List.of(0, 1);
            return null;
        });
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g1", "orders", 0, 4L, join.memberId(), join.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g1", "orders", 1, 8L, join.memberId(), join.generationId()));

        GroupStatusService service = new GroupStatusService(controller, () -> 123_456L);

        GroupStatusService.Result result = service.status("g1", "", 1, 2);

        assertEquals(ErrorCode.NONE, result.errorCode());
        GroupStatusResponse response = result.response();
        assertEquals("g1", response.groupId());
        assertEquals("orders", response.topicName());
        assertEquals(2, response.members().total());
        assertTrue(response.members().truncated());
        assertEquals(List.of("member-a"), response.members().items().stream().map(m -> m.memberId()).toList());
        assertEquals(2, response.members().items().get(0).assignments().total());
        assertFalse(response.members().items().get(0).assignments().truncated());
        assertEquals(3, response.partitionLags().total());
        assertTrue(response.partitionLags().truncated());
        assertEquals(List.of(0, 1), response.partitionLags().items().stream().map(p -> p.partitionId()).toList());
        assertEquals("NORMAL", response.partitionLags().items().get(0).commitState());
        assertEquals(6L, response.partitionLags().items().get(0).physicalLag());
        assertEquals(3L, response.partitionLags().items().get(0).visibleLag());
        assertEquals("OFFSET_AHEAD", response.partitionLags().items().get(1).commitState());
        assertEquals(0L, response.partitionLags().items().get(1).physicalLag());
        assertEquals(0L, response.partitionLags().items().get(1).visibleLag());
    }

    @Test
    void statusMapsGroupAndTopicFilterErrorsBeforeAssembly() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("payments", 1, 1)));
        GroupCoordinator coordinator = new GroupCoordinator(controller);
        coordinator.joinGroup(new JoinGroupRequest("g1", "member-a", "orders"));
        GroupStatusService service = new GroupStatusService(controller, () -> 123_456L);

        assertEquals(ErrorCode.GROUP_NOT_FOUND, service.status("missing", "", 10, 10).errorCode());
        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.status("g1", "missing", 10, 10).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, service.status("g1", "payments", 10, 10).errorCode());
    }

    @Test
    void statusReportsUncommittedLagFromKnownWatermark() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 1)));
        assertEquals(ErrorCode.NONE,
                controller.updateReplicaStateV2("orders", 0, 0, 1L, 0L, 0L, 10L, 7L, 9_000L));
        GroupCoordinator coordinator = new GroupCoordinator(controller);
        coordinator.joinGroup(new JoinGroupRequest("g1", "member-a", "orders"));

        GroupStatusService.Result result = new GroupStatusService(controller, () -> 123_456L)
                .status("g1", "", 10, 10);

        assertEquals(ErrorCode.NONE, result.errorCode());
        var lag = result.response().partitionLags().items().get(0);
        assertEquals(-1L, lag.committedOffset());
        assertEquals("UNCOMMITTED", lag.commitState());
        assertEquals(10L, lag.physicalHighWatermark());
        assertEquals(10L, lag.physicalLag());
        assertEquals(7L, lag.visibleHighWatermark());
        assertEquals(7L, lag.visibleLag());
        assertEquals(9_000L, lag.nextVisibleAtEpochMs());
        assertEquals("READY", lag.readiness());
    }

    @Test
    void lagMetricsSnapshotFoldsGroupsByMostSevereLagPerPartition() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 2, 1)));
        assertEquals(ErrorCode.NONE,
                controller.updateReplicaStateV2("orders", 0, 0, 1L, 0L, 0L, 10L, 7L, 9_000L));
        assertEquals(ErrorCode.NONE,
                controller.updateReplicaStateV2("orders", 1, 0, 1L, 0L, 0L, 10L, 7L, 9_000L));
        GroupCoordinator coordinator = new GroupCoordinator(controller);
        var ahead = coordinator.joinGroup(new JoinGroupRequest("g-ahead", "member-a", "orders"));
        var fast = coordinator.joinGroup(new JoinGroupRequest("g-fast", "member-b", "orders"));
        var slow = coordinator.joinGroup(new JoinGroupRequest("g-slow", "member-c", "orders"));

        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-fast", "orders", 0, 9L, fast.memberId(), fast.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-slow", "orders", 0, 2L, slow.memberId(), slow.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-ahead", "orders", 0, 4L, ahead.memberId(), ahead.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-fast", "orders", 1, 8L, fast.memberId(), fast.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-slow", "orders", 1, 6L, slow.memberId(), slow.generationId()));
        coordinator.offsetCommit(new OffsetCommitRequest(
                "g-ahead", "orders", 1, 12L, ahead.memberId(), ahead.generationId()));

        var lags = new GroupStatusService(controller, () -> 123_456L).lagMetricsSnapshot();

        assertEquals(2, lags.size());
        assertEquals(0, lags.get(0).partitionId());
        assertEquals(2L, lags.get(0).committedOffset());
        assertEquals("NORMAL", lags.get(0).commitState());
        assertEquals(8L, lags.get(0).physicalLag());
        assertEquals(5L, lags.get(0).visibleLag());
        assertEquals(1, lags.get(1).partitionId());
        assertEquals(12L, lags.get(1).committedOffset());
        assertEquals("OFFSET_AHEAD", lags.get(1).commitState());
        assertEquals(0L, lags.get(1).physicalLag());
        assertEquals(0L, lags.get(1).visibleLag());
    }

    @Test
    void statusBoundsMemberAssignmentsWithPartitionLimit() {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        controller.registerBroker(new RegisterBrokerRequest(0, "broker-0", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 4, 1)));
        GroupCoordinator coordinator = new GroupCoordinator(controller);
        coordinator.joinGroup(new JoinGroupRequest("g1", "member-a", "orders"));
        controller.updateMetadata(metadata -> {
            metadata.consumerGroups.get("g1").members.get("member-a").assignedPartitions =
                    List.of(2, 0, 1);
            return null;
        });

        GroupStatusService.Result result = new GroupStatusService(controller, () -> 123_456L)
                .status("g1", "", 10, 2);

        assertEquals(ErrorCode.NONE, result.errorCode());
        var assignments = result.response().members().items().get(0).assignments();
        assertEquals(3, assignments.total());
        assertTrue(assignments.truncated());
        assertEquals(List.of(0, 1),
                assignments.items().stream().map(a -> a.partitionId()).toList());
    }
}

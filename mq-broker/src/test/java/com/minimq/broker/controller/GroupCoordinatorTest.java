package com.minimq.broker.controller;

import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.assignor.AssignmentSerde;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupCoordinatorTest {

    @TempDir
    Path tempDir;

    private ClusterController clusterController;
    private GroupCoordinator groupCoordinator;

    @BeforeEach
    void setUp() {
        MetadataStore store = new MetadataStore(tempDir);
        clusterController = new ClusterController(store);
        groupCoordinator = new GroupCoordinator(clusterController);
        clusterController.registerBroker(new RegisterBrokerRequest(0, "localhost", 9092));
        clusterController.createTopic(new CreateTopicRequest("orders", 3));
    }

    @Test
    void joinSyncAssignsPartitionsAcrossMembers() {
        var join1 = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        var join2 = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        int generation = join2.generationId();

        byte[] leaderAssignment = groupCoordinator.computeLeaderAssignment(
                "g1", join2.members());

        groupCoordinator.syncGroup(new SyncGroupRequest(
                "g1", generation, join2.leaderMemberId(), leaderAssignment));

        var sync1 = groupCoordinator.syncGroup(new SyncGroupRequest(
                "g1", generation, join1.memberId(), new byte[0]));
        var sync2 = groupCoordinator.syncGroup(new SyncGroupRequest(
                "g1", generation, join2.memberId(), new byte[0]));

        List<Integer> p1 = AssignmentSerde.decodePartitions(sync1.assignment());
        List<Integer> p2 = AssignmentSerde.decodePartitions(sync2.assignment());

        assertEquals(3, p1.size() + p2.size());
        assertTrue(p1.contains(0) || p2.contains(0));
    }

    @Test
    void offsetCommitAndFetch() {
        assertEquals(ErrorCode.NONE, groupCoordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 1, 42L)));
        assertEquals(42L, groupCoordinator.offsetFetch(
                new OffsetFetchRequest("g1", "orders", 1)).offset());
    }

    @Test
    void heartbeatUpdatesMember() {
        var join = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(ErrorCode.NONE, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", join.memberId())));
    }

    @Test
    void returnsEmptyAssignmentForWrongGeneration() {
        var join = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        var sync = groupCoordinator.syncGroup(new SyncGroupRequest(
                "g1", join.generationId() + 1, join.memberId(), new byte[0]));

        assertTrue(AssignmentSerde.decodePartitions(sync.assignment()).isEmpty());
    }

    @Test
    void reportsMissingGroupAndOffset() {
        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.heartbeat(
                new HeartbeatRequest("missing", "member")));
        assertEquals(-1L, groupCoordinator.offsetFetch(
                new OffsetFetchRequest("missing", "orders", 0)).offset());
    }
}

package com.minimq.broker.controller;

import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.assignor.AssignmentSerde;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.HeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.JoinGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.LeaveGroupRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetCommitRequest;
import com.minimq.protocol.message.ProtocolMessages.OffsetFetchRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.SyncGroupRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
        var join = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(ErrorCode.NONE, groupCoordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 1, 42L, join.memberId(), join.generationId())));
        assertEquals(42L, groupCoordinator.offsetFetch(
                new OffsetFetchRequest("g1", "orders", 1)).offset());
    }

    @Test
    void heartbeatUpdatesMember() {
        var join = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(ErrorCode.NONE, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", join.memberId(), join.generationId())));
    }

    @Test
    void offsetCommitRequiresJoinedMemberAndMatchingGeneration() {
        var first = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        var second = groupCoordinator.joinGroup(new JoinGroupRequest("g1", first.memberId(), "orders"));

        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.offsetCommit(
                new OffsetCommitRequest("missing", "orders", 0, 1L, "member", 1)));
        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 0, 1L, "missing-member", second.generationId())));
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, groupCoordinator.offsetCommit(
                new OffsetCommitRequest(
                        "g1", "orders", 0, 1L, second.memberId(), first.generationId())));
        assertEquals(ErrorCode.INVALID_REQUEST, groupCoordinator.offsetCommit(
                new OffsetCommitRequest(
                        "g1", "payments", 0, 1L, second.memberId(), second.generationId())));
        assertEquals(ErrorCode.NONE, groupCoordinator.offsetCommit(
                new OffsetCommitRequest(
                        "g1", "orders", 0, 9L, second.memberId(), second.generationId())));
        assertEquals(9L, groupCoordinator.offsetFetch(
                new OffsetFetchRequest("g1", "orders", 0)).offset());
    }

    @Test
    void heartbeatRejectsUnknownMemberAndStaleGeneration() {
        var first = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        var second = groupCoordinator.joinGroup(new JoinGroupRequest("g1", first.memberId(), "orders"));

        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", "missing", second.generationId())));
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), first.generationId())));
        assertEquals(ErrorCode.NONE, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), second.generationId())));
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

    @Test
    void heartbeatEvictsTimedOutCaller() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        var join = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(GroupCoordinator.HEARTBEAT_TIMEOUT_MS + 1);
        assertEquals(ErrorCode.GROUP_NOT_FOUND, coordinator.heartbeat(
                new HeartbeatRequest("g1", join.memberId(), join.generationId())));
    }

    @Test
    void heartbeatFromLiveMemberEvictsStalePeerAndSignalsRebalance() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        clock.set(1_000);
        var second = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(1_000 + GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), second.generationId())));
    }

    @Test
    void heartbeatRefreshKeepsMemberAliveDuringRebalance() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        clock.set(1_000);
        var second = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(1_000 + GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), second.generationId())));

        clock.addAndGet(GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), second.generationId())));
    }

    @Test
    void offsetCommitOfTimedOutMemberIsRejected() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        var join = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(GroupCoordinator.HEARTBEAT_TIMEOUT_MS + 1);
        assertEquals(ErrorCode.GROUP_NOT_FOUND, coordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 0, 1L, join.memberId(), join.generationId())));
    }

    @Test
    void successfulOffsetCommitRefreshesHeartbeat() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        var join = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(5_000);
        assertEquals(ErrorCode.NONE, coordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 0, 1L, join.memberId(), join.generationId())));

        clock.set(5_000 + GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        assertEquals(ErrorCode.NONE, coordinator.heartbeat(
                new HeartbeatRequest("g1", join.memberId(), join.generationId())));
    }

    @Test
    void joinAfterTimeoutIncrementsGenerationOnceAndDropsStaleMember() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        var first = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(1, first.generationId());

        clock.set(GroupCoordinator.HEARTBEAT_TIMEOUT_MS + 1);
        var second = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        assertEquals(2, second.generationId());
        assertEquals(1, second.members().size());
        assertEquals(second.memberId(), second.leaderMemberId());
    }

    @Test
    void syncGroupAfterPeerTimeoutReturnsEmptyAssignment() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        clock.set(1_000);
        var second = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(1_000 + GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        var sync = coordinator.syncGroup(new SyncGroupRequest(
                "g1", second.generationId(), second.memberId(), new byte[0]));
        assertTrue(AssignmentSerde.decodePartitions(sync.assignment()).isEmpty());
    }

    @Test
    void rejoinAfterPeerEvictionElectsRemainingMemberLeader() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        clock.set(1_000);
        var second = coordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));

        clock.set(1_000 + GroupCoordinator.HEARTBEAT_TIMEOUT_MS);
        coordinator.heartbeat(new HeartbeatRequest("g1", second.memberId(), second.generationId()));

        var rejoin = coordinator.joinGroup(new JoinGroupRequest("g1", second.memberId(), "orders"));
        assertEquals(1, rejoin.members().size());
        assertEquals(second.memberId(), rejoin.leaderMemberId());
    }

    @Test
    void leaveRemovesMemberAndKeepsOffsetsWhenGroupEmpties() {
        var join = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(ErrorCode.NONE, groupCoordinator.offsetCommit(
                new OffsetCommitRequest("g1", "orders", 0, 7L, join.memberId(), join.generationId())));
        assertEquals(ErrorCode.NONE, groupCoordinator.leaveGroup(
                new LeaveGroupRequest("g1", join.memberId(), join.generationId())));
        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", join.memberId(), join.generationId())));
        assertEquals(7L, groupCoordinator.offsetFetch(
                new OffsetFetchRequest("g1", "orders", 0)).offset());
    }

    @Test
    void leaveDuringRebalanceStillRemovesMemberAndBumpsGeneration() {
        var first = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        var second = groupCoordinator.joinGroup(new JoinGroupRequest("g1", "", "orders"));
        assertEquals(ErrorCode.NONE, groupCoordinator.leaveGroup(
                new LeaveGroupRequest("g1", first.memberId(), first.generationId())));
        var remaining = groupCoordinator.heartbeat(
                new HeartbeatRequest("g1", second.memberId(), second.generationId()));
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, remaining);
        assertEquals(ErrorCode.GROUP_NOT_FOUND, groupCoordinator.leaveGroup(
                new LeaveGroupRequest("g1", first.memberId(), first.generationId())));
    }

    @Test
    void perMemberSessionTimeoutEvictsOnlyTheExpiredMember() {
        AtomicLong clock = new AtomicLong(0);
        GroupCoordinator coordinator = new GroupCoordinator(clusterController, clock::get);
        var shortLived = coordinator.joinGroup(
                new JoinGroupRequest("g1", "", "orders", 1_000));
        clock.set(100);
        var longLived = coordinator.joinGroup(
                new JoinGroupRequest("g1", "", "orders", 30_000));

        clock.set(1_200);
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat(
                new HeartbeatRequest("g1", longLived.memberId(), longLived.generationId())));
        assertEquals(ErrorCode.GROUP_NOT_FOUND, coordinator.heartbeat(
                new HeartbeatRequest("g1", shortLived.memberId(), shortLived.generationId())));
    }
}

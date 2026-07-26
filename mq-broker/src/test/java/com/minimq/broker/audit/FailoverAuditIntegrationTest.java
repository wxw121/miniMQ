package com.minimq.broker.audit;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FailoverAuditIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void controllerTransitionsAndRejectedEpochsAreAudited() {
        AtomicLong now = new AtomicLong();
        FailoverAuditLog audit = new FailoverAuditLog(0, () -> now.get() / 1_000_000L);
        ClusterController controller = new ClusterController(
                new MetadataStore(tempDir),
                5_000L,
                now::get,
                audit);

        controller.registerBroker(new RegisterBrokerRequest(0, "localhost", 9090));
        controller.registerBroker(new RegisterBrokerRequest(1, "localhost", 9091));
        controller.registerBroker(new RegisterBrokerRequest(2, "localhost", 9092));
        assertEquals(ErrorCode.NONE, controller.createTopic(new CreateTopicRequest("orders", 1, 3)));

        assertEquals(ErrorCode.NONE, controller.updateReplicaStateV2(
                "orders", 0, 0, 1L, 0L, 0L, 20L, 20L, -1L));
        assertEquals(ErrorCode.NONE, controller.updateReplicaStateV2(
                "orders", 0, 1, 1L, 0L, 0L, 20L, 20L, -1L));
        assertEquals(ErrorCode.STALE_LEADER_EPOCH, controller.updateReplicaStateV2(
                "orders", 0, 0, 1L, 0L, 1L, 20L, 20L, -1L));

        assertEquals(ErrorCode.NONE, controller.markBrokerOffline(0));
        now.set(TimeUnit.MILLISECONDS.toNanos(5_001));
        assertTrue(controller.expireBrokerLease(1, 1L));

        List<FailoverAuditEventType> types = audit.snapshot().stream()
                .map(FailoverAuditLog.AuditEvent::eventType)
                .toList();
        assertTrue(types.contains(FailoverAuditEventType.ISR_SHRUNK));
        assertTrue(types.contains(FailoverAuditEventType.ISR_EXPANDED));
        assertTrue(types.contains(FailoverAuditEventType.STALE_EPOCH_REJECTED));
        assertTrue(types.contains(FailoverAuditEventType.LEADER_CHANGED));
        assertTrue(types.contains(FailoverAuditEventType.BROKER_LEASE_EXPIRED));
    }
}

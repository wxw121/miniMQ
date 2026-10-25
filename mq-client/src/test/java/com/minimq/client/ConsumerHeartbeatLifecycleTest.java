package com.minimq.client;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusRequest;
import com.minimq.protocol.message.ProtocolMessages.GroupStatusResponse;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerHeartbeatLifecycleTest {

    @Test
    void heartbeatContinuesAfterTransientRuntimeFailure() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-heartbeat-runtime-failure");
        BrokerConfig config = BrokerTestHarness.controllerConfig(0, dataDir);
        BrokerTestHarness broker = BrokerTestHarness.start(config);
        try (NetworkClient admin = new NetworkClient("heartbeat-admin");
             Consumer consumer = new Consumer(
                     broker.bootstrapServers(), "heartbeat-group", "heartbeat-consumer")) {
            admin.connect("127.0.0.1", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            consumer.subscribe("orders");

            broker.close();
            broker = null;
            Thread.sleep(3_500L);

            broker = BrokerTestHarness.start(config);
            assertTrue(
                    awaitHeartbeat(broker, 5, TimeUnit.SECONDS),
                    "heartbeat scheduling stopped after the transient runtime failure");
        } finally {
            if (broker != null) {
                broker.close();
            }
        }
    }

    @Test
    void closeStopsHeartbeatsAfterRepeatedSubscribe() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-repeated-subscribe-heartbeat");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("repeated-subscribe-admin")) {
            admin.connect("127.0.0.1", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            Consumer consumer = new Consumer(
                    broker.bootstrapServers(), "repeated-group", "repeated-consumer");
            consumer.subscribe("orders");
            consumer.subscribe("orders");
            consumer.close();

            long heartbeatsAtClose = heartbeatCount(broker);
            assertEquals(
                    heartbeatsAtClose,
                    heartbeatCountAfter(broker, 4, TimeUnit.SECONDS),
                    "a heartbeat executor created by the first subscribe survived close");
        }
    }

    @Test
    void closeSendsLeaveGroupAndDropsMember() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("consumer-leave-on-close");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             NetworkClient admin = new NetworkClient("leave-on-close-admin")) {
            admin.connect("127.0.0.1", broker.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, "orders", 1, 1)));
            Consumer consumer = new Consumer(
                    broker.bootstrapServers(), "leave-group", "leave-consumer");
            consumer.subscribe("orders");
            GroupStatusResponse before = groupStatus(admin, "leave-group");
            assertEquals(1, before.members().total());
            consumer.close();

            GroupStatusResponse after = groupStatus(admin, "leave-group");
            assertEquals(0, after.members().total());
        }
    }

    private static GroupStatusResponse groupStatus(NetworkClient admin, String groupId)
            throws InterruptedException {
        var response = admin.send(
                ApiKeys.GROUP_STATUS,
                new GroupStatusRequest(BrokerTestHarness.ADMIN_TOKEN, groupId, null, 10, 10));
        admin.ensureSuccess(response);
        return (GroupStatusResponse) response.body();
    }

    private static boolean awaitHeartbeat(
            BrokerTestHarness broker, long timeout, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            if (broker.runtimeMetrics().requestsByApiKey()
                    .getOrDefault(ApiKeys.HEARTBEAT, 0L) > 0L) {
                return true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(25L));
        }
        return false;
    }

    private static long heartbeatCountAfter(
            BrokerTestHarness broker, long timeout, TimeUnit unit) {
        long initialCount = heartbeatCount(broker);
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            long currentCount = heartbeatCount(broker);
            if (currentCount > initialCount) {
                return currentCount;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(25L));
        }
        return heartbeatCount(broker);
    }

    private static long heartbeatCount(BrokerTestHarness broker) {
        return broker.runtimeMetrics().requestsByApiKey()
                .getOrDefault(ApiKeys.HEARTBEAT, 0L);
    }
}

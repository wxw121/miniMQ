package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerConnectionCleanupTest {

    @Test
    void failedCandidateCleanupSkipsTheGracefulShutdownQuietPeriod() {
        NetworkClient candidate = new NetworkClient("failed-candidate-cleanup");
        RuntimeException connectFailure = new IllegalStateException("broker unavailable");

        long startedNanos = System.nanoTime();
        boolean interrupted = NetworkClientCleanup.suppressCloseFailure(candidate, connectFailure);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

        assertFalse(interrupted);
        assertTrue(elapsedMillis < 1_000,
                "failed candidate cleanup must not wait for Netty's graceful shutdown quiet period");
    }

    @Test
    void cleanupPreservesInterruptedCloseFailureForThePrimaryError() throws Exception {
        RuntimeException primary = new IllegalStateException("connect failed");
        InterruptedException closeFailure = new InterruptedException("cleanup interrupted");
        try (NetworkClient candidate = new NetworkClient("cleanup-interrupted") {
            @Override
            void abortCandidate() throws InterruptedException {
                throw closeFailure;
            }
        }) {
            assertTrue(NetworkClientCleanup.suppressCloseFailure(candidate, primary));
        }

        assertSame(closeFailure, primary.getSuppressed()[0]);
    }

    @Test
    void cleanupPreservesRuntimeCloseFailureForThePrimaryError() throws Exception {
        RuntimeException primary = new IllegalStateException("connect failed");
        RuntimeException closeFailure = new IllegalStateException("cleanup failed");
        try (NetworkClient candidate = new NetworkClient("cleanup-runtime") {
            @Override
            void abortCandidate() {
                throw closeFailure;
            }
        }) {
            assertFalse(NetworkClientCleanup.suppressCloseFailure(candidate, primary));
        }

        assertSame(closeFailure, primary.getSuppressed()[0]);
    }

    @Test
    void producerClosesBrokerCandidateWhenConnectionFails() throws Exception {
        RuntimeException connectFailure = new IllegalStateException("producer broker unavailable");
        AtomicBoolean closed = new AtomicBoolean();
        try (BrokerTestHarness controller = BrokerTestHarness.startController(
                     BrokerTestHarness.newDataDir("producer-connect-cleanup"));
             NetworkClient admin = createTopic(controller, "producer-cleanup");
             Producer producer = new Producer(
                     controller.bootstrapServers(),
                     "producer-connect-cleanup",
                     clientId -> new FailingConnectClient(clientId, connectFailure, closed))) {
            producer.start();

            RuntimeException thrown = assertThrows(RuntimeException.class, () -> producer.send(
                    new ProducerRecord("producer-cleanup", null, new byte[] {1}, new byte[] {2}))
                    .offset());

            assertSame(connectFailure, thrown);
            assertTrue(closed.get());
        }
    }

    @Test
    void consumerClosesBrokerCandidateWhenConnectionFails() throws Exception {
        RuntimeException connectFailure = new IllegalStateException("consumer broker unavailable");
        AtomicBoolean closed = new AtomicBoolean();
        try (BrokerTestHarness controller = BrokerTestHarness.startController(
                     BrokerTestHarness.newDataDir("consumer-connect-cleanup"));
             NetworkClient admin = createTopic(controller, "consumer-cleanup");
             Consumer consumer = new Consumer(
                     controller.bootstrapServers(),
                     "consumer-cleanup-group",
                     "consumer-connect-cleanup",
                     clientId -> new FailingConnectClient(clientId, connectFailure, closed))) {
            consumer.subscribe("consumer-cleanup");

            RuntimeException thrown = assertThrows(
                    RuntimeException.class,
                    () -> consumer.poll(Duration.ofMillis(50)));

            assertSame(connectFailure, thrown);
            assertTrue(closed.get());
        }
    }

    private static NetworkClient createTopic(BrokerTestHarness controller, String topic)
            throws Exception {
        NetworkClient admin = new NetworkClient("connection-cleanup-admin");
        try {
            admin.connect("127.0.0.1", controller.port());
            admin.ensureSuccess(admin.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            BrokerTestHarness.ADMIN_TOKEN, topic, 1, 1)));
            return admin;
        } catch (Throwable failure) {
            try {
                admin.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static final class FailingConnectClient extends NetworkClient {
        private final RuntimeException connectFailure;
        private final AtomicBoolean closed;

        private FailingConnectClient(
                String clientId, RuntimeException connectFailure, AtomicBoolean closed) {
            super(clientId);
            this.connectFailure = connectFailure;
            this.closed = closed;
        }

        @Override
        public void connect(String host, int port) {
            throw connectFailure;
        }

        @Override
        public void connect(String host, int port, Duration timeout) {
            connect(host, port);
        }

        @Override
        void abortCandidate() throws InterruptedException {
            closed.set(true);
            super.abortCandidate();
        }

        @Override
        void abortCandidate(Duration timeout) throws InterruptedException {
            closed.set(true);
            super.abortCandidate(timeout);
        }
    }
}

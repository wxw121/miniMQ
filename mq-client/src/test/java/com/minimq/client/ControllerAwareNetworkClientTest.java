package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.exception.MiniMqRequestTimeoutException;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.ControllerMetadata;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControllerAwareNetworkClientTest {
    @Test
    void parsesEveryBootstrapEndpoint() {
        BootstrapServers servers = BootstrapServers.parse("host-a:19090, host-b:19091");
        assertEquals(2, servers.endpoints().size());
        assertEquals("host-b", servers.endpoints().get(1).host());
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host-only"));
    }

    @Test
    void rejectsInvalidBootstrapFormsAndExposesInitialEpoch() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapServers(List.of()));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(null));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(" "));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse(":9092"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:0"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:65536"));
        assertThrows(IllegalArgumentException.class, () -> BootstrapServers.parse("host:nope"));
        try (ControllerAwareNetworkClient client =
                     new ControllerAwareNetworkClient("localhost:9092", "validation")) {
            assertEquals(-1, client.controllerEpoch());
            assertThrows(com.minimq.client.exception.MiniMqServerException.class,
                    () -> client.ensureSuccess(new MqDecoder.DecodedResponse(
                            new MqResponseHeader(1, ErrorCode.NOT_CONTROLLER), null), "test"));
        }
    }

    @Test
    void rejectsInvalidRequestTimeoutsBeforeConnecting() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerAwareNetworkClient(
                        "localhost:9092", "zero", Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerAwareNetworkClient(
                        "localhost:9092", "negative", Duration.ofMillis(-1)));
        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "localhost:9092", "per-call", Duration.ofSeconds(1))) {
            assertThrows(IllegalArgumentException.class,
                    () -> client.send(ApiKeys.METADATA, new MetadataRequest("orders"),
                            Duration.ofMillis(-1)));
        }
    }

    @Test
    void skipsUnavailableBootstrapAndDiscoversControllerThroughFollower() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("client-discovery-controller");
        Path followerDir = BrokerTestHarness.newDataDir("client-discovery-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(controllerDir);
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", controller.port());
             ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                     "127.0.0.1:" + unusedPort() + "," + follower.bootstrapServers(),
                     "controller-aware-test")) {
            client.connect();
            var response = client.send(ApiKeys.METADATA, new MetadataRequest(""));
            assertEquals(ErrorCode.NONE, response.header().errorCode());
        }
    }

    @Test
    void reportsFailureAfterEveryBootstrapIsUnavailable() throws Exception {
        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:" + unusedPort(), "unavailable-bootstrap")) {
            assertThrows(com.minimq.client.exception.MiniMqConnectionException.class,
                    client::connect);
        }
    }

    @Test
    void closesDiscoveredControllerClientWhenItsConnectionFails() throws Exception {
        TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                new DiscoverControllerResponse(
                        new ControllerMetadata(1, "127.0.0.1", 19092, 3L), 4L));
        TrackingNetworkClient failedController = TrackingNetworkClient.failingConnection();
        AtomicInteger created = new AtomicInteger();

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "failed-controller-cleanup",
                Duration.ofMillis(100),
                (clientId, retryPolicy, requestTimeout) ->
                        created.getAndIncrement() == 0 ? bootstrap : failedController)) {
            long startedNanos = System.nanoTime();
            assertThrows(com.minimq.client.exception.MiniMqConnectionException.class,
                    client::connect);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startedNanos);
            assertTrue(failedController.closed,
                    "a failed Controller candidate must release its Netty resources");
            assertTrue(elapsedMillis < 1_000,
                    "failed discovery must not wait for graceful shutdown quiet periods");
        } finally {
            bootstrap.close();
            failedController.close();
        }
    }

    @Test
    void preservesConnectionFailureWhenCleanupThrowsUncheckedFailure() throws Exception {
        for (Throwable closeFailure : List.of(
                new IllegalStateException("synthetic close failure"),
                new AssertionError("synthetic close error"))) {
            TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                    new DiscoverControllerResponse(
                            new ControllerMetadata(1, "127.0.0.1", 19092, 3L), 4L));
            com.minimq.client.exception.MiniMqConnectionException connectFailure =
                    new com.minimq.client.exception.MiniMqConnectionException(
                            "synthetic connection failure", null);
            TrackingNetworkClient failedController =
                    TrackingNetworkClient.failingConnection(connectFailure, closeFailure);
            AtomicInteger created = new AtomicInteger();

            try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                    "127.0.0.1:19091",
                    "failed-controller-suppression",
                    Duration.ofMillis(100),
                    (clientId, retryPolicy, requestTimeout) ->
                            created.getAndIncrement() == 0 ? bootstrap : failedController)) {
                com.minimq.client.exception.MiniMqConnectionException thrown = assertThrows(
                        com.minimq.client.exception.MiniMqConnectionException.class,
                        client::connect);

                assertSame(connectFailure, thrown.getCause());
                assertEquals(List.of(closeFailure), List.of(connectFailure.getSuppressed()));
                assertFalse(Thread.currentThread().isInterrupted());
            } finally {
                bootstrap.close();
                failedController.close();
            }
        }
    }

    @Test
    void cleanupInterruptionIsSuppressedAndRestoresInterruptFlag() throws Exception {
        assertFalse(Thread.interrupted());
        TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                new DiscoverControllerResponse(
                        new ControllerMetadata(1, "127.0.0.1", 19092, 3L), 4L));
        com.minimq.client.exception.MiniMqConnectionException connectFailure =
                new com.minimq.client.exception.MiniMqConnectionException(
                        "synthetic connection failure", null);
        InterruptedException closeFailure =
                new InterruptedException("synthetic close interruption");
        TrackingNetworkClient failedController =
                TrackingNetworkClient.failingConnection(connectFailure, closeFailure);
        AtomicInteger created = new AtomicInteger();

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "failed-controller-interruption",
                Duration.ofMillis(100),
                (clientId, retryPolicy, requestTimeout) ->
                        created.getAndIncrement() == 0 ? bootstrap : failedController)) {
            com.minimq.client.exception.MiniMqConnectionException thrown = assertThrows(
                    com.minimq.client.exception.MiniMqConnectionException.class,
                    client::connect);

            assertSame(connectFailure, thrown.getCause());
            assertEquals(List.of(closeFailure), List.of(connectFailure.getSuppressed()));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            bootstrap.close();
            failedController.close();
        }
    }

    @Test
    void preWriteConnectionFailureRediscoversAndRetriesOnce() throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        TrackingNetworkClient deadController = TrackingNetworkClient.failingSend(
                new MiniMqConnectionException("client is not connected", null));
        TrackingNetworkClient secondBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(2L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(7, ErrorCode.NONE), null);
        TrackingNetworkClient replacementController = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(
                firstBootstrap, deadController, secondBootstrap, replacementController));

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-pre-write-retry",
                Duration.ofMillis(100),
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertSame(success, client.send(ApiKeys.METADATA, new MetadataRequest("orders")));
            assertEquals(1, deadController.sendCalls);
            assertTrue(deadController.closed);
            assertEquals(1, replacementController.sendCalls);
            assertEquals(2L, client.controllerEpoch());
            assertTrue(clients.isEmpty());
        }
    }

    @Test
    void requestTimeoutInvalidatesControllerWithoutReplayingUnknownRequest() throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        MiniMqRequestTimeoutException timeout =
                new MiniMqRequestTimeoutException("METADATA", Duration.ofMillis(100));
        TrackingNetworkClient timedOutController = TrackingNetworkClient.failingSend(timeout);
        TrackingNetworkClient secondBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(2L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(8, ErrorCode.NONE), null);
        TrackingNetworkClient replacementController = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(
                firstBootstrap, timedOutController, secondBootstrap, replacementController));

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-timeout-invalidation",
                Duration.ofMillis(100),
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertSame(timeout, assertThrows(
                    MiniMqRequestTimeoutException.class,
                    () -> client.send(ApiKeys.METADATA, new MetadataRequest("orders"))));
            assertEquals(1, timedOutController.sendCalls);
            assertTrue(timedOutController.closed);
            assertEquals(2, clients.size(), "the timed-out request must not be replayed");

            assertSame(success, client.send(ApiKeys.METADATA, new MetadataRequest("orders")));
            assertEquals(1, replacementController.sendCalls);
            assertTrue(clients.isEmpty());
        }
    }

    @Test
    void zeroPerCallTimeoutFailsBeforeSendingToAnAlreadyDiscoveredController() throws Exception {
        TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(discovery(1L));
        TrackingNetworkClient controller = TrackingNetworkClient.respondingWith(
                new MqDecoder.DecodedResponse(new MqResponseHeader(1, ErrorCode.NONE), null));
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(bootstrap, controller));

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-zero-timeout",
                Duration.ofSeconds(1),
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertThrows(MiniMqRequestTimeoutException.class,
                    () -> client.send(ApiKeys.METADATA, new MetadataRequest("orders"),
                            Duration.ZERO));
            assertEquals(0, controller.sendCalls);
        }
    }

    @Test
    void rejectsAControllerDiscoveryThatMovesToAnOlderEpoch() throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(discovery(2L));
        TrackingNetworkClient controller = TrackingNetworkClient.respondingWith(
                new MqDecoder.DecodedResponse(
                        new MqResponseHeader(1, ErrorCode.NOT_CONTROLLER), null));
        TrackingNetworkClient lowerEpochBootstrap =
                TrackingNetworkClient.bootstrapRespondingWith(discovery(1L));
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(
                List.of(firstBootstrap, controller, lowerEpochBootstrap));

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-lower-epoch",
                Duration.ofSeconds(1),
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertThrows(MiniMqConnectionException.class,
                    () -> client.send(ApiKeys.METADATA, new MetadataRequest("orders")));
            assertEquals(2L, client.controllerEpoch());
        }
    }

    @Test
    void perCallTimeoutIsPropagatedWithoutReplayingUnknownRequest() throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        MiniMqRequestTimeoutException timeout =
                new MiniMqRequestTimeoutException("METADATA", Duration.ofMillis(25));
        TrackingNetworkClient timedOutController = TrackingNetworkClient.failingSend(timeout);
        TrackingNetworkClient unusedBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(2L));
        TrackingNetworkClient unusedController = TrackingNetworkClient.respondingWith(
                new MqDecoder.DecodedResponse(new MqResponseHeader(9, ErrorCode.NONE), null));
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(
                firstBootstrap, timedOutController, unusedBootstrap, unusedController));

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-per-call-timeout",
                Duration.ofSeconds(1),
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertSame(timeout, assertThrows(
                    MiniMqRequestTimeoutException.class,
                    () -> client.send(
                            ApiKeys.METADATA,
                            new MetadataRequest("orders"),
                            Duration.ofMillis(25))));

            assertTrue(timedOutController.lastSendTimeout.compareTo(Duration.ofMillis(25)) <= 0);
            assertEquals(1, timedOutController.sendCalls);
            assertTrue(timedOutController.closed);
            assertTrue(timedOutController.lastAbortTimeout != null,
                    "Controller invalidation cleanup must use the remaining call budget");
            assertEquals(2, clients.size(), "the timed-out request must not be replayed");
        }
    }

    @Test
    void preWriteRecoveryUsesTheOriginalPerCallDeadline() throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        TrackingNetworkClient deadController = TrackingNetworkClient.failingSend(
                new MiniMqConnectionException("client is not connected", null));
        TrackingNetworkClient secondBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(2L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(10, ErrorCode.NONE), null);
        TrackingNetworkClient replacementController = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(
                firstBootstrap, deadController, secondBootstrap, replacementController));
        Duration callTimeout = Duration.ofSeconds(5);

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-shared-deadline",
                callTimeout,
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertSame(success, client.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"), callTimeout));

            assertTrue(deadController.lastSendTimeout.compareTo(callTimeout) <= 0);
            assertTrue(replacementController.lastSendTimeout.compareTo(
                            deadController.lastSendTimeout) < 0,
                    "recovery must pass only the time remaining on the original deadline");
            assertEquals(1, deadController.sendCalls);
            assertEquals(1, replacementController.sendCalls);
        }
    }

    @Test
    void preWriteRecoveryBoundsEveryRediscoveryOperationByTheOriginalDeadline()
            throws Exception {
        TrackingNetworkClient firstBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        TrackingNetworkClient deadController = TrackingNetworkClient.failingSend(
                new MiniMqConnectionException("client is not connected", null));
        TrackingNetworkClient recoveryBootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(2L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(11, ErrorCode.NONE), null);
        TrackingNetworkClient replacementController = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(
                firstBootstrap, deadController, recoveryBootstrap, replacementController));
        Duration callTimeout = Duration.ofSeconds(5);

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-bounded-rediscovery",
                callTimeout,
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();

            assertSame(success, client.send(
                    ApiKeys.METADATA, new MetadataRequest("orders"), callTimeout));

            assertTrue(recoveryBootstrap.lastConnectTimeout != null,
                    "bootstrap connect must use the remaining call budget");
            assertTrue(recoveryBootstrap.lastSendTimeout != null,
                    "controller discovery must use the remaining call budget");
            assertTrue(replacementController.lastConnectTimeout != null,
                    "Controller connect must use the remaining call budget");
            assertTrue(replacementController.lastSendTimeout != null,
                    "the recovered request must use the remaining call budget");
            assertTrue(recoveryBootstrap.lastSendTimeout.compareTo(
                    recoveryBootstrap.lastConnectTimeout) < 0);
            assertTrue(replacementController.lastConnectTimeout.compareTo(
                    recoveryBootstrap.lastSendTimeout) < 0);
            assertTrue(replacementController.lastSendTimeout.compareTo(
                    replacementController.lastConnectTimeout) < 0);
        }
    }

    @Test
    void absoluteDeadlineIsPassedDirectlyToTheControllerRequest() throws Exception {
        TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(12, ErrorCode.NONE), null);
        TrackingNetworkClient controller = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(bootstrap, controller));
        Duration callTimeout = Duration.ofSeconds(5);

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-absolute-deadline",
                callTimeout,
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            client.connect();
            long deadlineNanos = System.nanoTime() + callTimeout.toNanos();

            assertSame(success, client.sendUntil(
                    ApiKeys.METADATA, new MetadataRequest("orders"), deadlineNanos));

            assertTrue(controller.lastSendTimeout.compareTo(callTimeout) < 0,
                    "the Controller request must receive only time remaining to the caller deadline");
        }
    }

    @Test
    void defaultSendUsesOneConfiguredDeadlineForRediscovery() throws Exception {
        TrackingNetworkClient bootstrap = TrackingNetworkClient.bootstrapRespondingWith(
                discovery(1L));
        MqDecoder.DecodedResponse success = new MqDecoder.DecodedResponse(
                new MqResponseHeader(13, ErrorCode.NONE), null);
        TrackingNetworkClient controller = TrackingNetworkClient.respondingWith(success);
        Queue<TrackingNetworkClient> clients = new ArrayDeque<>(List.of(bootstrap, controller));
        Duration configuredTimeout = Duration.ofMillis(100);

        try (ControllerAwareNetworkClient client = new ControllerAwareNetworkClient(
                "127.0.0.1:19091",
                "controller-default-deadline",
                configuredTimeout,
                (clientId, retryPolicy, requestTimeout) -> clients.remove())) {
            assertSame(success, client.send(
                    ApiKeys.METADATA, new MetadataRequest("orders")));

            assertTrue(bootstrap.lastConnectTimeout != null,
                    "default rediscovery connect must use the configured deadline");
            assertTrue(bootstrap.lastSendTimeout != null,
                    "default discovery request must use the configured deadline");
            assertTrue(controller.lastConnectTimeout != null,
                    "default Controller connect must use the configured deadline");
            assertTrue(controller.lastSendTimeout != null,
                    "default business request must use the configured deadline");
        }
    }

    private static DiscoverControllerResponse discovery(long epoch) {
        return new DiscoverControllerResponse(
                new ControllerMetadata(1, "127.0.0.1", 19092, epoch), epoch + 1L);
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class TrackingNetworkClient extends NetworkClient {
        private final Object sendOutcome;
        private final RuntimeException connectFailure;
        private Throwable closeFailure;
        private boolean closed;
        private int sendCalls;
        private Duration lastConnectTimeout;
        private Duration lastSendTimeout;
        private Duration lastAbortTimeout;

        private TrackingNetworkClient(
                Object sendOutcome,
                RuntimeException connectFailure,
                Throwable closeFailure) {
            super("tracking-client");
            this.sendOutcome = sendOutcome;
            this.connectFailure = connectFailure;
            this.closeFailure = closeFailure;
        }

        private static TrackingNetworkClient bootstrapRespondingWith(
                DiscoverControllerResponse response) {
            return new TrackingNetworkClient(
                    new MqDecoder.DecodedResponse(
                            new MqResponseHeader(1, ErrorCode.NONE), response),
                    null,
                    null);
        }

        private static TrackingNetworkClient failingConnection() {
            return failingConnection(
                    new com.minimq.client.exception.MiniMqConnectionException(
                            "synthetic connection failure", null),
                    null);
        }

        private static TrackingNetworkClient failingConnection(
                RuntimeException connectFailure, Throwable closeFailure) {
            return new TrackingNetworkClient(null, connectFailure, closeFailure);
        }

        private static TrackingNetworkClient respondingWith(MqDecoder.DecodedResponse response) {
            return new TrackingNetworkClient(response, null, null);
        }

        private static TrackingNetworkClient failingSend(RuntimeException failure) {
            return new TrackingNetworkClient(failure, null, null);
        }

        @Override
        public void connect(String host, int port) {
            if (connectFailure != null) {
                throw connectFailure;
            }
        }

        @Override
        public void connect(String host, int port, Duration timeout) {
            lastConnectTimeout = timeout;
            connect(host, port);
        }

        @Override
        public MqDecoder.DecodedResponse send(short apiKey, Object body) {
            sendCalls++;
            if (sendOutcome instanceof RuntimeException failure) {
                throw failure;
            }
            return (MqDecoder.DecodedResponse) sendOutcome;
        }

        @Override
        public MqDecoder.DecodedResponse send(short apiKey, Object body, Duration timeout) {
            lastSendTimeout = timeout;
            return send(apiKey, body);
        }

        @Override
        public void close() throws InterruptedException {
            closed = true;
            super.close();
            throwCleanupFailure();
        }

        @Override
        void abortCandidate() throws InterruptedException {
            closed = true;
            super.abortCandidate();
            throwCleanupFailure();
        }

        void abortCandidate(Duration timeout) throws InterruptedException {
            lastAbortTimeout = timeout;
            abortCandidate();
        }

        private void throwCleanupFailure() throws InterruptedException {
            Throwable failure = closeFailure;
            closeFailure = null;
            if (failure instanceof InterruptedException interrupted) {
                throw interrupted;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }
}

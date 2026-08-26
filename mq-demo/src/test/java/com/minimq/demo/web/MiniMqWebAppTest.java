package com.minimq.demo.web;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.Consumer;
import com.minimq.client.ControllerAwareNetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.model.ProduceResult;
import com.minimq.client.model.ProducerRecord;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniMqWebAppTest {
    private static final String TEST_ADMIN_TOKEN = "test-admin";
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void installAdminToken() {
        System.setProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY, TEST_ADMIN_TOKEN);
    }

    @AfterAll
    static void clearAdminToken() {
        System.clearProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY);
    }

    @Test
    void rejectsBlankAdminCredential() {
        assertThrows(IllegalArgumentException.class,
                () -> MiniMqWebApp.start(0, "127.0.0.1:65535", " "));
    }

    @Test
    void twoArgStartRequiresAdminTokenProperty() {
        String previous = System.getProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY);
        System.clearProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY);
        try {
            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class,
                    () -> MiniMqWebApp.start(0, "127.0.0.1:65535"));
            assertTrue(failure.getMessage().contains(MiniMqWebApp.ADMIN_TOKEN_PROPERTY));
        } finally {
            if (previous == null) {
                System.clearProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY);
            } else {
                System.setProperty(MiniMqWebApp.ADMIN_TOKEN_PROPERTY, previous);
            }
        }
    }

    @Test
    void topicDeleteRequestTimeoutConfigurationIsBounded() {
        assertEquals(
                Duration.ofSeconds(45),
                MiniMqWebApp.topicDeleteRequestTimeout("45000"));

        assertThrows(IllegalArgumentException.class,
                () -> MiniMqWebApp.topicDeleteRequestTimeout("0"));
        assertThrows(IllegalArgumentException.class,
                () -> MiniMqWebApp.topicDeleteRequestTimeout("600001"));
        assertThrows(IllegalArgumentException.class,
                () -> MiniMqWebApp.topicDeleteRequestTimeout("not-a-number"));
    }

    @Test
    void rejectsBrowserSuppliedTargetsOutsideTheServerBootstrapAllowlist() throws Exception {
        try (ConnectionProbe brokerProbe = new ConnectionProbe();
             ConnectionProbe bootstrapProbe = new ConnectionProbe();
             MiniMqWebApp app = MiniMqWebApp.start(
                     0, "127.0.0.1:65535", BrokerTestHarness.ADMIN_TOKEN)) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> brokerResponse = postJson(baseUrl + "/api/ops-status", """
                    {"broker":"127.0.0.1:%d"}
                    """.formatted(brokerProbe.port()));
            HttpResponse<String> bootstrapResponse = postJson(baseUrl + "/api/topic/create", """
                    {
                      "bootstrapServers":"127.0.0.1:%d","topic":"exfiltration-test",
                      "partitions":1,"replicationFactor":1
                    }
                    """.formatted(bootstrapProbe.port()));
            HttpResponse<String> extraBrokerResponse = postJson(baseUrl + "/api/topic/create", """
                    {
                      "bootstrapServers":"127.0.0.1:65535","broker":"127.0.0.1:%d",
                      "topic":"ignored-exfiltration-test","partitions":1,"replicationFactor":1
                    }
                    """.formatted(brokerProbe.port()));
            HttpResponse<String> extraBootstrapResponse = postJson(baseUrl + "/api/ops-status", """
                    {"broker":"127.0.0.1:65535","bootstrapServers":"127.0.0.1:%d"}
                    """.formatted(bootstrapProbe.port()));

            assertEquals(400, brokerResponse.statusCode(), brokerResponse.body());
            assertEquals(400, bootstrapResponse.statusCode(), bootstrapResponse.body());
            assertEquals(400, extraBrokerResponse.statusCode(), extraBrokerResponse.body());
            assertEquals(400, extraBootstrapResponse.statusCode(), extraBootstrapResponse.body());
            assertFalse(brokerProbe.connectedWithin(Duration.ofMillis(300)));
            assertFalse(bootstrapProbe.connectedWithin(Duration.ofMillis(300)));
            assertFalse(brokerResponse.body().contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(bootstrapResponse.body().contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(extraBrokerResponse.body().contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(extraBootstrapResponse.body().contains(BrokerTestHarness.ADMIN_TOKEN));
        }
    }

    @Test
    void enforcesJsonAndSameOriginWithoutCorsWildcard() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();
            for (String path : List.of(
                    "/api/run-flow",
                    "/api/topic/create",
                    "/api/topic/delete",
                    "/api/topic/increase",
                    "/api/topic/alter-config",
                    "/api/topic/reassign",
                    "/api/topic/elect-preferred",
                    "/api/topic/reliability",
                    "/api/broker/offline",
                    "/api/retry-produce",
                    "/api/dead-letter-produce",
                    "/api/delayed-produce")) {
                HttpResponse<String> response = post(
                        baseUrl + path, "{}", "text/plain", null, Duration.ofSeconds(3));
                assertEquals(415, response.statusCode(), path + ": " + response.body());
            }

            HttpResponse<String> crossOrigin = post(
                    baseUrl + "/api/topic/delete",
                    "{\"topic\":\"orders\"}",
                    "application/json",
                    "https://attacker.example",
                    Duration.ofSeconds(3));
            HttpResponse<String> sameOrigin = post(
                    baseUrl + "/api/run-flow",
                    "{\"mode\":\"mock\",\"message\":\"same-origin\"}",
                    "application/json; charset=utf-8",
                    baseUrl,
                    Duration.ofSeconds(3));

            assertEquals(403, crossOrigin.statusCode(), crossOrigin.body());
            assertEquals(200, sameOrigin.statusCode(), sameOrigin.body());
            assertTrue(sameOrigin.body().contains("\"mode\":\"mock\""));
            assertTrue(sameOrigin.body().contains("\"message\":\"Mock main flow completed\""));
            assertFalse(crossOrigin.headers().firstValue("Access-Control-Allow-Origin").isPresent());
            assertFalse(sameOrigin.headers().firstValue("Access-Control-Allow-Origin").isPresent());

            HttpResponse<String> missingOrigin = post(
                    baseUrl + "/api/run-flow",
                    "{\"mode\":\"mock\",\"message\":\"missing-origin\"}",
                    "application/json",
                    null,
                    Duration.ofSeconds(3));
            assertEquals(403, missingOrigin.statusCode(), missingOrigin.body());
            assertTrue(missingOrigin.body().contains("Origin is not allowed"), missingOrigin.body());
        }
    }

    @Test
    void originMustExactlyMatchTheRequestHost() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String ipBaseUrl = "http://127.0.0.1:" + app.port();
            String localhostBaseUrl = "http://localhost:" + app.port();
            String body = "{\"mode\":\"mock\",\"message\":\"origin-check\"}";

            HttpResponse<String> exactIp = post(
                    ipBaseUrl + "/api/run-flow",
                    body, "application/json", ipBaseUrl, Duration.ofSeconds(3));
            HttpResponse<String> localhostAliasForIp = post(
                    ipBaseUrl + "/api/run-flow",
                    body, "application/json", localhostBaseUrl, Duration.ofSeconds(3));
            HttpResponse<String> exactLocalhost = post(
                    localhostBaseUrl + "/api/run-flow",
                    body, "application/json", localhostBaseUrl, Duration.ofSeconds(3));
            HttpResponse<String> ipAliasForLocalhost = post(
                    localhostBaseUrl + "/api/run-flow",
                    body, "application/json", ipBaseUrl, Duration.ofSeconds(3));

            assertEquals(200, exactIp.statusCode(), exactIp.body());
            assertEquals(403, localhostAliasForIp.statusCode(), localhostAliasForIp.body());
            assertEquals(200, exactLocalhost.statusCode(), exactLocalhost.body());
            assertEquals(403, ipAliasForLocalhost.statusCode(), ipAliasForLocalhost.body());
        }
    }

    @Test
    void rejectsDnsRebindingHostEvenWhenOriginMatches() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            byte[] body = "{\"mode\":\"mock\",\"message\":\"rebind\"}"
                    .getBytes(StandardCharsets.UTF_8);
            String authority = "attacker.example:" + app.port();
            String headers = "POST /api/run-flow HTTP/1.1\r\n"
                    + "Host: " + authority + "\r\n"
                    + "Origin: http://" + authority + "\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: " + body.length + "\r\n"
                    + "Connection: close\r\n\r\n";

            String response;
            try (Socket socket = new Socket("127.0.0.1", app.port())) {
                socket.getOutputStream().write(headers.getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().write(body);
                socket.getOutputStream().flush();
                response = new String(
                        socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }

            assertTrue(response.startsWith("HTTP/1.1 403"), response);
        }
    }

    @Test
    void requiresJsonContentTypeAndAcceptsCaseInsensitiveMediaType() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String url = "http://127.0.0.1:" + app.port() + "/api/run-flow";
            String body = "{\"mode\":\"mock\",\"message\":\"media-type-check\"}";

            HttpResponse<String> missing = post(
                    url, body, null, null, Duration.ofSeconds(3));
            HttpResponse<String> caseInsensitive = post(
                    url, body, "Application/JSON", originOf(url), Duration.ofSeconds(3));

            assertEquals(415, missing.statusCode(), missing.body());
            assertEquals(200, caseInsensitive.statusCode(), caseInsensitive.body());
            assertTrue(caseInsensitive.body().contains("\"mode\":\"mock\""));
        }
    }

    @Test
    void malformedJsonReturnsBadRequestForFlowAndOperatorRoutes() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> flow = postJson(baseUrl + "/api/run-flow", "{");
            HttpResponse<String> operator = postJson(baseUrl + "/api/topics", "{");

            assertEquals(400, flow.statusCode(), flow.body());
            assertEquals(400, operator.statusCode(), operator.body());
            assertTrue(flow.body().length() <= 384, flow.body());
            assertTrue(operator.body().length() <= 384, operator.body());
        }
    }

    @Test
    void trailingJsonReturnsBadRequestForFlowAndOperatorRoutes() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> flow = postJson(baseUrl + "/api/run-flow", "{} {}");
            HttpResponse<String> operator = postJson(baseUrl + "/api/topics", "{} {}");

            assertEquals(400, flow.statusCode(), flow.body());
            assertEquals(400, operator.statusCode(), operator.body());
        }
    }

    @Test
    void unknownJsonFieldsReturnBadRequestForFlowAndOperatorRoutes() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> flow = postJson(
                    baseUrl + "/api/run-flow",
                    "{\"mode\":\"mock\",\"unexpected\":true}");
            HttpResponse<String> operator = postJson(
                    baseUrl + "/api/topics",
                    "{\"bootstrapServers\":\"127.0.0.1:65535\",\"unexpected\":true}");

            assertEquals(400, flow.statusCode(), flow.body());
            assertEquals(400, operator.statusCode(), operator.body());
        }
    }

    @Test
    void duplicateJsonFieldsReturnBadRequestForFlowAndOperatorRoutes() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> flow = postJson(
                    baseUrl + "/api/run-flow",
                    "{\"mode\":\"mock\",\"mode\":\"mock\"}");
            HttpResponse<String> operator = postJson(
                    baseUrl + "/api/topic/reliability",
                    "{\"clearRetry\":false,\"clearRetry\":false}");

            assertEquals(400, flow.statusCode(), flow.body());
            assertEquals(400, operator.statusCode(), operator.body());
            assertTrue(flow.body().toLowerCase().contains("duplicate"), flow.body());
            assertTrue(operator.body().toLowerCase().contains("duplicate"), operator.body());
        }
    }

    @Test
    void exposesTheServerConfiguredBootstrapForTheFrontend() throws Exception {
        String configuredBootstrap = "127.0.0.1:19091,127.0.0.1:19092";
        try (MiniMqWebApp app = MiniMqWebApp.start(0, configuredBootstrap)) {
            HttpResponse<String> response = get(
                    "http://127.0.0.1:" + app.port() + "/api/config");

            assertEquals(200, response.statusCode(), response.body());
            assertTrue(response.body().contains(
                    "\"bootstrapServers\":\"" + configuredBootstrap + "\""),
                    response.body());
        }
    }

    @Test
    void topicDeleteUsesConfiguredTimeoutLongerThanBrokerDeletionDeadline() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("web-delete-timeout-controller");
        Path followerDir = BrokerTestHarness.newDataDir("web-delete-timeout-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(
                     controllerDir,
                     "--topic-delete-timeout-ms", "11000",
                     "--controller-broker-lease-timeout-ms", "30000",
                     "--controller-failover-grace-ms", "30000",
                     "--controller-session-ttl-seconds", "60");
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", controller.port());
             MiniMqWebApp app = MiniMqWebApp.start(
                     0,
                     controller.bootstrapServers(),
                     BrokerTestHarness.ADMIN_TOKEN,
                     Duration.ofSeconds(12))) {
            String baseUrl = "http://127.0.0.1:" + app.port();
            assertOk(postJson(baseUrl + "/api/topic/create", """
                    {
                      "bootstrapServers":"%s","topic":"slow-delete",
                      "partitions":1,"replicationFactor":1
                    }
                    """.formatted(controller.bootstrapServers())));
            follower.close();

            long startedNanos = System.nanoTime();
            HttpResponse<String> response = post(
                    baseUrl + "/api/topic/delete",
                    """
                    {"bootstrapServers":"%s","topic":"slow-delete"}
                    """.formatted(controller.bootstrapServers()),
                    "application/json",
                    originOf(baseUrl),
                    Duration.ofSeconds(20));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

            assertEquals(200, response.statusCode(), response.body());
            assertTrue(
                    response.body().contains("\"tombstoneState\":\"DELETING\"")
                            || response.body().contains("\"tombstoneState\":\"DELETED\""),
                    response.body());
            assertTrue(elapsedMs >= 10_000, "delete returned too early: " + elapsedMs + "ms");
        }
    }

    @Test
    void closeShutsDownTheHttpRequestExecutor() throws Exception {
        MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535");
        Field serverField = MiniMqWebApp.class.getDeclaredField("server");
        serverField.setAccessible(true);
        HttpServer server = (HttpServer) serverField.get(app);
        ExecutorService executor = (ExecutorService) server.getExecutor();

        app.close();

        assertTrue(executor.isShutdown());
    }

    @Test
    void autoFlowPropagatesInterruptionAndRestoresTheInterruptFlag() throws Exception {
        MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535");
        try {
            MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest();
            request.setMode("auto");
            request.setBootstrapServers("127.0.0.1:65535");
            request.setTopic("interrupted-auto-flow");
            request.setMessage("not-fallback");
            request.setGroupId("interrupted-auto-flow-group");
            Method runAutoFlow = MiniMqWebApp.class.getDeclaredMethod(
                    "runAutoFlow", MiniMqWebApp.FlowRequest.class);
            runAutoFlow.setAccessible(true);

            Thread.currentThread().interrupt();
            InvocationTargetException thrown = assertThrows(
                    InvocationTargetException.class,
                    () -> runAutoFlow.invoke(app, request));

            assertInstanceOf(InterruptedException.class, thrown.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            app.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void realFlowDoesNotRetryAfterPollingIsInterrupted() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-interrupted-runtime");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers())) {
            AtomicInteger sends = new AtomicInteger();
            Producer producer = new Producer(broker.bootstrapServers(), "interrupting-producer") {
                @Override
                public ProduceResult send(ProducerRecord record) {
                    sends.incrementAndGet();
                    return new ProduceResult(record.topic(), 0, 7L);
                }
            };
            Consumer consumer = new Consumer(
                    broker.bootstrapServers(), "interrupting-group", "interrupting-consumer") {
                @Override
                public List<com.minimq.client.model.ConsumerRecord> poll(Duration timeout)
                        throws InterruptedException {
                    throw new InterruptedException("simulated poll cancellation");
                }
            };
            ControllerAwareNetworkClient adminClient = new ControllerAwareNetworkClient(
                    broker.bootstrapServers(), "interrupting-admin");
            MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest()
                    .withDefaults(broker.bootstrapServers());
            request.setBootstrapServers("invalid-bootstrap");
            request.setTopic("interrupted-real-flow");

            Class<?> runtimeClass = Class.forName(
                    "com.minimq.demo.web.MiniMqWebApp$RealFlowRuntime");
            Constructor<?> runtimeConstructor = runtimeClass.getDeclaredConstructor(
                    ControllerAwareNetworkClient.class, Producer.class, Consumer.class, String.class);
            runtimeConstructor.setAccessible(true);
            Object runtime = runtimeConstructor.newInstance(adminClient, producer, consumer, "secret");
            Field topicReady = runtimeClass.getDeclaredField("topicReady");
            topicReady.setAccessible(true);
            topicReady.setBoolean(runtime, true);
            Field consumerReady = runtimeClass.getDeclaredField("consumerReady");
            consumerReady.setAccessible(true);
            consumerReady.setBoolean(runtime, true);

            Method keyFor = MiniMqWebApp.class.getDeclaredMethod(
                    "keyFor", MiniMqWebApp.FlowRequest.class);
            keyFor.setAccessible(true);
            Object key = keyFor.invoke(null, request);
            Field runtimes = MiniMqWebApp.class.getDeclaredField("runtimes");
            runtimes.setAccessible(true);
            ((Map<Object, Object>) runtimes.get(app)).put(key, runtime);

            Method runRealFlow = MiniMqWebApp.class.getDeclaredMethod(
                    "runRealFlow", MiniMqWebApp.FlowRequest.class);
            runRealFlow.setAccessible(true);
            InvocationTargetException thrown = assertThrows(
                    InvocationTargetException.class,
                    () -> runRealFlow.invoke(app, request));

            assertInstanceOf(InterruptedException.class, thrown.getCause());
            assertEquals(1, sends.get());
            assertTrue(Thread.currentThread().isInterrupted());
            Thread.interrupted();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void autoFlowDoesNotFallbackOrRetryAfterProduceSucceeded() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-post-produce-failure");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers())) {
            AtomicInteger sends = new AtomicInteger();
            Producer producer = new Producer(broker.bootstrapServers(), "side-effect-producer") {
                @Override
                public ProduceResult send(ProducerRecord record) {
                    sends.incrementAndGet();
                    return new ProduceResult(record.topic(), 0, 7L);
                }
            };
            Consumer consumer = new Consumer(
                    broker.bootstrapServers(), "side-effect-group", "side-effect-consumer") {
                @Override
                public List<com.minimq.client.model.ConsumerRecord> poll(Duration timeout) {
                    throw new IllegalStateException("simulated failure after produce");
                }
            };
            ControllerAwareNetworkClient adminClient = new ControllerAwareNetworkClient(
                    broker.bootstrapServers(), "side-effect-admin");
            MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest()
                    .withDefaults(broker.bootstrapServers());
            request.setBootstrapServers("invalid-bootstrap");
            request.setTopic("post-produce-failure");

            Class<?> runtimeClass = Class.forName(
                    "com.minimq.demo.web.MiniMqWebApp$RealFlowRuntime");
            Constructor<?> runtimeConstructor = runtimeClass.getDeclaredConstructor(
                    ControllerAwareNetworkClient.class, Producer.class, Consumer.class, String.class);
            runtimeConstructor.setAccessible(true);
            Object runtime = runtimeConstructor.newInstance(adminClient, producer, consumer, "secret");
            Field topicReady = runtimeClass.getDeclaredField("topicReady");
            topicReady.setAccessible(true);
            topicReady.setBoolean(runtime, true);
            Field consumerReady = runtimeClass.getDeclaredField("consumerReady");
            consumerReady.setAccessible(true);
            consumerReady.setBoolean(runtime, true);

            Method keyFor = MiniMqWebApp.class.getDeclaredMethod(
                    "keyFor", MiniMqWebApp.FlowRequest.class);
            keyFor.setAccessible(true);
            Object key = keyFor.invoke(null, request);
            Field runtimes = MiniMqWebApp.class.getDeclaredField("runtimes");
            runtimes.setAccessible(true);
            ((Map<Object, Object>) runtimes.get(app)).put(key, runtime);

            Method runAutoFlow = MiniMqWebApp.class.getDeclaredMethod(
                    "runAutoFlow", MiniMqWebApp.FlowRequest.class);
            runAutoFlow.setAccessible(true);

            assertThrows(
                    InvocationTargetException.class,
                    () -> runAutoFlow.invoke(app, request));
            assertEquals(1, sends.get());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void autoFlowDoesNotFallbackAfterTopicCreationWhenSubscribeFails() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-topic-create-side-effect");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            Consumer consumer = new Consumer(
                    broker.bootstrapServers(), "topic-create-side-effect-group", "topic-create-side-effect-consumer") {
                @Override
                public void subscribe(String topic) {
                    throw new IllegalStateException("simulated subscribe failure");
                }
            };
            Producer producer = new Producer(
                    broker.bootstrapServers(), "topic-create-side-effect-producer");
            ControllerAwareNetworkClient adminClient = new ControllerAwareNetworkClient(
                    broker.bootstrapServers(), "topic-create-side-effect-admin");
            adminClient.connect();

            MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest()
                    .withDefaults(broker.bootstrapServers());
            request.setTopic("topic-create-side-effect");
            request.setMessage("must-not-fallback");

            Class<?> runtimeClass = Class.forName(
                    "com.minimq.demo.web.MiniMqWebApp$RealFlowRuntime");
            Constructor<?> runtimeConstructor = runtimeClass.getDeclaredConstructor(
                    ControllerAwareNetworkClient.class, Producer.class, Consumer.class, String.class);
            runtimeConstructor.setAccessible(true);
            Object runtime = runtimeConstructor.newInstance(
                    adminClient, producer, consumer, BrokerTestHarness.ADMIN_TOKEN);
            Method keyFor = MiniMqWebApp.class.getDeclaredMethod(
                    "keyFor", MiniMqWebApp.FlowRequest.class);
            keyFor.setAccessible(true);
            Object key = keyFor.invoke(null, request);
            Field runtimes = MiniMqWebApp.class.getDeclaredField("runtimes");
            runtimes.setAccessible(true);
            ((Map<Object, Object>) runtimes.get(app)).put(key, runtime);

            Method runAutoFlow = MiniMqWebApp.class.getDeclaredMethod(
                    "runAutoFlow", MiniMqWebApp.FlowRequest.class);
            runAutoFlow.setAccessible(true);
            InvocationTargetException thrown = assertThrows(
                    InvocationTargetException.class,
                    () -> runAutoFlow.invoke(app, request));

            assertEquals("FlowSideEffectException", thrown.getCause().getClass().getSimpleName());
            assertTrue(thrown.getCause().getMessage().contains("external side effect"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void autoFlowDoesNotFallbackWhenSubscribeFailsForExistingTopic() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-existing-topic-subscribe");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(
                     0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            assertOk(postJson("http://127.0.0.1:" + app.port() + "/api/topic/create", """
                    {
                      "bootstrapServers":"%s","topic":"existing-subscribe-topic",
                      "partitions":1,"replicationFactor":1
                    }
                    """.formatted(broker.bootstrapServers())));

            Consumer consumer = new Consumer(
                    broker.bootstrapServers(),
                    "existing-subscribe-group",
                    "existing-subscribe-consumer") {
                @Override
                public void subscribe(String topic) {
                    throw new IllegalStateException("simulated subscribe failure");
                }
            };
            Producer producer = new Producer(
                    broker.bootstrapServers(), "existing-subscribe-producer");
            ControllerAwareNetworkClient adminClient = new ControllerAwareNetworkClient(
                    broker.bootstrapServers(), "existing-subscribe-admin");
            adminClient.connect();

            MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest()
                    .withDefaults(broker.bootstrapServers());
            request.setTopic("existing-subscribe-topic");
            request.setMessage("must-not-fallback");

            Class<?> runtimeClass = Class.forName(
                    "com.minimq.demo.web.MiniMqWebApp$RealFlowRuntime");
            Constructor<?> runtimeConstructor = runtimeClass.getDeclaredConstructor(
                    ControllerAwareNetworkClient.class, Producer.class, Consumer.class, String.class);
            runtimeConstructor.setAccessible(true);
            Object runtime = runtimeConstructor.newInstance(
                    adminClient, producer, consumer, BrokerTestHarness.ADMIN_TOKEN);
            Method keyFor = MiniMqWebApp.class.getDeclaredMethod(
                    "keyFor", MiniMqWebApp.FlowRequest.class);
            keyFor.setAccessible(true);
            Object key = keyFor.invoke(null, request);
            Field runtimes = MiniMqWebApp.class.getDeclaredField("runtimes");
            runtimes.setAccessible(true);
            ((Map<Object, Object>) runtimes.get(app)).put(key, runtime);

            Method runAutoFlow = MiniMqWebApp.class.getDeclaredMethod(
                    "runAutoFlow", MiniMqWebApp.FlowRequest.class);
            runAutoFlow.setAccessible(true);
            InvocationTargetException thrown = assertThrows(
                    InvocationTargetException.class,
                    () -> runAutoFlow.invoke(app, request));

            assertEquals("FlowSideEffectException", thrown.getCause().getClass().getSimpleName());
            assertTrue(thrown.getCause().getMessage().contains("external side effect"));
        }
    }

    @Test
    void rejectedAsyncRuntimeCleanupFallsBackToSynchronousClose() throws Exception {
        MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535");
        Class<?> runtimeType = Class.forName(
                "com.minimq.demo.web.MiniMqWebApp$RealFlowRuntime");
        Constructor<?> constructor = runtimeType.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object runtime = constructor.newInstance(null, null, null, "test-token");
        Method closeRuntimeAsync = MiniMqWebApp.class.getDeclaredMethod(
                "closeRuntimeAsync", runtimeType);
        closeRuntimeAsync.setAccessible(true);
        Field closed = runtimeType.getDeclaredField("closed");
        closed.setAccessible(true);

        app.close();
        try {
            closeRuntimeAsync.invoke(app, runtime);
        } catch (InvocationTargetException failure) {
            throw (Exception) failure.getCause();
        }

        assertTrue(closed.getBoolean(runtime), "rejected cleanup did not close the runtime");
    }

    @Test
    void servesOperatorConsoleFrontendAssets() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> html = get(baseUrl + "/");
            HttpResponse<String> css = get(baseUrl + "/styles.css");
            HttpResponse<String> js = get(baseUrl + "/app.js");
            HttpResponse<String> favicon = get(baseUrl + "/favicon.ico");

            assertEquals(200, html.statusCode());
            assertTrue(html.body().contains("MiniMQ Operator Console"));
            assertTrue(html.body().contains("data-testid=\"run-flow\""));
            assertTrue(html.body().contains("id=\"bootstrapServers\""));
            assertFalse(html.body().contains("value=\"127.0.0.1:19092\""));
            for (String view : List.of(
                    "overview", "messages", "topics", "groups", "brokers", "tools")) {
                assertTrue(html.body().contains("data-view-target=\"" + view + "\""),
                        "HTML missing view " + view);
            }
            assertTrue(html.body().contains("id=\"structuredResult\""));
            assertTrue(html.body().contains("id=\"rawJson\""));
            assertTrue(html.body().contains("id=\"flowRawJson\""));
            assertTrue(html.body().contains("id=\"flowError\""));
            assertTrue(html.body().indexOf("id=\"topic\"")
                    < html.body().indexOf("data-view=\"overview\""),
                    "Topic target must remain visible in the shared request context");
            assertEquals(200, css.statusCode());
            assertTrue(css.body().contains("--accent: #ff4f00"));
            assertTrue(css.body().contains("grid-template-columns: 232px minmax(0, 1fr)"));
            assertTrue(css.body().contains("font-variant-numeric: tabular-nums"));
            assertEquals(200, js.statusCode());
            assertTrue(js.body().contains("runMainFlow"));
            assertTrue(js.body().contains("fetch('/api/config'"));
            assertTrue(js.body().contains("activateView"));
            assertTrue(js.body().contains("renderStructuredResult"));
            assertTrue(js.body().contains("renderValue"));
            assertTrue(js.body().contains("window.confirm"));
            assertTrue(js.body().contains("viewResultState"));
            assertTrue(js.body().contains("restoreOperatorResult"));
            assertTrue(js.body().contains("hasSuccessfulFlowResult"));
            assertTrue(js.body().contains("Delete topic ${topic}?"));
            assertTrue(html.body().contains(
                    "MiniMQ rejects taking the last in-sync leader offline"));
            assertTrue(js.body().contains("friendlyOperatorError"));
            assertTrue(js.body().contains(
                    "Add another online broker and retry"));
            assertTrue(html.body().contains("data-testid=\"operator-log\""));
            for (String endpoint : List.of(
                    "/api/ops-status",
                    "/api/group-status",
                    "/api/topics",
                    "/api/topic/create",
                    "/api/topic/describe",
                    "/api/topic/delete",
                    "/api/topic/increase",
                    "/api/topic/alter-config",
                    "/api/topic/reassign",
                    "/api/topic/elect-preferred",
                    "/api/failover-audit",
                    "/api/auth-status",
                    "/api/topic/reliability",
                    "/api/broker/offline",
                    "/api/runtime-metrics",
                    "/api/retry-produce",
                    "/api/dead-letter-produce",
                    "/api/delayed-produce")) {
                assertTrue(html.body().contains(endpoint), "HTML missing " + endpoint);
                assertTrue(js.body().contains(endpoint), "JavaScript missing " + endpoint);
            }
            assertFalse(html.body().contains("admin-token"));
            assertFalse(js.body().contains("admin-token"));
            assertEquals(204, favicon.statusCode());
        }
    }

    @Test
    void operatorPanelSelectsBrokerAndMapsVisibleLimits() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(
                0, "127.0.0.1:19092,127.0.0.1:19093")) {
            String baseUrl = "http://127.0.0.1:" + app.port();
            HttpResponse<String> html = get(baseUrl + "/");
            HttpResponse<String> js = get(baseUrl + "/app.js");

            assertTrue(html.body().contains("id=\"operatorBroker\""));
            assertTrue(js.body().contains("brokerLimit: resultLimit"));
            assertTrue(js.body().contains("memberLimit: resultLimit"));
            assertTrue(js.body().contains("counterLimit: resultLimit"));
            assertTrue(js.body().contains("latencyLimit: resultLimit"));
            assertTrue(js.body().contains("lagLimit: resultLimit"));
            assertTrue(js.body().contains("partitionLimit"));
        }
    }

    @Test
    void exposesEveryV4OperatorEndpointWithoutReturningServerCredentials() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-v4-operator");
        Path followerDir = BrokerTestHarness.newDataDir("web-v4-operator-follower");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(
                     dataDir, "--topic-delete-timeout-ms", "500");
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", broker.port());
             MiniMqWebApp app = MiniMqWebApp.start(
                     0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String baseUrl = "http://127.0.0.1:" + app.port();
            String controller = "\"bootstrapServers\":\"" + broker.bootstrapServers() + "\"";
            String local = "\"broker\":\"" + broker.bootstrapServers() + "\"";

            HttpResponse<String> offline = postJson(baseUrl + "/api/broker/offline", """
                    {%s,"brokerId":1}
                    """.formatted(controller));
            assertOk(offline);

            for (String topic : new String[] {"web-ops", "web-ops-retry", "web-ops-dlq"}) {
                assertOk(postJson(baseUrl + "/api/topic/create", """
                        {%s,"topic":"%s","partitions":1,"replicationFactor":1}
                        """.formatted(controller, topic)));
            }
            assertOk(postJson(baseUrl + "/api/topic/reliability", """
                    {%s,"sourceTopic":"web-ops","retryTopic":"web-ops-retry",\
                    "deadLetterTopic":"web-ops-dlq"}
                    """.formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/increase", """
                    {%s,"topic":"web-ops","targetPartitionCount":2}
                    """.formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/alter-config", """
                    {%s,"topic":"web-ops","minInSyncReplicas":1}
                    """.formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/reassign", """
                    {%s,"topic":"web-ops","assignment":"0:0"}
                    """.formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/elect-preferred", """
                    {%s,"topic":"web-ops","partition":0}
                    """.formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/elect-preferred", """
                    {%s,"topic":"web-ops"}
                    """.formatted(controller)));
            assertEquals(400, postJson(baseUrl + "/api/topic/reassign", """
                    {%s,"topic":"web-ops","assignment":"0"}
                    """.formatted(controller)).statusCode());
            assertEquals(400, postJson(baseUrl + "/api/topic/reassign", """
                    {%s,"topic":"web-ops","assignment":"x:1"}
                    """.formatted(controller)).statusCode());
            assertEquals(400, postJson(baseUrl + "/api/topic/reassign", """
                    {%s,"topic":"web-ops","assignment":"0:a"}
                    """.formatted(controller)).statusCode());
            assertEquals(400, postJson(baseUrl + "/api/topic/reassign", """
                    {%s,"topic":"web-ops","assignment":"0:"}
                    """.formatted(controller)).statusCode());
            assertOk(postJson(baseUrl + "/api/topics", "{%s,\"limit\":25}".formatted(controller)));
            assertOk(postJson(baseUrl + "/api/topic/describe", """
                    {%s,"topic":"web-ops","partitionLimit":25}
                    """.formatted(controller)));

            assertOk(postJson(baseUrl + "/api/ops-status", "{%s}".formatted(local)));
            assertOk(postJson(baseUrl + "/api/failover-audit", "{%s}".formatted(local)));
            assertOk(postJson(baseUrl + "/api/auth-status", "{%s}".formatted(local)));
            HttpResponse<String> metrics = postJson(
                    baseUrl + "/api/runtime-metrics", "{%s}".formatted(local));
            assertOk(metrics);
            assertTrue(metrics.body().contains("\"uptimeMs\""));
            assertTrue(metrics.body().contains("\"produceBytes\""));
            assertTrue(metrics.body().contains("\"fetchBytes\""));

            assertOk(postJson(baseUrl + "/api/run-flow", """
                    {
                      "mode":"real","bootstrapServers":"%s","topic":"web-ops",
                      "message":"group-seed","groupId":"web-ops-group"
                    }
                    """.formatted(broker.bootstrapServers())));
            assertOk(postJson(baseUrl + "/api/group-status", """
                    {%s,"groupId":"web-ops-group","topic":"web-ops"}
                    """.formatted(controller)));

            long deliverAt = System.currentTimeMillis();
            assertOk(postJson(baseUrl + "/api/delayed-produce", """
                    {%s,"topic":"web-ops","message":"later","deliverAtEpochMs":%d}
                    """.formatted(controller, deliverAt)));
            assertOk(postJson(baseUrl + "/api/retry-produce", """
                    {%s,"sourceTopic":"web-ops","message":"retry","deliverAtEpochMs":%d}
                    """.formatted(controller, deliverAt)));
            assertOk(postJson(baseUrl + "/api/dead-letter-produce", """
                    {%s,"sourceTopic":"web-ops","message":"failed"}
                    """.formatted(controller)));

            assertOk(postJson(baseUrl + "/api/topic/delete", """
                    {%s,"topic":"web-ops-dlq"}
                    """.formatted(controller)));
            assertFalse(offline.body().contains(BrokerTestHarness.ADMIN_TOKEN));
        }
    }

    @Test
    void controllerRoutesUseTheCompleteBootstrapListAndDiscoverThroughFollower() throws Exception {
        Path controllerDir = BrokerTestHarness.newDataDir("web-controller-routing-controller");
        Path followerDir = BrokerTestHarness.newDataDir("web-controller-routing-follower");
        try (BrokerTestHarness controller = BrokerTestHarness.startController(controllerDir);
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", controller.port());
             MiniMqWebApp app = MiniMqWebApp.start(
                     0,
                     "127.0.0.1:1," + follower.bootstrapServers()
                             + "," + controller.bootstrapServers(),
                     BrokerTestHarness.ADMIN_TOKEN)) {
            String bootstrapServers = "127.0.0.1:1," + follower.bootstrapServers()
                    + "," + controller.bootstrapServers();
            HttpResponse<String> response = postJson(
                    "http://127.0.0.1:" + app.port() + "/api/topic/create", """
                    {
                      "bootstrapServers":"%s","topic":"web-routed-topic",
                      "partitions":1,"replicationFactor":1
                    }
                    """.formatted(bootstrapServers));

            assertOk(response);
            assertTrue(response.body().contains("web-routed-topic"));
            assertFalse(response.body().contains(BrokerTestHarness.ADMIN_TOKEN));
        }
    }

    @Test
    void brokerLocalRoutesRequireOneEndpointAndBoundCredentialFreeErrors() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(
                0, "127.0.0.1:65535", BrokerTestHarness.ADMIN_TOKEN)) {
            String url = "http://127.0.0.1:" + app.port() + "/api/ops-status";

            HttpResponse<String> missingBroker = postJson(url, "{}");
            HttpResponse<String> multipleBrokers = postJson(
                    url, "{\"broker\":\"127.0.0.1:1,127.0.0.1:2\"}");
            HttpResponse<String> malformed = postJson(
                    url, "{\"broker\":\"127.0.0.1:1\",\"noise\":\"" + "x".repeat(8_000));

            assertEquals(400, missingBroker.statusCode());
            assertEquals(400, multipleBrokers.statusCode());
            assertTrue(missingBroker.body().contains("single broker endpoint"));
            assertTrue(multipleBrokers.body().contains("single broker endpoint"));
            assertTrue(malformed.body().length() <= 384, malformed.body());
            for (HttpResponse<String> response :
                    List.of(missingBroker, multipleBrokers, malformed)) {
                assertFalse(response.body().contains(BrokerTestHarness.ADMIN_TOKEN));
            }
        }
    }

    @Test
    void runsRealMainFlowThroughHttpApi() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-real-flow");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String body = """
                    {
                      "mode": "real",
                      "bootstrapServers": "%s",
                      "topic": "web-orders-real",
                      "partitions": 1,
                      "key": "user-1",
                      "message": "hello-from-web",
                      "groupId": "web-group-real"
                    }
                    """.formatted(broker.bootstrapServers());

            HttpResponse<String> response = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", body);

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"ok\":true"));
            assertTrue(response.body().contains("\"mode\":\"real\""));
            assertTrue(response.body().contains("\"topic\":\"web-orders-real\""));
            assertTrue(response.body().contains("\"consumedValue\":\"hello-from-web\""));
            assertTrue(response.body().contains("\"offset\":0"));
            assertTrue(response.body().contains("\"message\":\"MiniMQ main flow completed\""));
        }
    }

    @Test
    void autoModeKeepsUsingRealFlowWhenTopicAlreadyExists() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-auto-existing-topic");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String firstBody = """
                    {
                      "mode": "auto",
                      "bootstrapServers": "%s",
                      "topic": "web-orders-existing",
                      "partitions": 1,
                      "key": "user-1",
                      "message": "first-message",
                      "groupId": "web-group-existing"
                    }
                    """.formatted(broker.bootstrapServers());
            String secondBody = firstBody.replace("first-message", "second-message");

            HttpResponse<String> first = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", firstBody);
            HttpResponse<String> second = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", secondBody);

            assertEquals(200, first.statusCode());
            assertTrue(first.body().contains("\"mode\":\"real\""));
            assertTrue(first.body().contains("\"fallback\":false"));
            assertEquals(200, second.statusCode());
            assertTrue(second.body().contains("\"mode\":\"real\""));
            assertTrue(second.body().contains("\"fallback\":false"));
            assertTrue(second.body().contains("\"consumedValue\":\"second-message\""));
        }
    }

    @Test
    void reusesRealFlowRuntimeForSameBootstrapTopicAndGroup() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-reuse-runtime");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String firstBody = """
                    {
                      "mode": "real",
                      "bootstrapServers": "%s",
                      "topic": "web-orders-reuse",
                      "partitions": 1,
                      "key": "user-1",
                      "message": "reuse-first",
                      "groupId": "web-group-reuse"
                    }
                    """.formatted(broker.bootstrapServers());
            String secondBody = firstBody.replace("reuse-first", "reuse-second");

            assertEquals(0, app.runtimeCount());

            HttpResponse<String> first = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", firstBody);
            HttpResponse<String> second = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", secondBody);

            assertEquals(200, first.statusCode());
            assertEquals(200, second.statusCode());
            assertTrue(second.body().contains("\"mode\":\"real\""));
            assertTrue(second.body().contains("\"fallback\":false"));
            assertTrue(second.body().contains("\"consumedValue\":\"reuse-second\""));
            assertEquals(1, app.runtimeCount());
        }
    }

    @Test
    void isolatesConcurrentRealFlowRequestsOnSharedRuntime() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-concurrent-runtime");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String url = "http://127.0.0.1:" + app.port() + "/api/run-flow";
            String bodyTemplate = """
                    {
                      "mode": "real",
                      "bootstrapServers": "%s",
                      "topic": "web-orders-concurrent",
                      "partitions": 1,
                      "key": "user-1",
                      "message": "%s",
                      "groupId": "web-group-concurrent"
                    }
                    """;
            HttpResponse<String> warmup = postJson(url, bodyTemplate.formatted(broker.bootstrapServers(), "concurrent-warmup"));
            assertEquals(200, warmup.statusCode());

            CompletableFuture<HttpResponse<String>> first = CompletableFuture.supplyAsync(
                    () -> postJsonUnchecked(url, bodyTemplate.formatted(broker.bootstrapServers(), "concurrent-first")));
            CompletableFuture<HttpResponse<String>> second = CompletableFuture.supplyAsync(
                    () -> postJsonUnchecked(url, bodyTemplate.formatted(broker.bootstrapServers(), "concurrent-second")));

            HttpResponse<String> firstResponse = first.get(15, TimeUnit.SECONDS);
            HttpResponse<String> secondResponse = second.get(15, TimeUnit.SECONDS);

            assertEquals(200, firstResponse.statusCode());
            assertEquals(200, secondResponse.statusCode());
            assertTrue(firstResponse.body().contains("\"consumedValue\":\"concurrent-first\""));
            assertTrue(secondResponse.body().contains("\"consumedValue\":\"concurrent-second\""));
            assertEquals(1, app.runtimeCount());
        }
    }

    @Test
    void rebuildsCachedRuntimeAfterRuntimeFailure() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-rebuild-runtime");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String body = """
                    {
                      "mode": "real",
                      "bootstrapServers": "%s",
                      "topic": "web-orders-rebuild",
                      "partitions": 1,
                      "key": "user-1",
                      "message": "before-rebuild",
                      "groupId": "web-group-rebuild"
                    }
                    """.formatted(broker.bootstrapServers());
            String secondBody = body.replace("before-rebuild", "after-rebuild");
            String url = "http://127.0.0.1:" + app.port() + "/api/run-flow";

            HttpResponse<String> first = postJson(url, body);
            assertEquals(200, first.statusCode());
            assertTrue(first.body().contains("\"mode\":\"real\""));
            assertEquals(1, app.runtimeCount());

            app.closeRuntimeFor(broker.bootstrapServers(), "web-orders-rebuild", "web-group-rebuild");
            HttpResponse<String> second = postJson(url, secondBody);

            assertEquals(200, second.statusCode());
            assertTrue(second.body().contains("\"mode\":\"real\""));
            assertTrue(second.body().contains("\"consumedValue\":\"after-rebuild\""));
            assertEquals(1, app.runtimeCount());
        }
    }

    @Test
    void evictsRuntimeCacheWhenLimitIsExceeded() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("web-runtime-cache-limit");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir);
             MiniMqWebApp app = MiniMqWebApp.start(0, broker.bootstrapServers(), BrokerTestHarness.ADMIN_TOKEN)) {
            String url = "http://127.0.0.1:" + app.port() + "/api/run-flow";
            for (int i = 0; i <= MiniMqWebApp.MAX_RUNTIMES; i++) {
                String body = """
                        {
                          "mode": "real",
                          "bootstrapServers": "%s",
                          "topic": "web-orders-cache-%d",
                          "partitions": 1,
                          "key": "user-1",
                          "message": "cache-message-%d",
                          "groupId": "web-group-cache-%d"
                        }
                        """.formatted(broker.bootstrapServers(), i, i, i);

                HttpResponse<String> response = postJson(url, body);

                assertEquals(200, response.statusCode());
                assertTrue(response.body().contains("\"mode\":\"real\""));
                assertTrue(response.body().contains("\"consumedValue\":\"cache-message-" + i + "\""));
                assertTrue(app.runtimeCount() <= MiniMqWebApp.MAX_RUNTIMES);
            }
            assertEquals(MiniMqWebApp.MAX_RUNTIMES, app.runtimeCount());
        }
    }

    @Test
    void fallsBackToMockWhenAutoModeCannotReachBroker() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:1")) {
            String body = """
                    {
                      "mode": "auto",
                      "bootstrapServers": "127.0.0.1:1",
                      "topic": "web-orders-mock",
                      "partitions": 1,
                      "key": "user-2",
                      "message": "mock-message",
                      "groupId": "web-group-mock"
                    }
                    """;

            HttpResponse<String> response = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", body);

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"ok\":true"));
            assertTrue(response.body().contains("\"mode\":\"mock\""));
            assertTrue(response.body().contains("\"fallback\":true"));
            assertTrue(response.body().contains("\"consumedValue\":\"mock-message\""));
            assertTrue(response.body().contains("\"message\":\"Broker unavailable; switched to mock main flow\""));
        }
    }


    @Test
    void returnsHttpErrorsForUnsupportedFrontendRequests() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> missing = get(baseUrl + "/missing");
            HttpResponse<String> methodNotAllowed = postJson(baseUrl + "/styles.css", "{}");
            HttpResponse<String> badJson = postJson(baseUrl + "/api/run-flow", "{");
            HttpResponse<String> invalidRealBroker = postJson(baseUrl + "/api/run-flow", """
                    {
                      "mode": "real",
                      "bootstrapServers": "127.0.0.1:1",
                      "topic": "web-orders-error",
                      "message": "will-fail"
                    }
                    """);

            assertEquals(404, missing.statusCode());
            assertTrue(missing.body().contains("Not found"));
            assertEquals(405, methodNotAllowed.statusCode());
            assertTrue(methodNotAllowed.body().contains("Method not allowed"));
            assertEquals(400, badJson.statusCode());
            assertTrue(badJson.body().contains("\"ok\":false"));
            assertEquals(400, invalidRealBroker.statusCode());
            assertTrue(invalidRealBroker.body().contains("\"ok\":false"));
        }
    }

    @Test
    void validatesMalformedBootstrapServers() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String body = """
                    {
                      "mode": "real",
                      "bootstrapServers": "127.0.0.1",
                      "topic": "web-orders-error",
                      "message": "will-fail"
                    }
                    """;

            HttpResponse<String> response = postJson("http://127.0.0.1:" + app.port() + "/api/run-flow", body);

            assertEquals(400, response.statusCode());
            assertTrue(response.body().contains("bootstrapServers must contain host:port"));
        }
    }

    @Test
    void rejectsUnsupportedMethodsForApiAndFavicon() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> apiGet = get(baseUrl + "/api/run-flow");
            HttpResponse<String> faviconPost = postJson(baseUrl + "/favicon.ico", "{}");

            assertEquals(405, apiGet.statusCode());
            assertTrue(apiGet.body().contains("Method not allowed"));
            assertEquals(405, faviconPost.statusCode());
            assertTrue(faviconPost.body().contains("Method not allowed"));
        }
    }

    @Test
    void flowRequestDefaultsAndModeNormalizationAreStable() {
        MiniMqWebApp.FlowRequest request = new MiniMqWebApp.FlowRequest();
        request.setMode("invalid");
        request.setBootstrapServers("");
        request.setTopic(" ");
        request.setPartitions(-1);
        request.setKey(null);
        request.setMessage("");
        request.setGroupId(null);

        MiniMqWebApp.FlowRequest normalized = request.withDefaults("127.0.0.1:9092");

        assertEquals("auto", normalized.normalizedMode());
        assertEquals("127.0.0.1:9092", normalized.bootstrapServers());
        assertEquals("web-orders", normalized.topic());
        assertEquals(4, normalized.partitions());
        assertEquals("web-key", normalized.key());
        assertEquals("hello-minimq", normalized.message());
        assertEquals("web-group", normalized.groupId());
        assertEquals("invalid", normalized.mode());
    }

    @Test
    void indexPathTreatsEmptyJava21RequestTargetAsTheFrontPanel() {
        assertTrue(MiniMqWebApp.isIndexPath("/"));
        assertTrue(MiniMqWebApp.isIndexPath(""));
        assertTrue(MiniMqWebApp.isIndexPath(" / "));
        assertTrue(MiniMqWebApp.isIndexPath(null));
        assertFalse(MiniMqWebApp.isIndexPath("/app.js"));
        assertFalse(MiniMqWebApp.isIndexPath("/api/config"));
    }

    @Test
    void operatorLimitsAreClampedToTheProtocolRange() {
        assertEquals(100, MiniMqWebApp.clampedOperatorLimit(null, 100));
        assertEquals(1, MiniMqWebApp.clampedOperatorLimit(0, 100));
        assertEquals(1, MiniMqWebApp.clampedOperatorLimit(-8, 100));
        assertEquals(1_000, MiniMqWebApp.clampedOperatorLimit(10_000, 100));
        assertEquals(25, MiniMqWebApp.clampedOperatorLimit(25, 100));
    }

    @Test
    void consumeWaitMsUsesDefaultAndClampsConfiguredValues() {
        String previous = System.getProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY);
        try {
            System.clearProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY);
            assertEquals(MiniMqWebApp.DEFAULT_CONSUME_WAIT_MS, MiniMqWebApp.consumeWaitMs());

            System.setProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY, "10");
            assertEquals(MiniMqWebApp.MIN_CONSUME_WAIT_MS, MiniMqWebApp.consumeWaitMs());
            System.setProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY, "250");
            assertEquals(250L, MiniMqWebApp.consumeWaitMs());
            System.setProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY, "99999");
            assertEquals(MiniMqWebApp.MAX_CONSUME_WAIT_MS, MiniMqWebApp.consumeWaitMs());
            System.setProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY, "not-a-number");
            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class, MiniMqWebApp::consumeWaitMs);
            assertTrue(failure.getMessage().contains(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY));
        } finally {
            if (previous == null) {
                System.clearProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY);
            } else {
                System.setProperty(MiniMqWebApp.CONSUME_WAIT_MS_PROPERTY, previous);
            }
        }
    }

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String url, String body) throws Exception {
        return post(url, body, "application/json", originOf(url), Duration.ofSeconds(8));
    }

    private static String originOf(String url) {
        URI uri = URI.create(url);
        return uri.getScheme() + "://" + uri.getAuthority();
    }

    private HttpResponse<String> post(
            String url, String body, String contentType, String origin, Duration timeout)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout);
        if (contentType != null) {
            request.header("Content-Type", contentType);
        }
        if (origin != null) {
            request.header("Origin", origin);
        }
        return http.send(
                request.POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static void assertOk(HttpResponse<String> response) {
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(response.body().contains("\"ok\":true"), response.body());
        assertFalse(response.body().contains(BrokerTestHarness.ADMIN_TOKEN), response.body());
    }

    private HttpResponse<String> postJsonUnchecked(String url, String body) {
        try {
            return postJson(url, body);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final class ConnectionProbe implements AutoCloseable {
        private final ServerSocket server;
        private final CompletableFuture<byte[]> observed;

        private ConnectionProbe() throws Exception {
            server = new ServerSocket(0);
            observed = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(1_000);
                    byte[] bytes = new byte[4_096];
                    int count = socket.getInputStream().read(bytes);
                    return count <= 0 ? new byte[0] : java.util.Arrays.copyOf(bytes, count);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }

        private int port() {
            return server.getLocalPort();
        }

        private boolean connectedWithin(Duration timeout) throws Exception {
            try {
                observed.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return true;
            } catch (TimeoutException expected) {
                return false;
            }
        }

        @Override
        public void close() throws Exception {
            server.close();
            try {
                observed.get(1, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Closing the listener releases an unconnected probe.
            }
        }
    }
}

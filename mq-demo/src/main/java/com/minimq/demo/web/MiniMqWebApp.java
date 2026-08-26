package com.minimq.demo.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minimq.client.BootstrapServers;
import com.minimq.client.Consumer;
import com.minimq.client.ControllerAwareNetworkClient;
import com.minimq.client.NetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.exception.MiniMqConnectionException;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * MiniMQ 前端网关，提供静态 Brutalism 页面，并把主流程按钮映射到真实或 mock 的 HTTP API。
 */
public final class MiniMqWebApp implements AutoCloseable {
    static final int MAX_RUNTIMES = 4;
    static final int MAX_ERROR_CHARS = 256;
    static final String TOPIC_DELETE_REQUEST_TIMEOUT_PROPERTY =
            "minimq.topic.delete.request.timeout.ms";
    static final String ADMIN_TOKEN_PROPERTY = "minimq.admin.token";
    static final String CONSUME_WAIT_MS_PROPERTY = "minimq.web.consume.wait.ms";
    static final Duration DEFAULT_TOPIC_DELETE_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    static final long DEFAULT_CONSUME_WAIT_MS = 750L;
    static final long MIN_CONSUME_WAIT_MS = 50L;
    static final long MAX_CONSUME_WAIT_MS = 5_000L;
    static final int MIN_OPERATOR_LIMIT = 1;
    static final int MAX_OPERATOR_LIMIT = 1_000;
    static final int DEFAULT_TOPIC_PARTITIONS = 4;
    private static final Duration MAX_TOPIC_DELETE_REQUEST_TIMEOUT = Duration.ofMinutes(10);

    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final HttpServer server;
    private final ExecutorService requestExecutor;
    private final String defaultBootstrapServers;
    private final List<BootstrapServers.Endpoint> allowedBootstrapEndpoints;
    private final String adminCredential;
    private final Duration topicDeleteRequestTimeout;
    private final ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "minimq-web-runtime-cleanup");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<RealFlowKey, RealFlowRuntime> runtimes = new LinkedHashMap<>(16, 0.75f, true);

    private MiniMqWebApp(
            HttpServer server,
            ExecutorService requestExecutor,
            String defaultBootstrapServers,
            List<BootstrapServers.Endpoint> allowedBootstrapEndpoints,
            String adminCredential,
            Duration topicDeleteRequestTimeout) {
        this.server = server;
        this.requestExecutor = requestExecutor;
        this.defaultBootstrapServers = defaultBootstrapServers;
        this.allowedBootstrapEndpoints = List.copyOf(allowedBootstrapEndpoints);
        this.adminCredential = adminCredential;
        this.topicDeleteRequestTimeout = topicDeleteRequestTimeout;
    }

    public static MiniMqWebApp start(int port, String defaultBootstrapServers) throws IOException {
        return start(port, defaultBootstrapServers, requiredAdminTokenProperty());
    }

    static String requiredAdminTokenProperty() {
        String token = System.getProperty(ADMIN_TOKEN_PROPERTY);
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException(ADMIN_TOKEN_PROPERTY + " must not be blank");
        }
        return token;
    }

    public static MiniMqWebApp start(
            int port, String defaultBootstrapServers, String adminCredential) throws IOException {
        return start(
                port,
                defaultBootstrapServers,
                adminCredential,
                topicDeleteRequestTimeout(System.getProperty(
                        TOPIC_DELETE_REQUEST_TIMEOUT_PROPERTY,
                        Long.toString(DEFAULT_TOPIC_DELETE_REQUEST_TIMEOUT.toMillis()))));
    }

    public static MiniMqWebApp start(
            int port,
            String defaultBootstrapServers,
            String adminCredential,
            Duration topicDeleteRequestTimeout) throws IOException {
        if (adminCredential == null || adminCredential.isBlank()) {
            throw new IllegalArgumentException("adminCredential must not be blank");
        }
        Duration validatedTopicDeleteRequestTimeout =
                validateTopicDeleteRequestTimeout(topicDeleteRequestTimeout);
        BootstrapServers allowedBootstrap = BootstrapServers.parse(defaultBootstrapServers);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        ExecutorService requestExecutor = Executors.newCachedThreadPool();
        MiniMqWebApp app = new MiniMqWebApp(
                server,
                requestExecutor,
                defaultBootstrapServers,
                allowedBootstrap.endpoints(),
                adminCredential,
                validatedTopicDeleteRequestTimeout);
        server.createContext("/", app::handleIndex);
        server.createContext("/styles.css", exchange -> app.handleStatic(exchange, "web/styles.css", "text/css; charset=utf-8"));
        server.createContext("/app.js", exchange -> app.handleStatic(exchange, "web/app.js", "application/javascript; charset=utf-8"));
        server.createContext("/favicon.ico", app::handleFavicon);
        server.createContext("/api/config", app::handleConfig);
        server.createContext("/api/run-flow", app::handleRunFlow);
        app.registerOperatorRoutes();
        server.setExecutor(requestExecutor);
        server.start();
        return app;
    }

    static Duration topicDeleteRequestTimeout(String configured) {
        long timeoutMs;
        try {
            timeoutMs = Long.parseLong(configured);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    TOPIC_DELETE_REQUEST_TIMEOUT_PROPERTY
                            + " must be a whole number of milliseconds",
                    ex);
        }
        return validateTopicDeleteRequestTimeout(Duration.ofMillis(timeoutMs));
    }

    private static Duration validateTopicDeleteRequestTimeout(Duration timeout) {
        if (timeout == null
                || timeout.compareTo(Duration.ofMillis(1)) < 0
                || timeout.compareTo(MAX_TOPIC_DELETE_REQUEST_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    "topicDeleteRequestTimeout must be between 1 and "
                            + MAX_TOPIC_DELETE_REQUEST_TIMEOUT.toMillis() + " milliseconds");
        }
        return timeout;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        requestExecutor.shutdownNow();
        List<RealFlowRuntime> openRuntimes;
        synchronized (runtimes) {
            openRuntimes = new ArrayList<>(runtimes.values());
            runtimes.clear();
        }
        openRuntimes.forEach(RealFlowRuntime::close);
        cleanupExecutor.shutdown();
    }

    int runtimeCount() {
        synchronized (runtimes) {
            return runtimes.size();
        }
    }

    void closeRuntimeFor(String bootstrapServers, String topic, String groupId) {
        RealFlowRuntime runtime;
        synchronized (runtimes) {
            runtime = runtimes.get(new RealFlowKey(bootstrapServers, topic, groupId));
        }
        if (runtime != null) {
            runtime.close();
        }
    }

    private void handleIndex(HttpExchange exchange) throws IOException {
        if (!isIndexPath(exchange.getRequestURI().getPath())) {
            sendJson(exchange, 404, Map.of("ok", false, "error", "Not found"));
            return;
        }
        handleStatic(exchange, "web/index.html", "text/html; charset=utf-8");
    }

    static boolean isIndexPath(String path) {
        if (path == null) {
            return true;
        }
        String normalized = path.trim();
        return normalized.isEmpty() || "/".equals(normalized);
    }

    private void handleStatic(HttpExchange exchange, String resource, String contentType) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("ok", false, "error", "Method not allowed"));
            return;
        }
        try (InputStream input = MiniMqWebApp.class.getClassLoader().getResourceAsStream(resource)) {
            if (input == null) {
                sendJson(exchange, 404, Map.of("ok", false, "error", "Resource not found"));
                return;
            }
            byte[] body = input.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }

    private void handleFavicon(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("ok", false, "error", "Method not allowed"));
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "image/x-icon");
        exchange.getResponseHeaders().set("Cache-Control", "public, max-age=86400");
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void handleConfig(HttpExchange exchange) throws IOException {
        if (!"/api/config".equals(exchange.getRequestURI().getPath())) {
            sendJson(exchange, 404, Map.of("ok", false, "error", "Not found"));
            return;
        }
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("ok", false, "error", "Method not allowed"));
            return;
        }
        sendJson(exchange, 200, Map.of("bootstrapServers", defaultBootstrapServers));
    }

    private boolean allowJsonPost(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("ok", false, "error", "Method not allowed"));
            return false;
        }
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        String mediaType = contentType == null ? "" : contentType.split(";", 2)[0].trim();
        if (!"application/json".equalsIgnoreCase(mediaType)) {
            sendJson(exchange, 415, Map.of(
                    "ok", false, "error", "Content-Type must be application/json"));
            return false;
        }
        URI requestOrigin = requestOrigin(exchange);
        if (requestOrigin == null || !isAllowedLoopbackRequest(exchange, requestOrigin)) {
            sendJson(exchange, 403, Map.of("ok", false, "error", "Host is not allowed"));
            return false;
        }
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin == null || origin.isBlank() || !isSameRequestOrigin(requestOrigin, origin)) {
            sendJson(exchange, 403, Map.of("ok", false, "error", "Origin is not allowed"));
            return false;
        }
        return true;
    }

    private URI requestOrigin(HttpExchange exchange) {
        try {
            String hostHeader = exchange.getRequestHeaders().getFirst("Host");
            URI requestOrigin = URI.create("http://" + requiredHostHeader(hostHeader));
            return isBareHttpOrigin(requestOrigin) ? requestOrigin : null;
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static boolean isAllowedLoopbackRequest(HttpExchange exchange, URI requestOrigin) {
        String host = requestOrigin.getHost();
        return ("127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host))
                && effectiveHttpPort(requestOrigin) == exchange.getLocalAddress().getPort();
    }

    private static boolean isSameRequestOrigin(URI requestOrigin, String value) {
        try {
            URI origin = URI.create(value.trim());
            return isBareHttpOrigin(origin)
                    && origin.getHost().equalsIgnoreCase(requestOrigin.getHost())
                    && effectiveHttpPort(origin) == effectiveHttpPort(requestOrigin);
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static String requiredHostHeader(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Host header is required");
        }
        return value.trim();
    }

    private static boolean isBareHttpOrigin(URI origin) {
        return "http".equalsIgnoreCase(origin.getScheme())
                && origin.getHost() != null
                && origin.getUserInfo() == null
                && (origin.getRawPath() == null || origin.getRawPath().isEmpty())
                && origin.getRawQuery() == null
                && origin.getRawFragment() == null;
    }

    private static int effectiveHttpPort(URI origin) {
        return origin.getPort() == -1 ? 80 : origin.getPort();
    }

    private void handleRunFlow(HttpExchange exchange) throws IOException {
        if (!allowJsonPost(exchange)) {
            return;
        }
        try {
            FlowRequest request = JSON.readValue(exchange.getRequestBody(), FlowRequest.class)
                    .withDefaults(defaultBootstrapServers);
            request.setBootstrapServers(allowedBootstrapServers(request.bootstrapServers()));
            FlowResult result = switch (request.normalizedMode()) {
                case "real" -> runRealFlow(request);
                case "mock" -> runMockFlow(request, false, null);
                default -> runAutoFlow(request);
            };
            sendJson(exchange, 200, result);
        } catch (JsonProcessingException ex) {
            sendError(exchange, 400, ex);
        } catch (IllegalArgumentException ex) {
            sendError(exchange, 400, ex);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            sendError(exchange, 500, ex);
        }
    }

    private void registerOperatorRoutes() {
        registerOperatorRoute("/api/ops-status", this::opsStatus);
        registerOperatorRoute("/api/group-status", this::groupStatus);
        registerOperatorRoute("/api/topics", this::listTopics);
        registerOperatorRoute("/api/topic/create", this::createTopic);
        registerOperatorRoute("/api/topic/describe", this::describeTopic);
        registerOperatorRoute("/api/topic/delete", this::deleteTopic);
        registerOperatorRoute("/api/topic/increase", this::increasePartitions);
        registerOperatorRoute("/api/topic/alter-config", this::alterTopicConfig);
        registerOperatorRoute("/api/topic/reassign", this::reassignPartitions);
        registerOperatorRoute("/api/topic/elect-preferred", this::electPreferredLeaders);
        registerOperatorRoute("/api/failover-audit", this::failoverAudit);
        registerOperatorRoute("/api/auth-status", this::authStatus);
        registerOperatorRoute("/api/topic/reliability", this::configureTopicReliability);
        registerOperatorRoute("/api/broker/offline", this::markBrokerOffline);
        registerOperatorRoute("/api/runtime-metrics", this::runtimeMetrics);
        registerOperatorRoute("/api/retry-produce", this::retryProduce);
        registerOperatorRoute("/api/dead-letter-produce", this::deadLetterProduce);
        registerOperatorRoute("/api/delayed-produce", this::delayedProduce);
    }

    private void registerOperatorRoute(String path, OperatorAction action) {
        server.createContext(path, exchange -> handleOperator(exchange, action));
    }

    private void handleOperator(HttpExchange exchange, OperatorAction action) throws IOException {
        if (!allowJsonPost(exchange)) {
            return;
        }
        try {
            OperatorRequest request = JSON.readValue(exchange.getRequestBody(), OperatorRequest.class);
            validateOperatorTargets(request);
            Object result = action.execute(request);
            sendJson(exchange, 200, Map.of("ok", true, "result", result));
        } catch (JsonProcessingException ex) {
            sendError(exchange, 400, ex);
        } catch (IllegalArgumentException ex) {
            sendError(exchange, 400, ex);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            sendError(exchange, 500, ex);
        }
    }

    private OpsStatusResponse opsStatus(OperatorRequest request) throws Exception {
        return (OpsStatusResponse) sendBroker(
                request,
                ApiKeys.OPS_STATUS,
                new OpsStatusRequest(
                        adminCredential,
                        clampedOperatorLimit(request.brokerLimit, 100),
                        clampedOperatorLimit(request.partitionLimit, 100)),
                "ops status");
    }

    private GroupStatusResponse groupStatus(OperatorRequest request) throws Exception {
        return (GroupStatusResponse) sendController(
                request,
                ApiKeys.GROUP_STATUS,
                new GroupStatusRequest(
                        adminCredential,
                        required(request.groupId, "groupId"),
                        request.topic,
                        clampedOperatorLimit(request.memberLimit, 100),
                        clampedOperatorLimit(request.partitionLimit, 100)),
                "group status");
    }

    private ListTopicsResponse listTopics(OperatorRequest request) throws Exception {
        return (ListTopicsResponse) sendController(
                request,
                ApiKeys.LIST_TOPICS,
                new ListTopicsRequest(adminCredential, clampedOperatorLimit(request.limit, 100)),
                "list topics");
    }

    private CreateTopicAdminResponse createTopic(OperatorRequest request) throws Exception {
        return (CreateTopicAdminResponse) sendController(
                request,
                ApiKeys.CREATE_TOPIC_ADMIN,
                new CreateTopicAdminRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        orDefault(request.partitions, DEFAULT_TOPIC_PARTITIONS),
                        orDefault(request.replicationFactor, 1)),
                "create topic");
    }

    private DescribeTopicResponse describeTopic(OperatorRequest request) throws Exception {
        return (DescribeTopicResponse) sendController(
                request,
                ApiKeys.DESCRIBE_TOPIC,
                new DescribeTopicRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        clampedOperatorLimit(request.partitionLimit, 100),
                        request.includeDeleted),
                "describe topic");
    }

    private DeleteTopicResponse deleteTopic(OperatorRequest request) throws Exception {
        return (DeleteTopicResponse) sendTopicDelete(
                request,
                new DeleteTopicRequest(adminCredential, required(request.topic, "topic")),
                "delete topic");
    }

    private IncreasePartitionsResponse increasePartitions(OperatorRequest request) throws Exception {
        return (IncreasePartitionsResponse) sendController(
                request,
                ApiKeys.INCREASE_PARTITIONS,
                new IncreasePartitionsRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        required(request.targetPartitionCount, "targetPartitionCount")),
                "increase partitions");
    }

    private AlterTopicConfigResponse alterTopicConfig(OperatorRequest request) throws Exception {
        return (AlterTopicConfigResponse) sendController(
                request,
                ApiKeys.ALTER_TOPIC_CONFIG,
                new AlterTopicConfigRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        required(request.minInSyncReplicas, "minInSyncReplicas")),
                "alter topic config");
    }

    private ReassignPartitionsResponse reassignPartitions(OperatorRequest request) throws Exception {
        return (ReassignPartitionsResponse) sendController(
                request,
                ApiKeys.REASSIGN_PARTITIONS,
                new ReassignPartitionsRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        parseAssignments(required(request.assignment, "assignment"))),
                "reassign partitions");
    }

    private ElectPreferredLeadersResponse electPreferredLeaders(OperatorRequest request)
            throws Exception {
        List<Integer> partitionIds = request.partition == null
                ? List.of()
                : List.of(request.partition);
        return (ElectPreferredLeadersResponse) sendController(
                request,
                ApiKeys.ELECT_PREFERRED_LEADERS,
                new ElectPreferredLeadersRequest(
                        adminCredential,
                        required(request.topic, "topic"),
                        partitionIds),
                "elect preferred leaders");
    }

    private FailoverAuditResponse failoverAudit(OperatorRequest request) throws Exception {
        return (FailoverAuditResponse) sendBroker(
                request,
                ApiKeys.FAILOVER_AUDIT,
                new FailoverAuditRequest(adminCredential, clampedOperatorLimit(request.limit, 100)),
                "failover audit");
    }

    private AuthStatusResponse authStatus(OperatorRequest request) throws Exception {
        return (AuthStatusResponse) sendBroker(
                request,
                ApiKeys.AUTH_STATUS,
                new AuthStatusRequest(adminCredential),
                "auth status");
    }

    private ConfigureTopicReliabilityResponse configureTopicReliability(OperatorRequest request)
            throws Exception {
        return (ConfigureTopicReliabilityResponse) sendController(
                request,
                ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                new ConfigureTopicReliabilityRequest(
                        adminCredential,
                        required(request.sourceTopic, "sourceTopic"),
                        request.retryTopic != null,
                        request.retryTopic,
                        request.clearRetry,
                        request.deadLetterTopic != null,
                        request.deadLetterTopic,
                        request.clearDeadLetter),
                "configure topic reliability");
    }

    private MarkBrokerOfflineAdminResponse markBrokerOffline(OperatorRequest request)
            throws Exception {
        return (MarkBrokerOfflineAdminResponse) sendController(
                request,
                ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                new MarkBrokerOfflineAdminRequest(
                        adminCredential, required(request.brokerId, "brokerId")),
                "mark broker offline");
    }

    private RuntimeMetricsV2Response runtimeMetrics(OperatorRequest request) throws Exception {
        return (RuntimeMetricsV2Response) sendBroker(
                request,
                ApiKeys.RUNTIME_METRICS_V2,
                new RuntimeMetricsV2Request(
                        adminCredential,
                        clampedOperatorLimit(request.counterLimit, 100),
                        clampedOperatorLimit(request.latencyLimit, 100),
                        clampedOperatorLimit(request.lagLimit, 100)),
                "runtime metrics");
    }

    private Object retryProduce(OperatorRequest request) throws Exception {
        String sourceTopic = required(request.sourceTopic, "sourceTopic");
        try (Producer producer = new Producer(bootstrapServers(request), "web-retry-producer")) {
            producer.start();
            return producer.sendToRetry(
                    sourceTopic,
                    producerRecord(sourceTopic, request),
                    required(request.deliverAtEpochMs, "deliverAtEpochMs"));
        }
    }

    private Object deadLetterProduce(OperatorRequest request) throws Exception {
        String sourceTopic = required(request.sourceTopic, "sourceTopic");
        try (Producer producer = new Producer(bootstrapServers(request), "web-dead-letter-producer")) {
            producer.start();
            return producer.sendToDeadLetter(sourceTopic, producerRecord(sourceTopic, request));
        }
    }

    private Object delayedProduce(OperatorRequest request) throws Exception {
        String topic = required(request.topic, "topic");
        try (Producer producer = new Producer(bootstrapServers(request), "web-delayed-producer")) {
            producer.start();
            return producer.sendDelayed(
                    producerRecord(topic, request),
                    required(request.deliverAtEpochMs, "deliverAtEpochMs"));
        }
    }

    private Object sendController(
            OperatorRequest request, short apiKey, Object body, String operation) throws Exception {
        try (ControllerAwareNetworkClient client =
                     new ControllerAwareNetworkClient(bootstrapServers(request), "web-controller-operator")) {
            client.connect();
            MqDecoder.DecodedResponse response = client.send(apiKey, body);
            client.ensureSuccess(response, operation);
            return response.body();
        }
    }

    private Object sendTopicDelete(
            OperatorRequest request, DeleteTopicRequest body, String operation) throws Exception {
        bootstrapServers(request);
        for (int attempt = 0; attempt < 2; attempt++) {
            DiscoverControllerResponse discovered = discoverControllerForDelete();
            try (NetworkClient client = new NetworkClient(
                    "web-controller-delete",
                    NetworkClient.RetryPolicy.fixedBackoff(1, 0),
                    topicDeleteRequestTimeout)) {
                client.connect(discovered.controller().host(), discovered.controller().port());
                MqDecoder.DecodedResponse response = client.send(ApiKeys.DELETE_TOPIC, body);
                if (isStaleController(response.header().errorCode()) && attempt == 0) {
                    continue;
                }
                client.ensureSuccess(response, operation);
                return response.body();
            }
        }
        throw new IllegalStateException("delete topic failed after Controller rediscovery");
    }

    private DiscoverControllerResponse discoverControllerForDelete() throws InterruptedException {
        RuntimeException lastFailure = null;
        for (BootstrapServers.Endpoint endpoint : allowedBootstrapEndpoints) {
            try (NetworkClient client = new NetworkClient("web-controller-delete-bootstrap")) {
                client.connect(endpoint.host(), endpoint.port());
                MqDecoder.DecodedResponse response = client.send(
                        ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
                client.ensureSuccess(response, "DISCOVER_CONTROLLER");
                return (DiscoverControllerResponse) response.body();
            } catch (RuntimeException ex) {
                lastFailure = ex;
            }
        }
        throw new MiniMqConnectionException(
                "Failed to discover Controller from all allowed bootstrap servers", lastFailure);
    }

    private Object sendBroker(
            OperatorRequest request, short apiKey, Object body, String operation) throws Exception {
        BrokerEndpoint endpoint = parseBrokerEndpoint(request.broker);
        try (NetworkClient client = new NetworkClient("web-broker-operator")) {
            client.connect(endpoint.host(), endpoint.port());
            MqDecoder.DecodedResponse response = client.send(apiKey, body);
            client.ensureSuccess(response, operation);
            return response.body();
        }
    }

    private String bootstrapServers(OperatorRequest request) {
        return allowedBootstrapServers(request.bootstrapServers);
    }

    private void validateOperatorTargets(OperatorRequest request) {
        if (request.bootstrapServers != null) {
            allowedBootstrapServers(request.bootstrapServers);
        }
        if (request.broker != null) {
            parseBrokerEndpoint(request.broker);
        }
    }

    private String allowedBootstrapServers(String value) {
        if (value == null || value.isBlank()) {
            return defaultBootstrapServers;
        }
        List<BootstrapServers.Endpoint> requested = BootstrapServers.parse(value).endpoints();
        if (!requested.equals(allowedBootstrapEndpoints)) {
            throw new IllegalArgumentException(
                    "bootstrapServers must match the server-configured bootstrap servers");
        }
        return defaultBootstrapServers;
    }

    private BrokerEndpoint parseBrokerEndpoint(String value) {
        if (value == null || value.isBlank() || value.contains(",")) {
            throw new IllegalArgumentException(
                    "broker must be an explicit single broker endpoint (host:port)");
        }
        List<BootstrapServers.Endpoint> parsed;
        try {
            parsed = BootstrapServers.parse(value).endpoints();
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    "broker must be an explicit single broker endpoint (host:port)", ex);
        }
        BootstrapServers.Endpoint endpoint = parsed.get(0);
        if (!allowedBootstrapEndpoints.contains(endpoint)) {
            throw new IllegalArgumentException(
                    "broker must be a server-configured bootstrap endpoint");
        }
        return new BrokerEndpoint(endpoint.host(), endpoint.port());
    }

    private static boolean isStaleController(ErrorCode errorCode) {
        return errorCode == ErrorCode.NOT_CONTROLLER || errorCode == ErrorCode.CONTROLLER_FENCED;
    }

    private static ProducerRecord producerRecord(String topic, OperatorRequest request) {
        byte[] key = request.key == null ? null : request.key.getBytes(StandardCharsets.UTF_8);
        String message = required(request.message, "message");
        return new ProducerRecord(topic, null, key, message.getBytes(StandardCharsets.UTF_8));
    }

    private static int orDefault(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    static int clampedOperatorLimit(Integer value, int defaultValue) {
        int resolved = orDefault(value, defaultValue);
        if (resolved < MIN_OPERATOR_LIMIT) {
            return MIN_OPERATOR_LIMIT;
        }
        return Math.min(MAX_OPERATOR_LIMIT, resolved);
    }

    static long consumeWaitMs() {
        String configured = System.getProperty(CONSUME_WAIT_MS_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_CONSUME_WAIT_MS;
        }
        long waitMs;
        try {
            waitMs = Long.parseLong(configured);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    CONSUME_WAIT_MS_PROPERTY + " must be a whole number of milliseconds",
                    ex);
        }
        return Math.min(MAX_CONSUME_WAIT_MS, Math.max(MIN_CONSUME_WAIT_MS, waitMs));
    }

    private static <T> T required(T value, String field) {
        if (value == null || value instanceof String text && text.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static List<PartitionReplicaAssignment> parseAssignments(String assignment) {
        List<PartitionReplicaAssignment> assignments = new ArrayList<>();
        for (String token : assignment.split("[;\\s]+")) {
            if (token.isBlank()) {
                continue;
            }
            assignments.add(parseAssignment(token));
        }
        if (assignments.isEmpty()) {
            throw new IllegalArgumentException("assignment is required");
        }
        return assignments;
    }

    private static PartitionReplicaAssignment parseAssignment(String value) {
        if (!value.contains(":")) {
            throw new IllegalArgumentException("assignment must be partitionId:brokerId[,brokerId...]");
        }
        String[] parts = value.split(":", 2);
        int partitionId;
        try {
            partitionId = Integer.parseInt(parts[0].trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("assignment partition id must be an integer", ex);
        }
        List<Integer> replicas = new ArrayList<>();
        for (String token : parts[1].split(",")) {
            if (token.isBlank()) {
                continue;
            }
            try {
                replicas.add(Integer.parseInt(token.trim()));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("assignment replica ids must be integers", ex);
            }
        }
        if (replicas.isEmpty()) {
            throw new IllegalArgumentException("assignment must include replica ids");
        }
        return new PartitionReplicaAssignment(partitionId, replicas);
    }

    private FlowResult runAutoFlow(FlowRequest request) throws Exception {
        try {
            return runRealFlow(request);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (FlowSideEffectException ex) {
            throw ex;
        } catch (Exception ex) {
            return runMockFlow(request, true, ex.getClass().getSimpleName() + ": " + safeError(ex));
        }
    }

    private FlowResult runRealFlow(FlowRequest request) throws Exception {
        RealFlowKey key = keyFor(request);
        RealFlowRuntime runtime = runtimeFor(key, request);
        try {
            return runtime.run(request);
        } catch (InterruptedException interrupted) {
            evictRuntime(key, runtime);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (StaleRuntimeException stale) {
            evictRuntime(key, runtime);
            return runtimeFor(key, request).run(request);
        } catch (Exception ex) {
            evictRuntime(key, runtime);
            throw ex;
        }
    }

    private RealFlowRuntime runtimeFor(RealFlowKey key, FlowRequest request) throws Exception {
        synchronized (runtimes) {
            RealFlowRuntime runtime = runtimes.get(key);
            if (runtime != null) {
                return runtime;
            }
        }

        RealFlowRuntime opened = RealFlowRuntime.open(request, adminCredential);
        List<RealFlowRuntime> evicted = new ArrayList<>();
        synchronized (runtimes) {
            RealFlowRuntime existing = runtimes.get(key);
            if (existing != null) {
                opened.close();
                return existing;
            }
            runtimes.put(key, opened);
            while (runtimes.size() > MAX_RUNTIMES) {
                RealFlowKey eldestKey = runtimes.keySet().iterator().next();
                evicted.add(runtimes.remove(eldestKey));
            }
        }
        evicted.forEach(this::closeRuntimeAsync);
        return opened;
    }

    private void evictRuntime(RealFlowKey key, RealFlowRuntime expected) {
        RealFlowRuntime removed = null;
        synchronized (runtimes) {
            RealFlowRuntime current = runtimes.get(key);
            if (current == expected) {
                removed = runtimes.remove(key);
            }
        }
        if (removed != null) {
            closeRuntimeAsync(removed);
        }
    }

    private void closeRuntimeAsync(RealFlowRuntime runtime) {
        try {
            cleanupExecutor.execute(runtime::close);
        } catch (RejectedExecutionException ignored) {
            runtime.close();
        }
    }

    private static RealFlowKey keyFor(FlowRequest request) {
        return new RealFlowKey(request.bootstrapServers(), request.topic(), request.groupId());
    }

    private static boolean topicExists(ControllerAwareNetworkClient client, String topic)
            throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(ApiKeys.METADATA, new MetadataRequest(topic));
        client.ensureSuccess(response);
        MetadataResponse metadata = (MetadataResponse) response.body();
        return metadata.topics().stream().anyMatch(entry -> entry.topicName().equals(topic));
    }

    private FlowResult runMockFlow(FlowRequest request, boolean fallback, String reason) {
        return new FlowResult(true, "mock", fallback, request.topic(), 0,
                request.message(), fallback
                        ? "Broker unavailable; switched to mock main flow"
                        : "Mock main flow completed",
                reason);
    }

    private record RealFlowKey(String bootstrapServers, String topic, String groupId) {
    }

    private record BrokerEndpoint(String host, int port) {
    }

    private static final class StaleRuntimeException extends IllegalStateException {
        private StaleRuntimeException() {
            super("Runtime is closed");
        }
    }

    private static final class FlowSideEffectException extends Exception {
        private FlowSideEffectException(long offset, Throwable cause) {
            super(offset >= 0L
                    ? "Real flow failed after PRODUCE completed at offset " + offset
                    : "Real flow failed after an external side effect was attempted", cause);
        }
    }

    @FunctionalInterface
    private interface OperatorAction {
        Object execute(OperatorRequest request) throws Exception;
    }

    private static final class RealFlowRuntime implements AutoCloseable {
        private final ControllerAwareNetworkClient adminClient;
        private final Producer producer;
        private final Consumer consumer;
        private final String adminCredential;
        private boolean topicReady;
        private boolean topicCreateAttempted;
        private boolean consumerReady;
        private boolean closed;

        private RealFlowRuntime(
                ControllerAwareNetworkClient adminClient,
                Producer producer,
                Consumer consumer,
                String adminCredential) {
            this.adminClient = adminClient;
            this.producer = producer;
            this.consumer = consumer;
            this.adminCredential = adminCredential;
        }

        static RealFlowRuntime open(FlowRequest request, String adminCredential) throws Exception {
            ControllerAwareNetworkClient adminClient = null;
            Producer producer = null;
            Consumer consumer = null;
            try {
                adminClient = new ControllerAwareNetworkClient(
                        request.bootstrapServers(), "web-admin-" + request.topic());
                adminClient.connect();
                producer = new Producer(request.bootstrapServers(), "web-producer-" + request.topic());
                producer.start();
                consumer = new Consumer(request.bootstrapServers(), request.groupId(), "web-consumer-" + request.topic());
                return new RealFlowRuntime(adminClient, producer, consumer, adminCredential);
            } catch (Exception ex) {
                closeQuietly(consumer);
                closeQuietly(producer);
                closeQuietly(adminClient);
                throw ex;
            }
        }

        synchronized FlowResult run(FlowRequest request) throws Exception {
            if (closed) {
                throw new StaleRuntimeException();
            }
            ensureTopic(request);
            if (!consumerReady) {
                try {
                    consumer.subscribe(request.topic());
                } catch (InterruptedException interrupted) {
                    throw interrupted;
                } catch (RuntimeException failure) {
                    throw new FlowSideEffectException(-1L, failure);
                }
                consumerReady = true;
            }
            long offset;
            try {
                offset = producer.send(new ProducerRecord(
                        request.topic(), null,
                        request.key().getBytes(StandardCharsets.UTF_8),
                        request.message().getBytes(StandardCharsets.UTF_8))).offset();
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (RuntimeException failure) {
                throw new FlowSideEffectException(-1L, failure);
            }
            try {
                ConsumerRecord consumed = pollProducedRecord(offset);
                consumer.commitSync(consumed.topic(), consumed.partition(), consumed.offset() + 1);
                return new FlowResult(true, "real", false, request.topic(), offset,
                        new String(consumed.value(), StandardCharsets.UTF_8), "MiniMQ main flow completed", null);
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception failure) {
                throw new FlowSideEffectException(offset, failure);
            }
        }

        private ConsumerRecord pollProducedRecord(long offset) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(consumeWaitMs());
            do {
                List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(50));
                for (ConsumerRecord record : records) {
                    if (record.offset() == offset) {
                        return record;
                    }
                }
            } while (System.nanoTime() < deadline);
            throw new IllegalStateException("Produced message was not consumed");
        }

        private void ensureTopic(FlowRequest request)
                throws InterruptedException, FlowSideEffectException {
            if (topicReady || topicExists(adminClient, request.topic())) {
                topicReady = true;
                return;
            }
            topicCreateAttempted = true;
            try {
                MqDecoder.DecodedResponse response = adminClient.send(
                        ApiKeys.CREATE_TOPIC_ADMIN,
                        new CreateTopicAdminRequest(adminCredential, request.topic(), request.partitions(), 1));
                adminClient.ensureSuccess(response);
                topicReady = true;
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (RuntimeException failure) {
                throw new FlowSideEffectException(-1L, failure);
            }
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                consumer.close();
            } catch (Exception e) {
                restoreInterruptIfNeeded(e);
            }
            try {
                producer.close();
            } catch (Exception e) {
                restoreInterruptIfNeeded(e);
            }
            try {
                adminClient.close();
            } catch (Exception e) {
                restoreInterruptIfNeeded(e);
            }
        }

        private static void closeQuietly(AutoCloseable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (Exception ex) {
                // Best-effort cleanup after partial runtime initialization.
            }
        }

        private static void restoreInterruptIfNeeded(Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private void sendError(HttpExchange exchange, int status, Throwable failure) throws IOException {
        sendJson(exchange, status, Map.of("ok", false, "error", safeError(failure)));
    }

    private String safeError(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            message = failure.getClass().getSimpleName();
        }
        message = message.replace(adminCredential, "[redacted]")
                .replace('\r', ' ')
                .replace('\n', ' ');
        return message.length() <= MAX_ERROR_CHARS
                ? message
                : message.substring(0, MAX_ERROR_CHARS);
    }

    public static final class OperatorRequest {
        private String bootstrapServers;
        private String broker;
        private String topic;
        private String sourceTopic;
        private String groupId;
        private Integer partitions;
        private Integer replicationFactor;
        private Integer limit;
        private Integer brokerLimit;
        private Integer partitionLimit;
        private Integer memberLimit;
        private Integer counterLimit;
        private Integer latencyLimit;
        private Integer lagLimit;
        private boolean includeDeleted;
        private Integer targetPartitionCount;
        private Integer minInSyncReplicas;
        private String assignment;
        private Integer partition;
        private String retryTopic;
        private String deadLetterTopic;
        private boolean clearRetry;
        private boolean clearDeadLetter;
        private Integer brokerId;
        private String key;
        private String message;
        private Long deliverAtEpochMs;

        public void setBootstrapServers(String value) { bootstrapServers = value; }
        public void setBroker(String value) { broker = value; }
        public void setTopic(String value) { topic = value; }
        public void setSourceTopic(String value) { sourceTopic = value; }
        public void setGroupId(String value) { groupId = value; }
        public void setPartitions(Integer value) { partitions = value; }
        public void setReplicationFactor(Integer value) { replicationFactor = value; }
        public void setLimit(Integer value) { limit = value; }
        public void setBrokerLimit(Integer value) { brokerLimit = value; }
        public void setPartitionLimit(Integer value) { partitionLimit = value; }
        public void setMemberLimit(Integer value) { memberLimit = value; }
        public void setCounterLimit(Integer value) { counterLimit = value; }
        public void setLatencyLimit(Integer value) { latencyLimit = value; }
        public void setLagLimit(Integer value) { lagLimit = value; }
        public void setIncludeDeleted(boolean value) { includeDeleted = value; }
        public void setTargetPartitionCount(Integer value) { targetPartitionCount = value; }
        public void setTargetPartitions(Integer value) { targetPartitionCount = value; }
        public void setMinInSyncReplicas(Integer value) { minInSyncReplicas = value; }
        public void setAssignment(String value) { assignment = value; }
        public void setPartition(Integer value) { partition = value; }
        public void setRetryTopic(String value) { retryTopic = value; }
        public void setDeadLetterTopic(String value) { deadLetterTopic = value; }
        public void setClearRetry(boolean value) { clearRetry = value; }
        public void setClearDeadLetter(boolean value) { clearDeadLetter = value; }
        public void setBrokerId(Integer value) { brokerId = value; }
        public void setKey(String value) { key = value; }
        public void setMessage(String value) { message = value; }
        public void setDeliverAtEpochMs(Long value) { deliverAtEpochMs = value; }
    }

    public static final class FlowRequest {
        private String mode;
        private String bootstrapServers;
        private String topic;
        private int partitions;
        private String key;
        private String message;
        private String groupId;

        public FlowRequest() {
        }

        FlowRequest withDefaults(String defaultBootstrapServers) {
            if (mode == null || mode.isBlank()) {
                mode = "auto";
            }
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                bootstrapServers = defaultBootstrapServers;
            }
            if (topic == null || topic.isBlank()) {
                topic = "web-orders";
            }
            if (partitions <= 0) {
                partitions = DEFAULT_TOPIC_PARTITIONS;
            }
            if (key == null) {
                key = "web-key";
            }
            if (message == null || message.isBlank()) {
                message = "hello-minimq";
            }
            if (groupId == null || groupId.isBlank()) {
                groupId = "web-group";
            }
            return this;
        }

        String normalizedMode() {
            String value = mode.toLowerCase(Locale.ROOT);
            return value.equals("real") || value.equals("mock") ? value : "auto";
        }

        public String mode() { return mode; }
        public String bootstrapServers() { return bootstrapServers; }
        public String topic() { return topic; }
        public int partitions() { return partitions; }
        public String key() { return key; }
        public String message() { return message; }
        public String groupId() { return groupId; }

        public void setMode(String mode) { this.mode = mode; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public void setTopic(String topic) { this.topic = topic; }
        public void setPartitions(int partitions) { this.partitions = partitions; }
        public void setKey(String key) { this.key = key; }
        public void setMessage(String message) { this.message = message; }
        public void setGroupId(String groupId) { this.groupId = groupId; }
    }

    public record FlowResult(
            boolean ok,
            String mode,
            boolean fallback,
            String topic,
            long offset,
            String consumedValue,
            String message,
            String reason) {
    }
}

package com.minimq.demo.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minimq.client.Consumer;
import com.minimq.client.NetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import com.minimq.protocol.message.ProtocolMessages.MetadataResponse;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * MiniMQ 前端网关，提供静态 Brutalism 页面，并把主流程按钮映射到真实或 mock 的 HTTP API。
 */
public final class MiniMqWebApp implements AutoCloseable {
    static final int MAX_RUNTIMES = 4;

    private static final ObjectMapper JSON = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final HttpServer server;
    private final String defaultBootstrapServers;
    private final String adminCredential;
    private final ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "minimq-web-runtime-cleanup");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<RealFlowKey, RealFlowRuntime> runtimes = new LinkedHashMap<>(16, 0.75f, true);

    private MiniMqWebApp(HttpServer server, String defaultBootstrapServers, String adminCredential) {
        this.server = server;
        this.defaultBootstrapServers = defaultBootstrapServers;
        this.adminCredential = adminCredential;
    }

    public static MiniMqWebApp start(int port, String defaultBootstrapServers) throws IOException {
        return start(
                port,
                defaultBootstrapServers,
                System.getProperty("minimq.admin.token", "web-dev-admin"));
    }

    public static MiniMqWebApp start(
            int port, String defaultBootstrapServers, String adminCredential) throws IOException {
        if (adminCredential == null || adminCredential.isBlank()) {
            throw new IllegalArgumentException("adminCredential must not be blank");
        }
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        MiniMqWebApp app = new MiniMqWebApp(server, defaultBootstrapServers, adminCredential);
        server.createContext("/", app::handleIndex);
        server.createContext("/styles.css", exchange -> app.handleStatic(exchange, "web/styles.css", "text/css; charset=utf-8"));
        server.createContext("/app.js", exchange -> app.handleStatic(exchange, "web/app.js", "application/javascript; charset=utf-8"));
        server.createContext("/favicon.ico", app::handleFavicon);
        server.createContext("/api/run-flow", app::handleRunFlow);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return app;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
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
        if (!"/".equals(exchange.getRequestURI().getPath())) {
            sendJson(exchange, 404, Map.of("ok", false, "error", "Not found"));
            return;
        }
        handleStatic(exchange, "web/index.html", "text/html; charset=utf-8");
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

    private void handleRunFlow(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("ok", false, "error", "Method not allowed"));
            return;
        }
        try {
            FlowRequest request = JSON.readValue(exchange.getRequestBody(), FlowRequest.class)
                    .withDefaults(defaultBootstrapServers);
            FlowResult result = switch (request.normalizedMode()) {
                case "real" -> runRealFlow(request);
                case "mock" -> runMockFlow(request, false, null);
                default -> runAutoFlow(request);
            };
            sendJson(exchange, 200, result);
        } catch (IllegalArgumentException ex) {
            sendJson(exchange, 400, Map.of("ok", false, "error", ex.getMessage()));
        } catch (Exception ex) {
            sendJson(exchange, 500, Map.of("ok", false, "error", ex.getMessage()));
        }
    }

    private FlowResult runAutoFlow(FlowRequest request) {
        try {
            return runRealFlow(request);
        } catch (Exception ex) {
            return runMockFlow(request, true, ex.getClass().getSimpleName() + ": " + ex.getMessage());
        }
    }

    private FlowResult runRealFlow(FlowRequest request) throws Exception {
        RealFlowKey key = keyFor(request);
        RealFlowRuntime runtime = runtimeFor(key, request);
        try {
            return runtime.run(request);
        } catch (Exception ex) {
            evictRuntime(key, runtime);
            return runtimeFor(key, request).run(request);
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
        cleanupExecutor.execute(runtime::close);
    }

    private static RealFlowKey keyFor(FlowRequest request) {
        return new RealFlowKey(request.bootstrapServers(), request.topic(), request.groupId());
    }

    private static boolean topicExists(NetworkClient client, String topic) throws InterruptedException {
        MqDecoder.DecodedResponse response = client.send(ApiKeys.METADATA, new MetadataRequest(topic));
        client.ensureSuccess(response);
        MetadataResponse metadata = (MetadataResponse) response.body();
        return metadata.topics().stream().anyMatch(entry -> entry.topicName().equals(topic));
    }

    private FlowResult runMockFlow(FlowRequest request, boolean fallback, String reason) {
        return new FlowResult(true, "mock", fallback, request.topic(), 0,
                request.message(), fallback ? "Broker 不可用，已切换 mock 主流程" : "mock 主流程通过", reason);
    }

    private static String[] splitBootstrap(String bootstrapServers) {
        String[] parts = bootstrapServers.split(",", 2)[0].split(":", 2);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must contain host:port");
        }
        return parts;
    }

    private record RealFlowKey(String bootstrapServers, String topic, String groupId) {
    }

    private static final class RealFlowRuntime implements AutoCloseable {
        private final NetworkClient adminClient;
        private final Producer producer;
        private final Consumer consumer;
        private final String adminCredential;
        private boolean topicReady;
        private boolean consumerReady;
        private boolean closed;

        private RealFlowRuntime(
                NetworkClient adminClient,
                Producer producer,
                Consumer consumer,
                String adminCredential) {
            this.adminClient = adminClient;
            this.producer = producer;
            this.consumer = consumer;
            this.adminCredential = adminCredential;
        }

        static RealFlowRuntime open(FlowRequest request, String adminCredential) throws Exception {
            String[] hostPort = splitBootstrap(request.bootstrapServers());
            NetworkClient adminClient = null;
            Producer producer = null;
            Consumer consumer = null;
            try {
                adminClient = new NetworkClient("web-admin-" + request.topic());
                adminClient.connect(hostPort[0], Integer.parseInt(hostPort[1]));
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
                throw new IllegalStateException("Runtime is closed");
            }
            ensureTopic(request);
            if (!consumerReady) {
                consumer.subscribe(request.topic());
                consumerReady = true;
            }
            long offset = producer.send(new ProducerRecord(
                    request.topic(), null,
                    request.key().getBytes(StandardCharsets.UTF_8),
                    request.message().getBytes(StandardCharsets.UTF_8))).offset();
            ConsumerRecord consumed = pollProducedRecord(offset);
            consumer.commitSync(consumed.topic(), consumed.partition(), consumed.offset() + 1);
            return new FlowResult(true, "real", false, request.topic(), offset,
                    new String(consumed.value(), StandardCharsets.UTF_8), "真实 MiniMQ 主流程通过", null);
        }

        private ConsumerRecord pollProducedRecord(long offset) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(750);
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

        private void ensureTopic(FlowRequest request) throws InterruptedException {
            if (topicReady || topicExists(adminClient, request.topic())) {
                topicReady = true;
                return;
            }
            MqDecoder.DecodedResponse response = adminClient.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(adminCredential, request.topic(), request.partitions(), 1));
            adminClient.ensureSuccess(response);
            topicReady = true;
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
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
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
                partitions = 1;
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

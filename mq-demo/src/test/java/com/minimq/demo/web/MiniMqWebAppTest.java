package com.minimq.demo.web;

import com.minimq.broker.support.BrokerTestHarness;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniMqWebAppTest {
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void rejectsBlankAdminCredential() {
        assertThrows(IllegalArgumentException.class,
                () -> MiniMqWebApp.start(0, "127.0.0.1:65535", " "));
    }

    @Test
    void servesBrutalistFrontendAssets() throws Exception {
        try (MiniMqWebApp app = MiniMqWebApp.start(0, "127.0.0.1:65535")) {
            String baseUrl = "http://127.0.0.1:" + app.port();

            HttpResponse<String> html = get(baseUrl + "/");
            HttpResponse<String> css = get(baseUrl + "/styles.css");
            HttpResponse<String> js = get(baseUrl + "/app.js");
            HttpResponse<String> favicon = get(baseUrl + "/favicon.ico");

            assertEquals(200, html.statusCode());
            assertTrue(html.body().contains("MiniMQ FRONT PANEL"));
            assertTrue(html.body().contains("data-testid=\"run-flow\""));
            assertTrue(html.body().contains("value=\"127.0.0.1:19092\""));
            assertEquals(200, css.statusCode());
            assertTrue(css.body().contains("border: 3px solid #000"));
            assertTrue(css.body().contains("border-radius: 0"));
            assertEquals(200, js.statusCode());
            assertTrue(js.body().contains("runMainFlow"));
            assertEquals(204, favicon.statusCode());
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
            assertEquals(500, badJson.statusCode());
            assertTrue(badJson.body().contains("\"ok\":false"));
            assertEquals(500, invalidRealBroker.statusCode());
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
        assertEquals(1, normalized.partitions());
        assertEquals("web-key", normalized.key());
        assertEquals("hello-minimq", normalized.message());
        assertEquals("web-group", normalized.groupId());
        assertEquals("invalid", normalized.mode());
    }

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String url, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(8))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJsonUnchecked(String url, String body) {
        try {
            return postJson(url, body);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

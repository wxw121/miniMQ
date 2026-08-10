package com.minimq.demo.web;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniMqWebDevMainTest {
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void startsBrokerAndWebInOneProcessForLocalDevelopment() throws Exception {
        MiniMqWebDevMain.DevConfig config = new MiniMqWebDevMain.DevConfig(
                "127.0.0.1",
                0,
                0,
                Files.createTempDirectory("minimq-web-dev-test-"));

        try (MiniMqWebDevMain launcher = MiniMqWebDevMain.start(config)) {
            String baseUrl = "http://127.0.0.1:" + launcher.webPort();
            HttpResponse<String> html = get(baseUrl + "/");
            HttpResponse<String> flow = postJson(baseUrl + "/api/run-flow", """
                    {
                      "mode": "real",
                      "topic": "dev-launcher-orders",
                      "partitions": 1,
                      "key": "dev-key",
                      "message": "hello-dev-launcher",
                      "groupId": "dev-launcher-group"
                    }
                    """);

            assertEquals(200, html.statusCode());
            assertTrue(html.body().contains("MiniMQ Operator Console"));
            assertEquals(200, flow.statusCode());
            assertTrue(flow.body().contains("\"mode\":\"real\""));
            assertTrue(flow.body().contains("\"consumedValue\":\"hello-dev-launcher\""));
            assertTrue(Files.exists(Path.of(launcher.dataDir().toString())));
        }
    }

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String url, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}

package com.minimq.demo;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoCliTest {

    @Test
    void printsUsageForNoArgsAndUnknownCommand() throws Exception {
        String noArgs = captureStdout(() -> DemoCli.main(new String[0]));
        String unknown = captureStdout(() -> DemoCli.main(new String[] {"unknown"}));

        assertTrue(noArgs.contains("Usage:"));
        assertTrue(unknown.contains("Usage:"));
        assertTrue(noArgs.contains("benchmark-fetch"));
        assertTrue(unknown.contains("benchmark-fetch"));
        assertTrue(noArgs.contains("maintenance-report"));
        assertTrue(unknown.contains("maintenance-metrics"));
        assertTrue(noArgs.contains("--broker localhost:9092 --admin-token"));
    }

    @Test
    void rejectsInvalidBrokerAddressesBeforeNetworkUse() {
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "create-topic", "--brokers", "localhost", "--name", "orders"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "produce", "--brokers", "localhost", "--topic", "orders", "--message", "hello"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "consume", "--brokers", "localhost", "--topic", "orders", "--group", "g1"
        }));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.parseSingleHostPort("localhost"));
    }

    @Test
    void brokerCommandPropagatesConfigValidation() {
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "broker", "--controller-addr", "localhost"
        }));
    }

    @Test
    void brokerMainParsesConfigWithoutStartingServer() {
        var config = BrokerMain.parseConfig(new String[] {
                "--broker-id", "3",
                "--host", "127.0.0.1",
                "--port", "19092",
                "--data-dir", "./tmp/broker-3",
                "--controller",
                "--admin-token", "test-admin-token"
        });

        assertEquals(3, config.brokerId());
        assertEquals("127.0.0.1", config.host());
        assertEquals(19092, config.port());
        assertTrue(config.controller());
    }

    @Test
    void parsesCliHelperValues() {
        String[] args = {
                "produce", "--brokers", "host1:9092,host2:9093", "--topic", "orders", "--message", "hello"
        };

        assertEquals("orders", DemoCli.arg(args, "--topic", "fallback"));
        assertEquals("fallback", DemoCli.arg(args, "--missing", "fallback"));
        assertArrayEquals(new String[] {"host1", "9092"},
                DemoCli.parseFirstHostPort(DemoCli.arg(args, "--brokers", "localhost:9092")));
        assertArrayEquals(new String[] {"--brokers", "host1:9092,host2:9093", "--topic", "orders", "--message", "hello"},
                DemoCli.copyTail(args));
    }

    @Test
    void rejectsMissingRequiredCliValues() {
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.arg(new String[] {"produce"}, "--message", null));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.arg(new String[] {"produce", "--message"}, "--message", null));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.arg(new String[] {"produce", "--message", "--topic"}, "--message", null));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.parseFirstHostPort("localhost"));
    }

    @Test
    void maintenanceCommandsRequireOneExplicitBrokerAndToken() {
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "maintenance-report", "--admin-token", "secret"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "maintenance-report", "--broker", "127.0.0.1:9092"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "maintenance-report", "--broker", "a:9092,b:9093", "--admin-token", "secret"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "maintenance-report", "--broker", "localhost", "--admin-token", "secret"
        }));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.parseSingleHostPort("localhost:0"));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.parseSingleHostPort("localhost:65536"));
    }

    @Test
    void createProduceAndConsumeThroughCli() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("demo-cli");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir)) {
            String brokers = broker.bootstrapServers();

            String createOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "create-topic", "--brokers", brokers, "--name", "orders", "--partitions", "1"
            }));
            String produceOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "produce", "--brokers", brokers, "--topic", "orders", "--key", "user-1", "--message", "hello"
            }));
            String consumeOutput = captureStdout(() -> DemoCli.consumeForPolls(new String[] {
                    "consume", "--brokers", brokers, "--topic", "orders", "--group", "demo-cli-group"
            }, 1));

            assertTrue(createOutput.contains("Created topic: orders"));
            assertTrue(produceOutput.contains("Produced to orders-0 offset=0"));
            assertTrue(consumeOutput.contains("Consumed orders-0 offset=0 value=hello"));
        }
    }

    @Test
    void maintenanceCommandsRenderBrokerReportsAndMetrics() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("demo-cli-maintenance");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(dataDir)) {
            String brokers = broker.bootstrapServers();

            MiniMqServerException unauthorized = assertThrows(
                    MiniMqServerException.class,
                    () -> captureStdout(() -> DemoCli.main(new String[] {
                            "maintenance-report", "--broker", brokers,
                            "--admin-token", "wrong-token"
                    })));
            assertEquals(ErrorCode.UNAUTHORIZED, unauthorized.errorCode());

            String reportOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "maintenance-report", "--broker", brokers,
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));
            String runOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "maintenance-run", "--broker", brokers,
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));
            String metricsOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "maintenance-metrics", "--broker", brokers,
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));

            assertTrue(reportOutput.contains("Maintenance target: " + brokers));
            assertTrue(reportOutput.contains("Maintenance report"));
            assertTrue(reportOutput.contains("dryRun=true"));
            assertTrue(reportOutput.contains("status=SKIPPED_DISABLED"));
            assertTrue(runOutput.contains("Maintenance report"));
            assertTrue(runOutput.contains("dryRun=false"));
            assertTrue(metricsOutput.contains("Maintenance metrics"));
            assertTrue(metricsOutput.contains("dryRuns=1"));
            assertTrue(metricsOutput.contains("lastRunStatus=SKIPPED_DISABLED"));
            assertFalse(reportOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(runOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(metricsOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
        }
    }

    @Test
    void maintenanceReportShowsRetentionSegmentsThroughRealBroker() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("demo-cli-retention");
        BrokerConfig config = BrokerTestHarness.controllerConfig(
                0,
                dataDir,
                "--segment-max-bytes", "128",
                "--retention-bytes", "256");
        try (BrokerTestHarness broker = BrokerTestHarness.start(config)) {
            captureStdout(() -> createTopicAndProduceRolledSegments(
                    broker.bootstrapServers(), "retained", "shared-key", 6));

            String output = captureStdout(() -> DemoCli.main(new String[] {
                    "maintenance-report",
                    "--broker", broker.bootstrapServers(),
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));

            assertTrue(output.contains("Maintenance target: " + broker.bootstrapServers()));
            assertTrue(output.contains("segments="));
            assertTrue(output.contains("action=RETENTION_DELETE"));
        }
    }

    @Test
    void maintenanceRunShowsCompactionSegmentsThroughRealBroker() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("demo-cli-compaction");
        BrokerConfig config = BrokerTestHarness.controllerConfig(
                0,
                dataDir,
                "--segment-max-bytes", "128",
                "--compaction-enabled",
                "--compaction-max-segments", "4");
        try (BrokerTestHarness broker = BrokerTestHarness.start(config)) {
            captureStdout(() -> createTopicAndProduceRolledSegments(
                    broker.bootstrapServers(), "compacted", "shared-key", 6));

            String output = captureStdout(() -> DemoCli.main(new String[] {
                    "maintenance-run",
                    "--broker", broker.bootstrapServers(),
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN
            }));

            assertTrue(output.contains("dryRun=false"));
            assertTrue(output.contains("segments="));
            assertTrue(output.contains("action=COMPACTION_"));
        }
    }

    private static void createTopicAndProduceRolledSegments(
            String brokers, String topic, String key, int recordCount) throws Exception {
        DemoCli.main(new String[] {
                "create-topic", "--brokers", brokers, "--name", topic, "--partitions", "1"
        });
        for (int i = 0; i < recordCount; i++) {
            DemoCli.main(new String[] {
                    "produce",
                    "--brokers", brokers,
                    "--topic", topic,
                    "--key", key,
                    "--message", "value-" + i + "-" + "x".repeat(96)
            });
        }
    }

    private static String captureStdout(ThrowingRunnable action) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            action.run();
            return output.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}

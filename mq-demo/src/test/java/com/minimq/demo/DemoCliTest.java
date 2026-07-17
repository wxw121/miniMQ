package com.minimq.demo;

import com.minimq.broker.support.BrokerTestHarness;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
                "--controller"
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

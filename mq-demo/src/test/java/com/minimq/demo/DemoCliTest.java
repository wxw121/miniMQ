package com.minimq.demo;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoCliTest {

    @Test
    void operatorRequestTimeoutIsConfigurableAndBounded() {
        assertEquals(
                Duration.ofSeconds(45),
                DemoCli.operatorRequestTimeout(new String[] {
                        "delete-topic", "--request-timeout-ms", "45000"
                }));
        assertEquals(
                Duration.ofSeconds(15),
                DemoCli.operatorRequestTimeout(new String[] {"delete-topic"}));

        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.operatorRequestTimeout(new String[] {
                        "delete-topic", "--request-timeout-ms", "0"
                }));
        assertThrows(IllegalArgumentException.class,
                () -> DemoCli.operatorRequestTimeout(new String[] {
                        "delete-topic", "--request-timeout-ms", "600001"
                }));
    }

    @Test
    void startBrokersScriptDefinesOwnedBoundedRunContract() throws Exception {
        String script = Files.readString(repositoryRoot().resolve("scripts/start-brokers.ps1"));

        assertTrue(script.contains("[string]$RunDirectory"));
        assertTrue(script.contains("broker-processes.json"));
        assertTrue(script.contains("processStartTimeUtc"));
        assertTrue(script.contains("ReadinessTimeoutSeconds"));
        assertTrue(script.contains(".minimq-run-directory"));
        assertTrue(script.contains("docker"));
        assertTrue(script.contains("compose"));
        assertTrue(script.contains("\"ops-status\""));
        assertTrue(script.contains("brokers=total=3"));
    }

    @Test
    void stopBrokersScriptIsIdempotentAndManifestScoped() throws Exception {
        String script = Files.readString(repositoryRoot().resolve("scripts/stop-brokers.ps1"));

        assertTrue(script.contains("[string]$RunDirectory"));
        assertTrue(script.contains("broker-processes.json"));
        assertTrue(script.contains("processStartTimeUtc"));
        assertTrue(script.contains("StopTimeoutSeconds"));
        assertTrue(script.contains("CleanupRunDirectory"));
        assertTrue(script.contains(".minimq-run-directory"));
        assertFalse(script.contains("Get-Process -Name"));
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void stopBrokersRefusesToDeleteDirectoryWithoutOwnershipMarker(@TempDir Path tempDir)
            throws Exception {
        Path unowned = Files.createDirectory(tempDir.resolve("unowned"));
        Path userFile = Files.writeString(unowned.resolve("keep.txt"), "keep");

        Process process = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                repositoryRoot().resolve("scripts/stop-brokers.ps1").toString(),
                "-RunDirectory",
                unowned.toString(),
                "-CleanupRunDirectory")
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "stop script did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertNotEquals(0, process.exitValue(), output);
        assertTrue(Files.exists(userFile), output);
        assertTrue(output.contains("ownership marker"), output);
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void stopBrokersRefusesManifestWithoutOwnershipMarkerBeforeProcessingPids(
            @TempDir Path tempDir) throws Exception {
        Path unowned = Files.createDirectory(tempDir.resolve("unowned-manifest"));
        Path manifest = Files.writeString(
                unowned.resolve("broker-processes.json"),
                """
                        {
                          "schemaVersion": 1,
                          "createdAtUtc": "2026-07-28T00:00:00Z",
                          "runDirectory": "%s",
                          "composeFile": "compose.yml",
                          "composeProjectName": "test",
                          "brokers": [
                            {
                              "pid": 999999,
                              "processStartTimeUtc": "2026-07-28T00:00:00Z",
                              "brokerId": 0,
                              "port": 9092
                            }
                          ]
                        }
                        """.formatted(unowned.toString().replace("\\", "\\\\")));

        Process process = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                repositoryRoot().resolve("scripts/stop-brokers.ps1").toString(),
                "-RunDirectory",
                unowned.toString())
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "stop script did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertNotEquals(0, process.exitValue(), output);
        assertTrue(output.contains("ownership marker"), output);
        assertTrue(Files.exists(manifest), output);
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void stopBrokersRejectsUnknownBrokerId(@TempDir Path tempDir) throws Exception {
        Path runDirectory = Files.createDirectory(tempDir.resolve("owned-run"));
        Files.writeString(
                runDirectory.resolve(".minimq-run-directory"),
                "minimq-v4-owned-run-directory-v1");
        Path manifest = Files.writeString(
                runDirectory.resolve("broker-processes.json"),
                """
                        {
                          "schemaVersion": 1,
                          "createdAtUtc": "2026-07-28T00:00:00Z",
                          "runDirectory": "%s",
                          "composeFile": "compose.yml",
                          "composeProjectName": "test",
                          "brokers": [
                            {
                              "pid": 999999,
                              "processStartTimeUtc": "2026-07-28T00:00:00Z",
                              "brokerId": 0,
                              "port": 9092
                            }
                          ]
                        }
                        """.formatted(runDirectory.toString().replace("\\", "\\\\")));

        Process process = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                repositoryRoot().resolve("scripts/stop-brokers.ps1").toString(),
                "-RunDirectory",
                runDirectory.toString(),
                "-BrokerId",
                "99")
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "stop script did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertNotEquals(0, process.exitValue(), output);
        assertTrue(output.contains("not present in the Broker manifest"), output);
        assertTrue(Files.exists(manifest), output);
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void failoverDemoDoesNotTearDownComposeBeforeOwnershipIsEstablished(@TempDir Path tempDir)
            throws Exception {
        Path runDirectory = Files.createDirectories(tempDir.resolve("unowned-run").resolve("logs"))
                .getParent();
        Files.writeString(runDirectory.resolve("keep.txt"), "keep");
        Path fakeBin = Files.createDirectory(tempDir.resolve("fake-bin"));
        Path dockerLog = tempDir.resolve("docker-invocations.log");
        Files.writeString(
                fakeBin.resolve("docker.cmd"),
                "@echo %*>>\"%FAKE_DOCKER_LOG%\"\r\n@exit /b 0\r\n");

        ProcessBuilder builder = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                repositoryRoot().resolve("scripts/demo-v4-failover.ps1").toString(),
                "-RunDirectory",
                runDirectory.toString(),
                "-ComposeProjectName",
                "preexisting-project",
                "-SkipBuild",
                "-KeepRunDirectory");
        builder.environment().put("FAKE_DOCKER_LOG", dockerLog.toString());
        builder.environment().put(
                "PATH",
                fakeBin + System.getProperty("path.separator") + builder.environment().get("PATH"));
        Process process = builder.redirectErrorStream(true).start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), "failover script did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertNotEquals(0, process.exitValue(), output);
        assertFalse(Files.exists(dockerLog),
                "startup failed before compose ownership, but Docker was invoked: "
                        + (Files.exists(dockerLog) ? Files.readString(dockerLog) : ""));
        assertTrue(Files.exists(runDirectory.resolve("keep.txt")), output);
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void failoverDemoPreservesComposeOwnershipWhenVolumeCleanupFails(@TempDir Path tempDir)
            throws Exception {
        Path harnessRoot = Files.createDirectory(tempDir.resolve("harness"));
        Path scripts = Files.createDirectory(harnessRoot.resolve("scripts"));
        Files.writeString(
                harnessRoot.resolve("pom.xml"),
                "<project><version>4.0.0</version></project>");
        Files.writeString(
                scripts.resolve("demo-v4-failover.ps1"),
                Files.readString(repositoryRoot().resolve("scripts/demo-v4-failover.ps1")));
        Files.writeString(
                scripts.resolve("start-brokers.ps1"),
                """
                        param(
                            [int[]]$Ports,
                            [string]$EtcdEndpoints,
                            [string]$ClusterId,
                            [string]$RunDirectory,
                            [string]$AdminToken,
                            [string]$ComposeFile,
                            [string]$ComposeProjectName,
                            [int]$ReadinessTimeoutSeconds,
                            [int]$CommandTimeoutSeconds,
                            [switch]$RequireUnusedComposeProject,
                            [switch]$SkipBuild
                        )
                        New-Item -ItemType Directory -Force -Path $RunDirectory | Out-Null
                        [IO.File]::WriteAllText(
                            (Join-Path $RunDirectory '.minimq-run-directory'),
                            'minimq-v4-owned-run-directory-v1')
                        [IO.File]::WriteAllText(
                            (Join-Path $RunDirectory '.minimq-compose-owned'),
                            ('minimq-v4-owned-compose-project-v1:' + $ComposeProjectName))
                        throw 'forced startup failure after compose ownership'
                        """);
        Files.writeString(
                scripts.resolve("stop-brokers.ps1"),
                """
                        param(
                            [string]$RunDirectory,
                            [int[]]$BrokerId = @(),
                            [int]$StopTimeoutSeconds,
                            [switch]$CleanupRunDirectory
                        )
                        if ($CleanupRunDirectory) {
                            Remove-Item -LiteralPath $RunDirectory -Recurse -Force
                        }
                        """);
        Path fakeBin = Files.createDirectory(tempDir.resolve("fake-bin"));
        Files.writeString(fakeBin.resolve("docker.cmd"), "@exit /b 1\r\n");
        Path runDirectory = tempDir.resolve("owned-run");

        ProcessBuilder builder = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                scripts.resolve("demo-v4-failover.ps1").toString(),
                "-RunDirectory",
                runDirectory.toString(),
                "-ComposeProjectName",
                "owned-project",
                "-AdminToken",
                "secret",
                "-SkipBuild");
        builder.environment().put(
                "PATH",
                fakeBin + System.getProperty("path.separator") + builder.environment().get("PATH"));
        Process process = builder.redirectErrorStream(true).start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), "failover script did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertNotEquals(0, process.exitValue(), output);
        assertTrue(Files.exists(runDirectory.resolve(".minimq-run-directory")), output);
        assertTrue(Files.exists(runDirectory.resolve(".minimq-compose-owned")), output);
    }

    @Test
    void failoverDemoRunsRecoveryWorkflowWithFinallyCleanup() throws Exception {
        String script = Files.readString(repositoryRoot().resolve("scripts/demo-v4-failover.ps1"));

        for (String command : List.of(
                "create-topic", "produce", "consume", "ops-status", "failover-audit")) {
            assertTrue(script.contains("\"" + command + "\""), "workflow missing " + command);
        }
        assertTrue(script.contains("FailoverTimeoutSeconds"));
        assertTrue(script.contains("-BrokerId $leaderId"));
        assertTrue(script.contains("RequireFullIsr"));
        assertTrue(script.contains("isrIds=V4BoundedList[total=3"));
        assertTrue(script.contains("$candidate -ne [int]$PreviousLeaderId"));
        assertTrue(script.contains("\"produce-after\""));
        assertTrue(script.contains("\"consume-after\""));
        assertTrue(script.contains("finally"));
        assertTrue(script.contains("stop-brokers.ps1"));
    }

    private static String windowsPowerShell() {
        return Path.of(
                System.getenv("SystemRoot"),
                "System32",
                "WindowsPowerShell",
                "v1.0",
                "powershell.exe").toString();
    }

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
        assertTrue(noArgs.contains("runtime-metrics"));
        assertTrue(unknown.contains("runtime-metrics"));
        for (String command : List.of(
                "ops-status",
                "group-status",
                "list-topics",
                "describe-topic",
                "delete-topic",
                "increase-partitions",
                "failover-audit",
                "auth-status",
                "configure-topic-reliability",
                "broker-offline",
                "retry-produce",
                "dead-letter-produce",
                "delayed-produce")) {
            assertTrue(noArgs.contains(command), "usage missing " + command);
            assertTrue(unknown.contains(command), "unknown-command usage missing " + command);
        }
        String brokerUsage =
                "broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --etcd-endpoints http://localhost:2379 --initialize-empty-cluster --admin-token <secret>";
        assertTrue(noArgs.contains(brokerUsage));
        assertTrue(unknown.contains(brokerUsage));
        assertTrue(noArgs.contains("create-topic --brokers localhost:9092 --admin-token <secret>"));
        assertTrue(noArgs.contains("--broker localhost:9092 --admin-token"));
    }

    @Test
    void rejectsInvalidBrokerAddressesBeforeNetworkUse() {
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "create-topic", "--brokers", "localhost", "--admin-token", "secret", "--name", "orders"
        }));
        assertThrows(IllegalArgumentException.class, () -> DemoCli.main(new String[] {
                "create-topic", "--brokers", "localhost:9092", "--name", "orders"
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
                "--etcd-endpoints", "http://127.0.0.1:2379",
                "--initialize-empty-cluster",
                "--admin-token", "test-admin-token"
        });

        assertEquals(3, config.brokerId());
        assertEquals("127.0.0.1", config.host());
        assertEquals(19092, config.port());
        assertTrue(config.initializeEmptyCluster());
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
                    "create-topic", "--brokers", brokers,
                    "--admin-token", BrokerTestHarness.ADMIN_TOKEN,
                    "--name", "orders", "--partitions", "1"
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
            String runtimeMetricsOutput = captureStdout(() -> DemoCli.main(new String[] {
                    "runtime-metrics", "--broker", brokers,
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
            assertTrue(runtimeMetricsOutput.contains("Runtime metrics"));
            assertTrue(runtimeMetricsOutput.contains("uptimeMs="));
            assertTrue(runtimeMetricsOutput.contains("requestRatePerSecond="));
            assertTrue(runtimeMetricsOutput.contains("produceBytes="));
            assertTrue(runtimeMetricsOutput.contains("fetchBytes="));
            assertTrue(runtimeMetricsOutput.contains("lagMetrics="));
            assertTrue(runtimeMetricsOutput.contains("auditOverwrittenCount="));
            assertFalse(reportOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(runOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(metricsOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
            assertFalse(runtimeMetricsOutput.contains(BrokerTestHarness.ADMIN_TOKEN));
        }
    }

    @Test
    void v4OperatorAndReliabilityCommandsRunThroughProtocolWithoutCredentialLeaks() throws Exception {
        Path dataDir = BrokerTestHarness.newDataDir("demo-cli-v4-operator");
        Path followerDir = BrokerTestHarness.newDataDir("demo-cli-v4-operator-follower");
        try (BrokerTestHarness broker = BrokerTestHarness.startController(
                     dataDir, "--topic-delete-timeout-ms", "500");
             BrokerTestHarness follower = BrokerTestHarness.startFollower(
                     1, followerDir, "127.0.0.1", broker.port())) {
            String endpoint = broker.bootstrapServers();
            String credential = BrokerTestHarness.ADMIN_TOKEN;
            StringBuilder output = new StringBuilder();

            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "broker-offline", "--brokers", endpoint, "--admin-token", credential,
                    "--broker-id", "1"
            })));

            for (String topic : List.of("orders", "orders-retry", "orders-dlq")) {
                output.append(captureStdout(() -> DemoCli.main(new String[] {
                        "create-topic", "--brokers", endpoint, "--admin-token", credential,
                        "--name", topic, "--partitions", "1"
                })));
            }
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "configure-topic-reliability", "--brokers", endpoint,
                    "--admin-token", credential, "--source-topic", "orders",
                    "--retry-topic", "orders-retry", "--dead-letter-topic", "orders-dlq"
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "increase-partitions", "--brokers", endpoint, "--admin-token", credential,
                    "--topic", "orders", "--target-count", "2"
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "list-topics", "--brokers", endpoint, "--admin-token", credential
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "describe-topic", "--brokers", endpoint, "--admin-token", credential,
                    "--topic", "orders"
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "ops-status", "--broker", endpoint, "--admin-token", credential
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "auth-status", "--broker", endpoint, "--admin-token", credential
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "failover-audit", "--broker", endpoint, "--admin-token", credential
            })));

            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "produce", "--brokers", endpoint, "--topic", "orders", "--message", "group-seed"
            })));
            output.append(captureStdout(() -> DemoCli.consumeForPolls(new String[] {
                    "consume", "--brokers", endpoint, "--topic", "orders", "--group", "ops-group"
            }, 1)));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "group-status", "--brokers", endpoint, "--admin-token", credential,
                    "--group", "ops-group", "--topic", "orders"
            })));

            long deliverAt = System.currentTimeMillis();
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "delayed-produce", "--brokers", endpoint, "--topic", "orders",
                    "--message", "delayed", "--deliver-at-epoch-ms", Long.toString(deliverAt)
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "retry-produce", "--brokers", endpoint, "--source-topic", "orders",
                    "--message", "retry", "--deliver-at-epoch-ms", Long.toString(deliverAt)
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "dead-letter-produce", "--brokers", endpoint, "--source-topic", "orders",
                    "--message", "dead-letter"
            })));

            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "delete-topic", "--brokers", endpoint, "--admin-token", credential,
                    "--topic", "orders-dlq"
            })));
            output.append(captureStdout(() -> DemoCli.main(new String[] {
                    "describe-topic", "--brokers", endpoint, "--admin-token", credential,
                    "--topic", "orders-dlq", "--include-deleted"
            })));
            String rendered = output.toString();
            for (String marker : List.of(
                    "Reliability configured", "Partitions increased", "Topics total=",
                    "Topic orders active=true", "Ops status", "Auth status", "Failover audit",
                    "Group status", "Delayed produce", "Retry produce", "Dead-letter produce",
                    "Topic deletion", "active=false", "Broker offline")) {
                assertTrue(rendered.contains(marker), "missing output marker " + marker);
            }
            assertFalse(rendered.contains(credential));
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
                "create-topic", "--brokers", brokers,
                "--admin-token", BrokerTestHarness.ADMIN_TOKEN,
                "--name", topic, "--partitions", "1"
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

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.isDirectory(current.resolve("scripts"))
                    && Files.isRegularFile(current.resolve("pom.xml"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Cannot locate repository root");
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}

package com.minimq.demo;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V4ScriptContractTest {

    @Test
    void startBuildUsesCommandTimeoutBoundedProcess() throws Exception {
        String script = readScript("start-brokers.ps1");

        assertFalse(script.contains("& mvn"), "Maven must not bypass the bounded process helper");
        Pattern boundedMaven = Pattern.compile(
                "-FilePath \\\"mvn\\\".*?-TimeoutSeconds \\$CommandTimeoutSeconds.*?"
                        + "-Description \\\"Maven build\\\"",
                Pattern.DOTALL);
        assertTrue(boundedMaven.matcher(script).find(),
                "Maven must run as a tracked process bounded by CommandTimeoutSeconds");
    }

    @Test
    void boundedProcessesTerminateTheEntireNativeProcessTree() throws Exception {
        String startScript = readScript("start-brokers.ps1");
        String failoverScript = readScript("demo-v4-failover.ps1");

        assertTrue(startScript.contains("function Stop-NativeProcessTree"));
        assertTrue(failoverScript.contains("function Stop-NativeProcessTree"));
        assertTrue(startScript.contains("/T"), "Windows timeout cleanup must include descendants");
        assertTrue(failoverScript.contains("/T"), "Windows timeout cleanup must include descendants");
        assertTrue(startScript.contains("Stop-NativeProcessTree -Process $process"));
        assertTrue(failoverScript.contains("Stop-NativeProcessTree -Process $process"));
    }

    @Test
    void startReadinessRequiresActiveMaintenanceAndThreeOnlineBrokers() throws Exception {
        String script = readScript("start-brokers.ps1");

        assertTrue(script.contains("maintenanceState=ACTIVE"),
                "readiness must reject a broker in maintenance mode");
        assertTrue(script.contains("state=ONLINE"),
                "readiness must inspect individual broker states");
        assertTrue(script.contains("$onlineBrokerCount -eq 3"),
                "readiness must require all three brokers to be online");
        assertTrue(script.contains("brokers=total=3 truncated=false"),
                "readiness must inspect the complete three-broker list");
    }

    @Test
    void failoverDemoValidatesPostFailoverStatusAndAuditEvidence() throws Exception {
        String script = readScript("demo-v4-failover.ps1");

        assertTrue(script.contains("$opsStatusAfter = Invoke-DemoCommand \"ops-status-after\""),
                "post-failover ops-status output must be captured");
        assertTrue(script.contains("maintenanceState=ACTIVE"),
                "post-failover status must prove the surviving broker is active");
        assertTrue(script.contains("topicName=$Topic"),
                "post-failover status must include the demo topic");
        assertTrue(script.contains("leaderId=$newLeaderId"),
                "post-failover status must prove the newly elected leader");
        assertTrue(script.contains("eventType=LEADER_CHANGED"),
                "audit output must prove the leader transition");
        assertTrue(script.contains("oldValue=$leaderId"),
                "audit evidence must identify the stopped leader");
        assertTrue(script.contains("newValue=$newLeaderId"),
                "audit evidence must identify the replacement leader");
        assertTrue(script.contains("events=total=([1-9][0-9]*)"),
                "audit output must be non-empty");
    }

    @Test
    void failoverLeaderProbeUsesOnlyTheRemainingDeadlineBudget() throws Exception {
        String script = readScript("demo-v4-failover.ps1");

        assertTrue(script.contains("$probeTimeoutSeconds"));
        assertTrue(script.contains("-TimeoutSeconds $probeTimeoutSeconds"));
        assertTrue(script.contains("($Deadline - [DateTime]::UtcNow).TotalSeconds"));
    }

    @Test
    void failoverDemoRemovesComposeVolumesDuringCleanup() throws Exception {
        String startScript = readScript("start-brokers.ps1");
        String script = readScript("demo-v4-failover.ps1");

        assertTrue(startScript.contains("[switch]$RequireUnusedComposeProject"),
                "the failover workflow must be able to reserve an unused compose project");
        assertTrue(startScript.contains(".minimq-compose-owned"),
                "compose ownership must be persisted in the owned run directory");
        assertTrue(script.contains("RequireUnusedComposeProject = $true"),
                "the failover demo must request exclusive compose ownership");
        assertTrue(script.contains("Test-Path -LiteralPath $composeOwnershipMarkerPath"),
                "cleanup must verify compose ownership before deleting volumes");
        assertTrue(script.contains("\"down\", \"--volumes\""),
                "compose cleanup must remove the demo's persistent volumes");
    }

    private static String readScript(String name) throws IOException {
        return Files.readString(repositoryRoot().resolve("scripts").resolve(name));
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
}

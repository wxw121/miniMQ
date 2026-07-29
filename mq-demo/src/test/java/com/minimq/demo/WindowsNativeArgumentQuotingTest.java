package com.minimq.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledOnOs(OS.WINDOWS)
class WindowsNativeArgumentQuotingTest {

    @Test
    void scriptsPreserveNativeArgumentsThroughWindowsParsing() throws Exception {
        Path repositoryRoot = repositoryRoot();
        Process process = new ProcessBuilder(
                windowsPowerShell(),
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                repositoryRoot.resolve(
                        "mq-demo/src/test/resources/native-argument-quoting.ps1").toString(),
                "-RepositoryRoot",
                repositoryRoot.toString())
                .redirectErrorStream(true)
                .start();

        assertTrue(process.waitFor(20, TimeUnit.SECONDS),
                "native argument quoting test did not terminate");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), output);
    }

    private static String windowsPowerShell() {
        return Path.of(
                System.getenv("SystemRoot"),
                "System32",
                "WindowsPowerShell",
                "v1.0",
                "powershell.exe").toString();
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

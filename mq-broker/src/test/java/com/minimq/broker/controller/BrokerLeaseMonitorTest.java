package com.minimq.broker.controller;

import com.minimq.broker.metadata.MetadataStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerLeaseMonitorTest {

    @TempDir
    Path tempDir;

    @Test
    void scansControllerAndClosesInjectedScheduler() {
        CountingController controller = new CountingController(new MetadataStore(tempDir));
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        BrokerLeaseMonitor monitor = new BrokerLeaseMonitor(controller, 100L, executor);

        monitor.runOnce();

        assertEquals(1, controller.expireCalls);
        monitor.close();
        assertTrue(executor.isShutdown());
    }

    @Test
    void scanFailureDoesNotPreventNextRun() {
        CountingController controller = new CountingController(new MetadataStore(tempDir));
        controller.failNext = true;
        BrokerLeaseMonitor monitor = new BrokerLeaseMonitor(
                controller, 100L, new ScheduledThreadPoolExecutor(1));

        monitor.runOnce();
        monitor.runOnce();

        assertEquals(2, controller.expireCalls);
        monitor.close();
    }

    @Test
    void givesRecoveryWindowControlBeforeScanningLeases() {
        CountingController controller = new CountingController(new MetadataStore(tempDir));
        BrokerLeaseMonitor monitor = new BrokerLeaseMonitor(
                controller, 100L, new ScheduledThreadPoolExecutor(1));

        monitor.runOnce();

        assertEquals(1, controller.recoveryCalls);
        assertEquals(1, controller.expireCalls);
        assertEquals(1, controller.activationCalls);
        monitor.close();
    }

    @Test
    void inactiveControllerDoesNotAttemptMetadataMutation() {
        CountingController controller = new CountingController(new MetadataStore(tempDir));
        BrokerLeaseMonitor monitor = new BrokerLeaseMonitor(
                controller, 100L, 2_000L, () -> false,
                new ScheduledThreadPoolExecutor(1));

        monitor.runOnce();

        assertEquals(0, controller.recoveryCalls);
        assertEquals(0, controller.expireCalls);
        assertEquals(0, controller.activationCalls);
        monitor.close();
    }

    private static final class CountingController extends ClusterController {
        private int expireCalls;
        private int recoveryCalls;
        private int activationCalls;
        private boolean failNext;

        private CountingController(MetadataStore store) {
            super(store);
        }

        @Override
        public synchronized int expireOverdueBrokerLeases() {
            expireCalls++;
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("scan failed");
            }
            return 0;
        }

        @Override
        public synchronized boolean advanceRecoveryWindow() {
            recoveryCalls++;
            return false;
        }

        @Override
        public synchronized int expireLeaderActivationTimeouts(long timeoutMs) {
            activationCalls++;
            return 0;
        }
    }
}

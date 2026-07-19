package com.minimq.broker;

import com.minimq.common.error.ErrorCode;
import com.minimq.storage.LogMaintenanceMetrics;
import com.minimq.storage.LogMaintenanceReport;
import com.minimq.storage.LogManager;
import com.minimq.storage.MaintenanceStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MaintenanceAdminServiceTest {
    private static final String TOKEN = "secret";

    @TempDir
    Path tempDir;

    @Test
    void invalidTokenDoesNotInvokeMaintenanceOperations() throws Exception {
        try (TrackingPartitionManager manager = manager("unauthorized");
             MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager)) {
            var report = service.report("wrong").toCompletableFuture().get(2, TimeUnit.SECONDS);
            var run = service.run("wrong").toCompletableFuture().get(2, TimeUnit.SECONDS);
            var metrics = service.metrics("wrong");

            assertEquals(ErrorCode.UNAUTHORIZED, report.errorCode());
            assertEquals(ErrorCode.UNAUTHORIZED, run.errorCode());
            assertEquals(ErrorCode.UNAUTHORIZED, metrics.errorCode());
            assertNull(report.body());
            assertEquals(0, manager.totalCalls());
        }
    }

    @Test
    void authorizedReportAndMetricsReturnStorageResults() throws Exception {
        try (TrackingPartitionManager manager = manager("authorized");
             MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager)) {
            var report = service.report(TOKEN).toCompletableFuture().get(2, TimeUnit.SECONDS);
            var metrics = service.metrics(TOKEN);

            assertEquals(ErrorCode.NONE, report.errorCode());
            assertEquals(TrackingPartitionManager.DRY_RUN_REPORT, report.body());
            assertEquals(ErrorCode.NONE, metrics.errorCode());
            assertEquals(LogMaintenanceMetrics.empty(), metrics.body());
            assertEquals(2, manager.totalCalls());
        }
    }

    @Test
    void runExecutesOffCallerThreadAndRejectsOverlap() throws Exception {
        try (TrackingPartitionManager manager = manager("overlap");
             MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager)) {
            manager.blockNextRun();
            String callerThread = Thread.currentThread().getName();

            CompletionStage<MaintenanceAdminService.Result<LogMaintenanceReport>> first = service.run(TOKEN);
            assertTrue(manager.runStarted.await(2, TimeUnit.SECONDS));
            var second = service.run(TOKEN).toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertEquals(ErrorCode.MAINTENANCE_BUSY, second.errorCode());
            assertTrue(manager.runThread.get().startsWith(MaintenanceAdminService.THREAD_NAME_PREFIX));
            assertFalse(callerThread.equals(manager.runThread.get()));

            manager.releaseRun.countDown();
            assertEquals(ErrorCode.NONE, first.toCompletableFuture().get(2, TimeUnit.SECONDS).errorCode());
        }
    }

    @Test
    void failedRunReleasesAdmissionForRetry() throws Exception {
        try (TrackingPartitionManager manager = manager("failure");
             MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager)) {
            manager.failNextRun.set(true);

            assertThrows(CompletionException.class, () -> service.run(TOKEN).toCompletableFuture().join());
            var retry = service.run(TOKEN).toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertEquals(ErrorCode.NONE, retry.errorCode());
            assertEquals(2, manager.runCalls.get());
        }
    }

    @Test
    void fullExecutorQueueReturnsBusy() throws Exception {
        try (TrackingPartitionManager manager = manager("queue");
             MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager)) {
            manager.blockNextRun();
            CompletionStage<MaintenanceAdminService.Result<LogMaintenanceReport>> running = service.run(TOKEN);
            assertTrue(manager.runStarted.await(2, TimeUnit.SECONDS));
            CompletionStage<MaintenanceAdminService.Result<LogMaintenanceReport>> queued = service.report(TOKEN);

            var rejected = service.report(TOKEN).toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(ErrorCode.MAINTENANCE_BUSY, rejected.errorCode());

            manager.releaseRun.countDown();
            assertEquals(ErrorCode.NONE, running.toCompletableFuture().get(2, TimeUnit.SECONDS).errorCode());
            assertEquals(ErrorCode.NONE, queued.toCompletableFuture().get(2, TimeUnit.SECONDS).errorCode());
        }
    }

    @Test
    void closeIsIdempotentAndRejectsNewWork() throws Exception {
        try (TrackingPartitionManager manager = manager("closed")) {
            MaintenanceAdminService service = new MaintenanceAdminService(TOKEN, manager);
            service.close();
            service.close();

            var report = service.report(TOKEN).toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(ErrorCode.MAINTENANCE_BUSY, report.errorCode());
            assertEquals(0, manager.totalCalls());
        }
    }

    private TrackingPartitionManager manager(String name) {
        return new TrackingPartitionManager(new LogManager(tempDir.resolve(name)));
    }

    private static final class TrackingPartitionManager extends PartitionManager implements AutoCloseable {
        private static final LogMaintenanceReport DRY_RUN_REPORT =
                LogMaintenanceReport.skipped(true, MaintenanceStatus.SKIPPED_DISABLED);
        private static final LogMaintenanceReport RUN_REPORT =
                LogMaintenanceReport.skipped(false, MaintenanceStatus.SKIPPED_DISABLED);

        private final AtomicInteger reportCalls = new AtomicInteger();
        private final AtomicInteger runCalls = new AtomicInteger();
        private final AtomicInteger metricsCalls = new AtomicInteger();
        private final AtomicBoolean failNextRun = new AtomicBoolean();
        private final AtomicReference<String> runThread = new AtomicReference<>();
        private volatile CountDownLatch runStarted = new CountDownLatch(0);
        private volatile CountDownLatch releaseRun = new CountDownLatch(0);

        private TrackingPartitionManager(LogManager logManager) {
            super(logManager);
        }

        void blockNextRun() {
            runStarted = new CountDownLatch(1);
            releaseRun = new CountDownLatch(1);
        }

        int totalCalls() {
            return reportCalls.get() + runCalls.get() + metricsCalls.get();
        }

        @Override
        public LogMaintenanceReport dryRunMaintenance() {
            reportCalls.incrementAndGet();
            return DRY_RUN_REPORT;
        }

        @Override
        public LogMaintenanceReport maintain() {
            runCalls.incrementAndGet();
            runThread.set(Thread.currentThread().getName());
            runStarted.countDown();
            try {
                releaseRun.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("run interrupted", e);
            }
            if (failNextRun.compareAndSet(true, false)) {
                throw new IllegalStateException("planned failure");
            }
            return RUN_REPORT;
        }

        @Override
        public LogMaintenanceMetrics maintenanceMetrics() {
            metricsCalls.incrementAndGet();
            return LogMaintenanceMetrics.empty();
        }
    }
}

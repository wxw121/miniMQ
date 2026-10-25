package com.minimq.broker.controller;

import com.minimq.broker.audit.FailoverAuditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public final class BrokerLeaseMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerLeaseMonitor.class);

    private final ClusterController clusterController;
    private final long scanIntervalMs;
    private final long leaderActivationTimeoutMs;
    private final BooleanSupplier controllerActive;
    private final FailoverAuditLog auditLog;
    private final ScheduledExecutorService scheduler;

    public BrokerLeaseMonitor(ClusterController clusterController, long scanIntervalMs) {
        this(clusterController, scanIntervalMs, 5_000L);
    }

    public BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs) {
        this(clusterController, scanIntervalMs, leaderActivationTimeoutMs, () -> true);
    }

    public BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            BooleanSupplier controllerActive) {
        this(clusterController, scanIntervalMs, leaderActivationTimeoutMs, controllerActive,
                (FailoverAuditLog) null);
    }

    public BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            BooleanSupplier controllerActive,
            FailoverAuditLog auditLog) {
        this(
                clusterController,
                scanIntervalMs,
                leaderActivationTimeoutMs,
                controllerActive,
                auditLog,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "broker-lease-monitor");
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            ScheduledExecutorService scheduler) {
        this(clusterController, scanIntervalMs, 5_000L, scheduler);
    }

    BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            ScheduledExecutorService scheduler) {
        this(clusterController, scanIntervalMs, leaderActivationTimeoutMs, () -> true, scheduler);
    }

    BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            BooleanSupplier controllerActive,
            ScheduledExecutorService scheduler) {
        this(clusterController, scanIntervalMs, leaderActivationTimeoutMs, controllerActive,
                (FailoverAuditLog) null, scheduler);
    }

    BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            BooleanSupplier controllerActive,
            FailoverAuditLog auditLog,
            ScheduledExecutorService scheduler) {
        this.clusterController = clusterController;
        this.scanIntervalMs = scanIntervalMs;
        this.leaderActivationTimeoutMs = leaderActivationTimeoutMs;
        this.controllerActive = controllerActive;
        this.auditLog = auditLog;
        this.scheduler = scheduler;
    }

    BrokerLeaseMonitor(
            ClusterController clusterController,
            long scanIntervalMs,
            long leaderActivationTimeoutMs,
            BooleanSupplier controllerActive,
            ScheduledExecutorService scheduler,
            FailoverAuditLog auditLog) {
        this(clusterController, scanIntervalMs, leaderActivationTimeoutMs,
                controllerActive, auditLog, scheduler);
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::runOnce,
                scanIntervalMs,
                scanIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    void runOnce() {
        if (!controllerActive.getAsBoolean()) {
            return;
        }
        try {
            clusterController.advanceRecoveryWindow();
            clusterController.expireOverdueBrokerLeases();
            clusterController.expireLeaderActivationTimeouts(leaderActivationTimeoutMs);
        } catch (RuntimeException failure) {
            log.warn("Broker lease scan failed", failure);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Broker lease monitor did not terminate");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
        }
    }
}

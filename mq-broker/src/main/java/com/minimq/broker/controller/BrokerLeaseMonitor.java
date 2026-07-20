package com.minimq.broker.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class BrokerLeaseMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerLeaseMonitor.class);

    private final ClusterController clusterController;
    private final long scanIntervalMs;
    private final ScheduledExecutorService scheduler;

    public BrokerLeaseMonitor(ClusterController clusterController, long scanIntervalMs) {
        this(
                clusterController,
                scanIntervalMs,
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
        this.clusterController = clusterController;
        this.scanIntervalMs = scanIntervalMs;
        this.scheduler = scheduler;
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::runOnce,
                scanIntervalMs,
                scanIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    void runOnce() {
        try {
            clusterController.advanceRecoveryWindow();
            clusterController.expireOverdueBrokerLeases();
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

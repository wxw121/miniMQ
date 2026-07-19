package com.minimq.broker;

import com.minimq.common.error.ErrorCode;
import com.minimq.storage.LogMaintenanceMetrics;
import com.minimq.storage.LogMaintenanceReport;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Broker-scoped boundary for authenticated maintenance administration.
 */
public final class MaintenanceAdminService implements AutoCloseable {
    public static final String THREAD_NAME_PREFIX = "minimq-maintenance-admin-";

    private final byte[] adminTokenBytes;
    private final PartitionManager partitionManager;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean runInFlight = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public MaintenanceAdminService(String adminToken, PartitionManager partitionManager) {
        if (adminToken == null || adminToken.isBlank()) {
            throw new IllegalArgumentException("adminToken is required and must not be blank");
        }
        this.adminTokenBytes = adminToken.getBytes(StandardCharsets.UTF_8);
        this.partitionManager = Objects.requireNonNull(partitionManager, "partitionManager");
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                maintenanceThreadFactory());
    }

    public CompletionStage<Result<LogMaintenanceReport>> report(String suppliedToken) {
        if (!authorized(suppliedToken)) {
            return completedError(ErrorCode.UNAUTHORIZED);
        }
        return submit(partitionManager::dryRunMaintenance, () -> {
        });
    }

    public CompletionStage<Result<LogMaintenanceReport>> run(String suppliedToken) {
        if (!authorized(suppliedToken)) {
            return completedError(ErrorCode.UNAUTHORIZED);
        }
        if (!runInFlight.compareAndSet(false, true)) {
            return completedError(ErrorCode.MAINTENANCE_BUSY);
        }
        return submit(partitionManager::maintain, () -> runInFlight.set(false));
    }

    public Result<LogMaintenanceMetrics> metrics(String suppliedToken) {
        if (!authorized(suppliedToken)) {
            return Result.error(ErrorCode.UNAUTHORIZED);
        }
        if (closed.get()) {
            return Result.error(ErrorCode.MAINTENANCE_BUSY);
        }
        return Result.success(partitionManager.maintenanceMetrics());
    }

    private <T> CompletionStage<Result<T>> submit(Supplier<T> operation, Runnable releaseAdmission) {
        CompletableFuture<Result<T>> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                Result<T> result = null;
                Throwable failure = null;
                try {
                    result = Result.success(operation.get());
                } catch (Throwable caught) {
                    failure = caught;
                } finally {
                    releaseAdmission.run();
                }
                if (failure == null) {
                    future.complete(result);
                } else {
                    future.completeExceptionally(failure);
                }
            });
        } catch (RejectedExecutionException rejected) {
            releaseAdmission.run();
            future.complete(Result.error(ErrorCode.MAINTENANCE_BUSY));
        }
        return future;
    }

    private boolean authorized(String suppliedToken) {
        byte[] suppliedBytes = suppliedToken == null
                ? new byte[0]
                : suppliedToken.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(adminTokenBytes, suppliedBytes);
    }

    private static ThreadFactory maintenanceThreadFactory() {
        AtomicInteger threadIds = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, THREAD_NAME_PREFIX + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static <T> CompletionStage<Result<T>> completedError(ErrorCode errorCode) {
        return CompletableFuture.completedFuture(Result.error(errorCode));
    }

    @Override
    public void close() throws InterruptedException {
        if (closed.compareAndSet(false, true)) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    // Storage stays open until the maintenance thread has terminated.
                }
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            throw interrupted;
        }
    }

    public record Result<T>(ErrorCode errorCode, T body) {
        public static <T> Result<T> success(T body) {
            return new Result<>(ErrorCode.NONE, body);
        }

        public static <T> Result<T> error(ErrorCode errorCode) {
            return new Result<>(errorCode, null);
        }
    }
}

package com.minimq.broker.admin;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadata.PartitionEntry;
import com.minimq.broker.metadata.ClusterMetadata.TopicEntry;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityResponse;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminResponse;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsResponse;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsResponse;
import com.minimq.protocol.message.ProtocolMessages.TopicPartitionInfo;
import com.minimq.protocol.message.ProtocolMessages.TopicTombstoneInfo;
import com.minimq.protocol.message.ProtocolMessages.V4BoundedList;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Protocol-facing topic administration facade.  Metadata mutation remains in
 * {@link ClusterController}; this class only validates the request boundary,
 * applies response bounds, and converts snapshots into V4 records.
 */
public final class TopicAdminService implements AutoCloseable {
    private static final long DELETE_POLL_INTERVAL_MS = 10L;
    private static final int MAX_DELETE_WAITERS = 1_024;

    private final ClusterController controller;
    private final Clock clock;
    private final long topicDeleteTimeoutMs;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService waiterExecutor;
    private final ExecutorService mutationExecutor;
    private final int maxDeleteWaiters;
    private final AtomicInteger pendingDeleteWaiters = new AtomicInteger();
    private final Set<CompletableFuture<Result<DeleteTopicResponse>>> deleteWaiters =
            ConcurrentHashMap.newKeySet();
    private final Object deleteWaiterAdmission = new Object();
    private volatile boolean closed;

    public TopicAdminService(ClusterController controller) {
        this(controller, Clock.systemUTC(), BrokerConfig.DEFAULT_TOPIC_DELETE_TIMEOUT_MS);
    }

    TopicAdminService(ClusterController controller, Clock clock) {
        this(controller, clock, BrokerConfig.DEFAULT_TOPIC_DELETE_TIMEOUT_MS);
    }

    public TopicAdminService(ClusterController controller, long topicDeleteTimeoutMs) {
        this(controller, Clock.systemUTC(), topicDeleteTimeoutMs);
    }

    TopicAdminService(ClusterController controller, Clock clock, long topicDeleteTimeoutMs) {
        this(
                controller,
                clock,
                topicDeleteTimeoutMs,
                System::nanoTime,
                newDeleteWaiterExecutor(),
                newDeleteMutationExecutor(MAX_DELETE_WAITERS),
                MAX_DELETE_WAITERS);
    }

    TopicAdminService(
            ClusterController controller,
            Clock clock,
            long topicDeleteTimeoutMs,
            LongSupplier nanoTime,
            ScheduledExecutorService waiterExecutor,
            int maxDeleteWaiters) {
        this(
                controller,
                clock,
                topicDeleteTimeoutMs,
                nanoTime,
                waiterExecutor,
                newDeleteMutationExecutor(maxDeleteWaiters),
                maxDeleteWaiters);
    }

    TopicAdminService(
            ClusterController controller,
            Clock clock,
            long topicDeleteTimeoutMs,
            LongSupplier nanoTime,
            ScheduledExecutorService waiterExecutor,
            ExecutorService mutationExecutor,
            int maxDeleteWaiters) {
        if (controller == null
                || clock == null
                || nanoTime == null
                || waiterExecutor == null
                || mutationExecutor == null
                || topicDeleteTimeoutMs <= 0L
                || maxDeleteWaiters <= 0) {
            throw new IllegalArgumentException("invalid topic admin waiter configuration");
        }
        this.controller = controller;
        this.clock = clock;
        this.topicDeleteTimeoutMs = topicDeleteTimeoutMs;
        this.nanoTime = nanoTime;
        this.waiterExecutor = waiterExecutor;
        this.mutationExecutor = mutationExecutor;
        this.maxDeleteWaiters = maxDeleteWaiters;
        if (waiterExecutor instanceof ScheduledThreadPoolExecutor scheduled) {
            scheduled.setRemoveOnCancelPolicy(true);
        }
    }

    public Result<ListTopicsResponse> list(ListTopicsRequest request) {
        List<String> names = controller.listTopicNames();
        return Result.success(new ListTopicsResponse(bounded(names, request.limit())));
    }

    public Result<DescribeTopicResponse> describe(DescribeTopicRequest request) {
        ClusterMetadata snapshot = controller.snapshot();
        TopicEntry topic = snapshot.topics.get(request.topicName());
        if (topic != null) {
            List<TopicPartitionInfo> partitions = topic.partitions.stream()
                    .sorted(Comparator.comparingInt(p -> p.partitionId))
                    .map(this::toPartitionInfo)
                    .toList();
            String retryTopic = snapshot.retryTopicBySource.get(request.topicName());
            String deadLetterTopic = snapshot.deadLetterTopicBySource.get(request.topicName());
            return Result.success(new DescribeTopicResponse(
                    request.topicName(),
                    snapshot.metadataVersion,
                    true,
                    retryTopic,
                    deadLetterTopic,
                    bounded(partitions, request.partitionLimit()),
                    null,
                    topic.effectiveMinInSyncReplicas()));
        }
        ClusterMetadata.TopicTombstone tombstone = snapshot.topicTombstones.get(request.topicName());
        if (tombstone == null || !request.includeDeleted()) {
            return Result.failure(ErrorCode.UNKNOWN_TOPIC);
        }
        return Result.success(new DescribeTopicResponse(
                request.topicName(),
                snapshot.metadataVersion,
                false,
                null,
                null,
                new V4BoundedList<>(0, false, List.of()),
                toTombstoneInfo(tombstone)));
    }

    public Result<IncreasePartitionsResponse> increase(IncreasePartitionsRequest request) {
        ClusterController.IncreasePartitionsResult result = controller.increasePartitions(
                request.topicName(), request.targetPartitionCount());
        if (result.errorCode() != ErrorCode.NONE) {
            return Result.failure(result.errorCode());
        }
        return Result.success(new IncreasePartitionsResponse(
                request.topicName(),
                result.previousPartitionCount(),
                result.newPartitionCount(),
                result.metadataVersion()));
    }

    public Result<CreateTopicAdminResponse> create(CreateTopicAdminRequest request) {
        ErrorCode error = controller.createTopicAdmin(request);
        if (error != ErrorCode.NONE) {
            return Result.failure(error);
        }
        return Result.success(new CreateTopicAdminResponse(
                request.topicName(), controller.metadataVersion()));
    }

    public Result<ConfigureTopicReliabilityResponse> configureReliability(
            ConfigureTopicReliabilityRequest request) {
        ClusterController.ReliabilityConfigResult result =
                controller.configureTopicReliability(request);
        if (result.errorCode() != ErrorCode.NONE) {
            return Result.failure(result.errorCode());
        }
        return Result.success(new ConfigureTopicReliabilityResponse(
                request.sourceTopicName(),
                result.retryTopicName(),
                result.deadLetterTopicName(),
                result.metadataVersion()));
    }

    /**
     * Deletion is intentionally idempotent.  A repeated request returns the
     * current tombstone state without allocating another metadata version.
     */
    public Result<DeleteTopicResponse> delete(DeleteTopicRequest request) {
        ErrorCode error = controller.deleteTopic(request.topicName(), clock.millis());
        if (error != ErrorCode.NONE) {
            return Result.failure(error);
        }
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone(request.topicName());
        if (tombstone == null) {
            return Result.failure(ErrorCode.UNKNOWN_TOPIC);
        }
        return Result.success(toDeleteResponse(tombstone));
    }

    public CompletionStage<Result<DeleteTopicResponse>> deleteAndWait(DeleteTopicRequest request) {
        long now = nanoTime.getAsLong();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(topicDeleteTimeoutMs);
        long deadlineNanos = now > Long.MAX_VALUE - timeoutNanos
                ? Long.MAX_VALUE
                : now + timeoutNanos;
        CompletableFuture<Result<DeleteTopicResponse>> result;
        AtomicReference<Result<DeleteTopicResponse>> latestResult = new AtomicReference<>();
        synchronized (deleteWaiterAdmission) {
            if (closed) {
                return CompletableFuture.completedFuture(
                        Result.failure(ErrorCode.CONTROLLER_NOT_READY));
            }
            if (pendingDeleteWaiters.get() >= maxDeleteWaiters) {
                return CompletableFuture.completedFuture(Result.failure(ErrorCode.THROTTLED));
            }
            pendingDeleteWaiters.incrementAndGet();
            result = new CompletableFuture<>();
            deleteWaiters.add(result);
            result.whenComplete((ignored, failure) -> {
                deleteWaiters.remove(result);
                pendingDeleteWaiters.decrementAndGet();
            });
        }
        ScheduledFuture<?> deadlineTask = scheduleDeleteDeadline(deadlineNanos, result, latestResult);
        if (deadlineTask != null) {
            result.whenComplete((ignored, failure) -> deadlineTask.cancel(false));
        }
        try {
            mutationExecutor.execute(
                    () -> beginDelete(request, deadlineNanos, result, latestResult));
        } catch (RejectedExecutionException rejected) {
            result.complete(Result.failure(closed
                    ? ErrorCode.CONTROLLER_NOT_READY : ErrorCode.THROTTLED));
        }
        return result;
    }

    private void beginDelete(
            DeleteTopicRequest request,
            long deadlineNanos,
            CompletableFuture<Result<DeleteTopicResponse>> result,
            AtomicReference<Result<DeleteTopicResponse>> latestResult) {
        if (result.isDone() || deadlineNanos - nanoTime.getAsLong() <= 0L) {
            result.complete(Result.failure(ErrorCode.CONTROLLER_NOT_READY));
            return;
        }
        Result<DeleteTopicResponse> initial;
        try {
            long remainingNanos = deadlineNanos - nanoTime.getAsLong();
            if (remainingNanos <= 0L) {
                result.complete(Result.failure(ErrorCode.CONTROLLER_NOT_READY));
                return;
            }
            initial = delete(request, Duration.ofNanos(remainingNanos));
        } catch (RuntimeException failure) {
            result.complete(Result.failure(ErrorCode.CONTROLLER_NOT_READY));
            return;
        }
        latestResult.set(initial);
        if (initial.errorCode() != ErrorCode.NONE
                || "DELETED".equals(initial.response().tombstoneState())) {
            result.complete(initial);
            return;
        }
        long remainingNanos = Math.max(0L, deadlineNanos - nanoTime.getAsLong());
        scheduleDeletePoll(
                request.topicName(),
                initial.response().tombstoneVersion(),
                deadlineNanos,
                result,
                latestResult,
                Math.min(
                        remainingNanos,
                        TimeUnit.MILLISECONDS.toNanos(DELETE_POLL_INTERVAL_MS)));
    }

    private Result<DeleteTopicResponse> delete(
            DeleteTopicRequest request, Duration timeout) {
        ErrorCode error = controller.deleteTopic(
                request.topicName(), clock.millis(), timeout);
        if (error != ErrorCode.NONE) {
            return Result.failure(error);
        }
        ClusterMetadata.TopicTombstone tombstone =
                controller.getTopicTombstone(request.topicName());
        if (tombstone == null) {
            return Result.failure(ErrorCode.UNKNOWN_TOPIC);
        }
        return Result.success(toDeleteResponse(tombstone));
    }

    private void scheduleDeletePoll(
            String topicName,
            long tombstoneVersion,
            long deadlineNanos,
            CompletableFuture<Result<DeleteTopicResponse>> result,
            AtomicReference<Result<DeleteTopicResponse>> latestResult,
            long delayNanos) {
        try {
            waiterExecutor.schedule(
                    () -> dispatchDeletePoll(
                            topicName,
                            tombstoneVersion,
                            deadlineNanos,
                            result,
                            latestResult),
                    Math.max(0L, delayNanos),
                    TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException rejected) {
            result.complete(Result.failure(
                    closed ? ErrorCode.CONTROLLER_NOT_READY : ErrorCode.THROTTLED));
        }
    }

    private void dispatchDeletePoll(
            String topicName,
            long tombstoneVersion,
            long deadlineNanos,
            CompletableFuture<Result<DeleteTopicResponse>> result,
            AtomicReference<Result<DeleteTopicResponse>> latestResult) {
        if (result.isDone()) {
            return;
        }
        try {
            mutationExecutor.execute(() -> pollDelete(
                    topicName,
                    tombstoneVersion,
                    deadlineNanos,
                    result,
                    latestResult));
        } catch (RejectedExecutionException rejected) {
            result.complete(Result.failure(
                    closed ? ErrorCode.CONTROLLER_NOT_READY : ErrorCode.THROTTLED));
        }
    }

    private ScheduledFuture<?> scheduleDeleteDeadline(
            long deadlineNanos,
            CompletableFuture<Result<DeleteTopicResponse>> result,
            AtomicReference<Result<DeleteTopicResponse>> latestResult) {
        long remainingNanos = Math.max(0L, deadlineNanos - nanoTime.getAsLong());
        try {
            return waiterExecutor.schedule(() -> {
                Result<DeleteTopicResponse> latest = latestResult.get();
                result.complete(latest == null
                        ? Result.failure(ErrorCode.CONTROLLER_NOT_READY)
                        : latest);
            }, remainingNanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException rejected) {
            result.complete(Result.failure(
                    closed ? ErrorCode.CONTROLLER_NOT_READY : ErrorCode.THROTTLED));
            return null;
        }
    }

    private void pollDelete(
            String topicName,
            long tombstoneVersion,
            long deadlineNanos,
            CompletableFuture<Result<DeleteTopicResponse>> result,
            AtomicReference<Result<DeleteTopicResponse>> latestResult) {
        if (result.isDone()) {
            return;
        }
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone(topicName);
        if (tombstone == null) {
            result.complete(Result.failure(ErrorCode.UNKNOWN_TOPIC));
            return;
        }
        if (tombstone.tombstoneVersion != tombstoneVersion) {
            result.complete(Result.failure(ErrorCode.STALE_TOMBSTONE));
            return;
        }
        Result<DeleteTopicResponse> current = Result.success(toDeleteResponse(tombstone));
        latestResult.set(current);
        long remainingNanos = deadlineNanos - nanoTime.getAsLong();
        if ("DELETED".equals(tombstone.state) || remainingNanos <= 0L) {
            result.complete(current);
            return;
        }
        scheduleDeletePoll(
                topicName,
                tombstoneVersion,
                deadlineNanos,
                result,
                latestResult,
                Math.min(
                        remainingNanos,
                        TimeUnit.MILLISECONDS.toNanos(DELETE_POLL_INTERVAL_MS)));
    }

    private static ScheduledExecutorService newDeleteWaiterExecutor() {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "topic-delete-waiter");
            thread.setDaemon(true);
            return thread;
        });
    }

    private static ExecutorService newDeleteMutationExecutor(int capacity) {
        return new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity),
                runnable -> {
                    Thread thread = new Thread(runnable, "topic-delete-mutation");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    private TopicPartitionInfo toPartitionInfo(PartitionEntry partition) {
        List<Integer> replicas = partition.replicaBrokerIds == null
                ? List.of() : new ArrayList<>(partition.replicaBrokerIds);
        List<Integer> isr = partition.isrBrokerIds == null
                ? List.of() : new ArrayList<>(partition.isrBrokerIds);
        replicas.sort(Integer::compareTo);
        isr.sort(Integer::compareTo);
        String readiness = partition.frozen
                ? "FROZEN" : partition.leaderReady ? "READY" : "LEADER_NOT_READY";
        return new TopicPartitionInfo(
                partition.partitionId,
                partition.leaderBrokerId,
                partition.leaderEpoch,
                new V4BoundedList<>(replicas.size(), false, replicas),
                new V4BoundedList<>(isr.size(), false, isr),
                readiness,
                partition.frozen);
    }

    private TopicTombstoneInfo toTombstoneInfo(ClusterMetadata.TopicTombstone tombstone) {
        List<Integer> required = sorted(tombstone.requiredBrokerIds);
        List<Integer> acknowledged = sorted(tombstone.acknowledgedBrokerIds);
        List<Integer> pending = required.stream().filter(id -> !acknowledged.contains(id)).toList();
        return new TopicTombstoneInfo(
                tombstone.state,
                tombstone.tombstoneVersion,
                tombstone.deletedAtEpochMs,
                new V4BoundedList<>(required.size(), false, required),
                new V4BoundedList<>(acknowledged.size(), false, acknowledged),
                new V4BoundedList<>(pending.size(), false, pending));
    }

    private DeleteTopicResponse toDeleteResponse(ClusterMetadata.TopicTombstone tombstone) {
        TopicTombstoneInfo info = toTombstoneInfo(tombstone);
        return new DeleteTopicResponse(
                tombstone.topicName,
                info.state(),
                info.tombstoneVersion(),
                info.deletedAtEpochMs(),
                info.requiredBrokerIds(),
                info.acknowledgedBrokerIds(),
                info.pendingBrokerIds());
    }

    private static List<Integer> sorted(java.util.Collection<Integer> values) {
        return values == null ? List.of() : values.stream().sorted().toList();
    }

    private static <T> V4BoundedList<T> bounded(List<T> values, int limit) {
        int total = values.size();
        int count = Math.min(total, limit);
        return new V4BoundedList<>(total, count < total, values.subList(0, count));
    }

    public record Result<T>(ErrorCode errorCode, T response) {
        public static <T> Result<T> success(T response) {
            return new Result<>(ErrorCode.NONE, response);
        }

        public static <T> Result<T> failure(ErrorCode errorCode) {
            return new Result<>(errorCode, null);
        }
    }

    @Override
    public void close() {
        List<CompletableFuture<Result<DeleteTopicResponse>>> waiters;
        synchronized (deleteWaiterAdmission) {
            if (closed) {
                return;
            }
            closed = true;
            waiters = List.copyOf(deleteWaiters);
        }
        for (CompletableFuture<Result<DeleteTopicResponse>> waiter : waiters) {
            waiter.complete(Result.failure(ErrorCode.CONTROLLER_NOT_READY));
        }
        mutationExecutor.shutdownNow();
        waiterExecutor.shutdownNow();
    }
}

package com.minimq.broker.admin;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.ClusterMetadata;
import com.minimq.broker.metadata.ClusterMetadataRepository;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.VersionedClusterMetadata;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.BrokerHeartbeatRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.ConfigureTopicReliabilityRequest;
import com.minimq.protocol.message.ProtocolMessages.DeleteTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.DescribeTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.IncreasePartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.ListTopicsRequest;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.protocol.message.ProtocolMessages.AlterTopicConfigRequest;
import com.minimq.protocol.message.ProtocolMessages.ReassignPartitionsRequest;
import com.minimq.protocol.message.ProtocolMessages.ElectPreferredLeadersRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicAdminServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void createsListsDescribesAndIncreasesPartitionsWithoutRewritingExisting() {
        ClusterController controller = controllerWithBrokers(3);
        TopicAdminService service = new TopicAdminService(controller);

        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 2, 2)).errorCode());
        ClusterMetadata.PartitionEntry firstBefore =
                controller.snapshot().topics.get("orders").partitions.get(0);
        List<Integer> replicasBefore = List.copyOf(firstBefore.replicaBrokerIds);

        assertEquals(ErrorCode.NONE,
                service.increase(new IncreasePartitionsRequest("token", "orders", 5)).errorCode());
        assertEquals(List.of("orders"),
                service.list(new ListTopicsRequest("token", 10)).response().topicNames().items());
        var description = service.describe(new DescribeTopicRequest("token", "orders", 10, false));
        assertEquals(ErrorCode.NONE, description.errorCode());
        assertEquals(5, description.response().partitions().total());
        assertEquals(1, description.response().minInSyncReplicas());
        assertEquals(replicasBefore,
                controller.snapshot().topics.get("orders").partitions.get(0).replicaBrokerIds);
        assertEquals(4, description.response().partitions().items().get(4).partitionId());

        assertEquals(ErrorCode.NONE, service.alterConfig(
                new AlterTopicConfigRequest("token", "orders", 2)).errorCode());
        assertEquals(2, service.describe(new DescribeTopicRequest("token", "orders", 10, false))
                .response().minInSyncReplicas());
        var reassigned = service.reassign(new ReassignPartitionsRequest(
                "token",
                "orders",
                List.of(new com.minimq.protocol.message.ProtocolMessages.PartitionReplicaAssignment(
                        0, List.of(1, 0, 2)))));
        assertEquals(ErrorCode.NONE, reassigned.errorCode());
        assertEquals(List.of(1, 0, 2),
                controller.snapshot().topics.get("orders").partitions.get(0).replicaBrokerIds);
        var elected = service.electPreferred(new ElectPreferredLeadersRequest(
                "token", "orders", List.of()));
        assertEquals(ErrorCode.NONE, elected.errorCode());
        assertEquals(1, controller.snapshot().topics.get("orders").partitions.get(0).leaderBrokerId);
        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.alterConfig(
                new AlterTopicConfigRequest("token", "missing", 1)).errorCode());
        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.reassign(new ReassignPartitionsRequest(
                "token",
                "missing",
                List.of(new com.minimq.protocol.message.ProtocolMessages.PartitionReplicaAssignment(
                        0, List.of(0, 1))))).errorCode());
        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.electPreferred(
                new ElectPreferredLeadersRequest("token", "missing", List.of())).errorCode());
    }

    @Test
    void deletionInstallsTombstoneAndAckCompletesItIdempotently() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());

        var deleting = service.delete(new DeleteTopicRequest("token", "orders"));
        assertEquals(ErrorCode.NONE, deleting.errorCode());
        assertEquals("DELETING", deleting.response().tombstoneState());
        assertEquals(ErrorCode.UNKNOWN_TOPIC,
                service.describe(new DescribeTopicRequest("token", "orders", 10, false)).errorCode());
        assertEquals("DELETING", service.describe(
                new DescribeTopicRequest("token", "orders", 10, true)).response().tombstone().state());

        ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
        ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
        assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                "orders", tombstone.tombstoneVersion, 0, broker.brokerEpoch,
                controller.snapshot().controllerEpoch));
        assertEquals("DELETED", service.describe(
                new DescribeTopicRequest("token", "orders", 10, true)).response().tombstone().state());
        assertTrue(controller.snapshot().topics.isEmpty());
    }

    @Test
    void deletionUsesLogicalMetadataVersionInsteadOfRepositoryRevision() {
        ClusterController controller = new ClusterController(new SkewedRevisionRepository());
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9000)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        long expectedTombstoneVersion = controller.metadataVersion() + 1L;

        assertEquals(ErrorCode.NONE, controller.deleteTopic("orders", 1_000L));

        assertEquals(expectedTombstoneVersion, controller.metadataVersion());
        assertEquals(expectedTombstoneVersion,
                controller.getTopicTombstone("orders").tombstoneVersion);
    }

    @Test
    void deleteWaiterTimesOutAsDeletingWithoutBlockingCaller() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));

        ScheduledThreadPoolExecutor waiterExecutor = new ScheduledThreadPoolExecutor(1);
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
        CompletableFuture.runAsync(() -> { }, mutationExecutor).get(1, TimeUnit.SECONDS);

        try (TopicAdminService service = new TopicAdminService(
                controller,
                java.time.Clock.systemUTC(),
                250L,
                System::nanoTime,
                waiterExecutor,
                mutationExecutor,
                1)) {
            CompletionStage<TopicAdminService.Result<com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse>>
                    stage = service.deleteAndWait(new DeleteTopicRequest("token", "orders"));

            assertFalse(stage.toCompletableFuture().isDone());
            var result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);

            assertEquals(ErrorCode.NONE, result.errorCode());
            assertEquals("DELETING", result.response().tombstoneState());
            assertEquals(List.of(0), result.response().pendingBrokerIds().items());
        }
    }

    @Test
    void deleteWaiterDoesNotRunMetadataMutationOnCallerThread() throws Exception {
        BlockingMetadataRepository repository =
                new BlockingMetadataRepository(new MetadataStore(tempDir));
        ClusterController controller = new ClusterController(repository);
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9000)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        repository.blockNextCompareAndSet();
        ExecutorService caller = Executors.newSingleThreadExecutor();

        try (TopicAdminService service = new TopicAdminService(
                controller, java.time.Clock.systemUTC(), 1_000L)) {
            CompletableFuture<CompletionStage<TopicAdminService.Result<
                    com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse>>> invocation =
                    CompletableFuture.supplyAsync(
                            () -> service.deleteAndWait(new DeleteTopicRequest("token", "orders")),
                            caller);
            assertTrue(repository.awaitBlockedCompare(1, TimeUnit.SECONDS));

            CompletionStage<TopicAdminService.Result<
                    com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse>> stage = null;
            boolean returnedWithoutWaitingForMutation = true;
            try {
                stage = invocation.get(250, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timedOut) {
                returnedWithoutWaitingForMutation = false;
            } finally {
                repository.releaseBlockedCompare();
            }
            assertTrue(returnedWithoutWaitingForMutation,
                    "deleteAndWait blocked its caller on the metadata mutation");
            assertFalse(stage.toCompletableFuture().isDone());

            ClusterMetadata.TopicTombstone tombstone = controller.getTopicTombstone("orders");
            ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
            assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                    "orders",
                    tombstone.tombstoneVersion,
                    0,
                    broker.brokerEpoch,
                    controller.snapshot().controllerEpoch));
            assertEquals(ErrorCode.NONE,
                    stage.toCompletableFuture().get(1, TimeUnit.SECONDS).errorCode());
        } finally {
            repository.releaseBlockedCompare();
            caller.shutdownNow();
        }
    }

    @Test
    void blockedDeleteMetadataCasDoesNotBlockBrokerHeartbeat() throws Exception {
        BlockingMetadataRepository repository =
                new BlockingMetadataRepository(new MetadataStore(tempDir));
        ClusterController controller = new ClusterController(repository);
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9000)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        repository.blockNextCompareAndSet();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        CompletableFuture<ErrorCode> deletion = CompletableFuture.supplyAsync(
                () -> controller.deleteTopic("orders", 1_000L, Duration.ofSeconds(2L)),
                executor);
        try {
            assertTrue(repository.awaitBlockedCompare(1, TimeUnit.SECONDS));
            CompletableFuture<ErrorCode> heartbeat = CompletableFuture.supplyAsync(
                    () -> controller.heartbeatBroker(new BrokerHeartbeatRequest(0, 1L)),
                    executor);

            ErrorCode heartbeatResult = null;
            boolean heartbeatCompleted = true;
            try {
                heartbeatResult = heartbeat.get(250L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timedOut) {
                heartbeatCompleted = false;
            } finally {
                repository.releaseBlockedCompare();
            }

            assertTrue(heartbeatCompleted,
                    "metadata CAS held the lock needed by broker heartbeat");
            assertEquals(ErrorCode.NONE, heartbeatResult);
            assertEquals(ErrorCode.NONE, deletion.get(1, TimeUnit.SECONDS));
        } finally {
            repository.releaseBlockedCompare();
            executor.shutdownNow();
        }
    }

    @Test
    void repeatedDeleteRepairsEmptyDeletingTombstoneAfterTerminalCasFailure() {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterMetadata seeded = new ClusterMetadata();
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        seeded.topics.put(topic.topicName, topic);
        assertEquals(ErrorCode.NONE, store.initialize(seeded).errorCode());
        FailTerminalDeleteOnceRepository repository =
                new FailTerminalDeleteOnceRepository(store, "orders");
        ClusterController controller = new ClusterController(repository);

        assertEquals(
                ErrorCode.CONTROLLER_NOT_READY,
                controller.deleteTopic("orders", 1_000L, Duration.ofSeconds(1L)));
        assertEquals("DELETING", controller.getTopicTombstone("orders").state);

        assertEquals(
                ErrorCode.NONE,
                controller.deleteTopic("orders", 1_000L, Duration.ofSeconds(1L)));
        assertEquals("DELETED", controller.getTopicTombstone("orders").state);
    }

    @Test
    void deleteWaiterDeadlineIncludesTimeQueuedBeforeMutation() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        CountDownLatch mutationThreadStarted = new CountDownLatch(1);
        CountDownLatch releaseMutationThread = new CountDownLatch(1);
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
        mutationExecutor.execute(() -> {
            mutationThreadStarted.countDown();
            try {
                releaseMutationThread.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(mutationThreadStarted.await(1, TimeUnit.SECONDS));

        try (TopicAdminService service = new TopicAdminService(
                controller,
                java.time.Clock.systemUTC(),
                25L,
                System::nanoTime,
                new ScheduledThreadPoolExecutor(1),
                mutationExecutor,
                1)) {
            var stage = service.deleteAndWait(new DeleteTopicRequest("token", "orders"));
            TopicAdminService.Result<
                    com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse> completed = null;
            boolean completedWithinBudget = true;
            try {
                completed = stage.toCompletableFuture().get(250, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timedOut) {
                completedWithinBudget = false;
            } finally {
                releaseMutationThread.countDown();
            }

            assertTrue(completedWithinBudget, "queued metadata mutation ignored the request deadline");
            assertEquals(ErrorCode.CONTROLLER_NOT_READY, completed.errorCode());
            assertTrue(controller.snapshot().topics.containsKey("orders"));
            assertEquals(null, controller.getTopicTombstone("orders"));
        } finally {
            releaseMutationThread.countDown();
        }
    }

    @Test
    void deleteWaiterPassesRemainingBudgetToEveryMetadataCas() throws Exception {
        MetadataStore store = new MetadataStore(tempDir);
        ClusterMetadata seeded = new ClusterMetadata();
        ClusterMetadata.TopicEntry topic = new ClusterMetadata.TopicEntry();
        topic.topicName = "orders";
        seeded.topics.put(topic.topicName, topic);
        assertEquals(ErrorCode.NONE, store.initialize(seeded).errorCode());
        BudgetRecordingRepository repository =
                new BudgetRecordingRepository(store);
        ClusterController controller = new ClusterController(repository);
        repository.requireBudgetedWrites();

        try (TopicAdminService service = new TopicAdminService(
                controller, java.time.Clock.systemUTC(), 250L)) {
            var result = service.deleteAndWait(new DeleteTopicRequest("token", "orders"))
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);

            assertEquals(ErrorCode.NONE, result.errorCode());
            assertEquals("DELETED", result.response().tombstoneState());
            assertEquals(2, repository.budgets().size());
            assertFalse(repository.budgets().get(0).isZero());
            assertFalse(repository.budgets().get(0).isNegative());
            assertFalse(repository.budgets().get(1).isZero());
            assertFalse(repository.budgets().get(1).isNegative());
            assertTrue(repository.budgets().get(1).compareTo(repository.budgets().get(0)) <= 0);
        }
    }

    @Test
    void deleteWaiterDeadlineIncludesTimeWaitingForControllerLock() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        CountDownLatch controllerLocked = new CountDownLatch(1);
        CountDownLatch releaseController = new CountDownLatch(1);
        ExecutorService lockHolder = Executors.newSingleThreadExecutor();
        ExecutorService mutationExecutor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> held = CompletableFuture.runAsync(() -> {
            synchronized (controller) {
                controllerLocked.countDown();
                try {
                    releaseController.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }, lockHolder);
        assertTrue(controllerLocked.await(1, TimeUnit.SECONDS));

        try (TopicAdminService service = new TopicAdminService(
                controller,
                java.time.Clock.systemUTC(),
                25L,
                System::nanoTime,
                new ScheduledThreadPoolExecutor(1),
                mutationExecutor,
                1)) {
            var result = service.deleteAndWait(new DeleteTopicRequest("token", "orders"))
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals(ErrorCode.CONTROLLER_NOT_READY, result.errorCode());

            releaseController.countDown();
            held.get(1, TimeUnit.SECONDS);
            CompletableFuture.runAsync(() -> { }, mutationExecutor)
                    .get(1, TimeUnit.SECONDS);

            assertTrue(controller.snapshot().topics.containsKey("orders"));
            assertEquals(null, controller.getTopicTombstone("orders"));
        } finally {
            releaseController.countDown();
            lockHolder.shutdownNow();
        }
    }

    @Test
    void blockedTombstonePollCannotStarveDeleteDeadline() throws Exception {
        BlockingPollController controller =
                new BlockingPollController(new MetadataStore(tempDir));
        assertEquals(ErrorCode.NONE, controller.registerBroker(
                new RegisterBrokerRequest(0, "127.0.0.1", 9000)).errorCode());
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        controller.blockSecondTombstoneRead();

        try (TopicAdminService service = new TopicAdminService(
                controller, java.time.Clock.systemUTC(), 500L)) {
            var stage = service.deleteAndWait(new DeleteTopicRequest("token", "orders"));
            assertTrue(controller.awaitBlockedPoll(1, TimeUnit.SECONDS));
            TopicAdminService.Result<
                    com.minimq.protocol.message.ProtocolMessages.DeleteTopicResponse> completed = null;
            boolean completedByDeadline = true;
            try {
                completed = stage.toCompletableFuture().get(750, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timedOut) {
                completedByDeadline = false;
            } finally {
                controller.releaseBlockedPoll();
            }

            assertTrue(completedByDeadline, "blocked poll starved the delete deadline");
            assertEquals(ErrorCode.NONE, completed.errorCode());
            assertEquals("DELETING", completed.response().tombstoneState());
        } finally {
            controller.releaseBlockedPoll();
        }
    }

    @Test
    void deleteWaiterCompletesAfterAckAndRepeatedDeleteDoesNotMutate() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "payments", 1, 1)));

        try (TopicAdminService service = new TopicAdminService(
                controller, java.time.Clock.systemUTC(), 1_000L)) {
            var stage = service.deleteAndWait(new DeleteTopicRequest("token", "payments"));
            ClusterMetadata.TopicTombstone deleting = awaitTombstone(controller, "payments");
            ClusterMetadata.BrokerEntry broker = controller.getBroker(0);
            assertEquals(ErrorCode.NONE, controller.acknowledgeTopicDeletion(
                    "payments",
                    deleting.tombstoneVersion,
                    0,
                    broker.brokerEpoch,
                    controller.snapshot().controllerEpoch));

            var completed = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals("DELETED", completed.response().tombstoneState());
            long completedMetadataVersion = controller.metadataVersion();

            var repeated = service.deleteAndWait(new DeleteTopicRequest("token", "payments"))
                    .toCompletableFuture().get(100, TimeUnit.MILLISECONDS);

            assertEquals("DELETED", repeated.response().tombstoneState());
            assertEquals(deleting.tombstoneVersion, repeated.response().tombstoneVersion());
            assertEquals(completedMetadataVersion, controller.metadataVersion());
        }
    }

    @Test
    void closedDeleteWaiterRejectsBeforeInstallingTombstone() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "orders", 1, 1)));
        TopicAdminService service = new TopicAdminService(controller);
        service.close();

        var result = service.deleteAndWait(new DeleteTopicRequest("token", "orders"))
                .toCompletableFuture().get(100, TimeUnit.MILLISECONDS);

        assertEquals(ErrorCode.CONTROLLER_NOT_READY, result.errorCode());
        assertTrue(controller.snapshot().topics.containsKey("orders"));
        assertEquals(null, controller.getTopicTombstone("orders"));
    }

    @Test
    void fastDeleteWaiterFailureCancelsDeadlineTask() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        ScheduledThreadPoolExecutor waiterExecutor = new ScheduledThreadPoolExecutor(1);
        try (TopicAdminService service = new TopicAdminService(
                controller,
                java.time.Clock.systemUTC(),
                60_000L,
                System::nanoTime,
                waiterExecutor,
                Executors.newSingleThreadExecutor(),
                1)) {
            var result = service.deleteAndWait(new DeleteTopicRequest("token", "missing"))
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);

            assertEquals(ErrorCode.UNKNOWN_TOPIC, result.errorCode());
            assertTrue(waiterExecutor.getRemoveOnCancelPolicy());
            assertTrue(waiterExecutor.getQueue().isEmpty(),
                    "completed delete waiters must not leave full-timeout deadline tasks queued");
        }
    }

    @Test
    void fullDeleteWaiterCapacityRejectsBeforeInstallingAnotherTombstone() throws Exception {
        ClusterController controller = controllerWithBrokers(1);
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "first", 1, 1)));
        assertEquals(ErrorCode.NONE, controller.createTopicAdmin(
                new CreateTopicAdminRequest("token", "second", 1, 1)));

        try (TopicAdminService service = new TopicAdminService(
                controller,
                java.time.Clock.systemUTC(),
                10_000L,
                System::nanoTime,
                new ScheduledThreadPoolExecutor(1),
                1)) {
            var first = service.deleteAndWait(new DeleteTopicRequest("token", "first"));
            assertFalse(first.toCompletableFuture().isDone());

            var rejected = service.deleteAndWait(new DeleteTopicRequest("token", "second"))
                    .toCompletableFuture().get(100, TimeUnit.MILLISECONDS);

            assertEquals(ErrorCode.THROTTLED, rejected.errorCode());
            assertTrue(controller.snapshot().topics.containsKey("second"));
            assertEquals(null, controller.getTopicTombstone("second"));
        }
    }

    @Test
    void configuresReliabilityLinksAndClearsThemAtomically() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.retry", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.dlq", 1, 1)).errorCode());

        var configured = service.configureReliability(new ConfigureTopicReliabilityRequest(
                "token", "orders", true, "orders.retry", false, true, "orders.dlq", false));

        assertEquals(ErrorCode.NONE, configured.errorCode());
        assertEquals("orders.retry", configured.response().retryTopicName());
        assertEquals("orders.dlq", configured.response().deadLetterTopicName());
        var described = service.describe(new DescribeTopicRequest("token", "orders", 10, false));
        assertEquals("orders.retry", described.response().retryTopic());
        assertEquals("orders.dlq", described.response().deadLetterTopic());

        var cleared = service.configureReliability(new ConfigureTopicReliabilityRequest(
                "token", "orders", false, null, true, false, null, true));

        assertEquals(ErrorCode.NONE, cleared.errorCode());
        assertEquals(null, cleared.response().retryTopicName());
        assertEquals(null, cleared.response().deadLetterTopicName());
        assertEquals(null, controller.snapshot().retryTopicBySource.get("orders"));
        assertEquals(null, controller.snapshot().deadLetterTopicBySource.get("orders"));
    }

    @Test
    void rejectsReliabilityLinksToInactiveSelfOrCyclicTargets() {
        ClusterController controller = controllerWithBrokers(1);
        TopicAdminService service = new TopicAdminService(controller);
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders", 1, 1)).errorCode());
        assertEquals(ErrorCode.NONE, service.create(new CreateTopicAdminRequest(
                "token", "orders.retry", 1, 1)).errorCode());

        assertEquals(ErrorCode.UNKNOWN_TOPIC, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "missing", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "orders", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.NONE, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders", true, "orders.retry", false, false, null, false)).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, service.configureReliability(
                new ConfigureTopicReliabilityRequest(
                        "token", "orders.retry", true, "orders", false, false, null, false)).errorCode());
    }

    private ClusterController controllerWithBrokers(int count) {
        ClusterController controller = new ClusterController(new MetadataStore(tempDir));
        for (int brokerId = 0; brokerId < count; brokerId++) {
            assertEquals(ErrorCode.NONE, controller.registerBroker(
                    new RegisterBrokerRequest(brokerId, "127.0.0.1", 9000 + brokerId)).errorCode());
        }
        return controller;
    }

    private static ClusterMetadata.TopicTombstone awaitTombstone(
            ClusterController controller, String topicName) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        ClusterMetadata.TopicTombstone tombstone;
        while ((tombstone = controller.getTopicTombstone(topicName)) == null
                && System.nanoTime() < deadlineNanos) {
            Thread.sleep(1L);
        }
        if (tombstone == null) {
            throw new AssertionError("topic tombstone was not installed within one second");
        }
        return tombstone;
    }

    private static final class SkewedRevisionRepository implements ClusterMetadataRepository {
        private ClusterMetadata metadata = new ClusterMetadata();
        private long revision = 100L;

        @Override
        public VersionedClusterMetadata read() {
            return new VersionedClusterMetadata(metadata, revision);
        }

        @Override
        public VersionedClusterMetadata read(Duration timeout) {
            return read();
        }

        @Override
        public UpdateResult initialize(ClusterMetadata candidate) {
            return new UpdateResult(ErrorCode.NONE, read());
        }

        @Override
        public UpdateResult initialize(ClusterMetadata candidate, Duration timeout) {
            return initialize(candidate);
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata candidate) {
            if (expectedRevision != revision) {
                return new UpdateResult(ErrorCode.CONTROLLER_FENCED, read());
            }
            candidate.metadataVersion = metadata.metadataVersion + 1L;
            metadata = candidate;
            revision += 10L;
            return new UpdateResult(ErrorCode.NONE, read());
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata candidate, Duration timeout) {
            return compareAndSet(expectedRevision, candidate);
        }
    }

    private static final class BlockingMetadataRepository implements ClusterMetadataRepository {
        private final ClusterMetadataRepository delegate;
        private final AtomicBoolean blockNextCompare = new AtomicBoolean();
        private final CountDownLatch blockedCompare = new CountDownLatch(1);
        private final CountDownLatch releaseCompare = new CountDownLatch(1);

        private BlockingMetadataRepository(ClusterMetadataRepository delegate) {
            this.delegate = delegate;
        }

        private void blockNextCompareAndSet() {
            blockNextCompare.set(true);
        }

        private boolean awaitBlockedCompare(long timeout, TimeUnit unit) throws InterruptedException {
            return blockedCompare.await(timeout, unit);
        }

        private void releaseBlockedCompare() {
            releaseCompare.countDown();
        }

        @Override
        public VersionedClusterMetadata read() {
            return delegate.read();
        }

        @Override
        public VersionedClusterMetadata read(Duration timeout) {
            return delegate.read(timeout);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata) {
            return delegate.initialize(metadata);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata, Duration timeout) {
            return delegate.initialize(metadata, timeout);
        }

        private void maybeBlock() {
            if (!blockNextCompare.compareAndSet(true, false)) {
                return;
            }
            blockedCompare.countDown();
            try {
                releaseCompare.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("blocked metadata mutation interrupted", interrupted);
            }
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
            maybeBlock();
            return delegate.compareAndSet(expectedRevision, metadata);
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata metadata, Duration timeout) {
            maybeBlock();
            return delegate.compareAndSet(expectedRevision, metadata, timeout);
        }
    }

    private static final class FailTerminalDeleteOnceRepository
            implements ClusterMetadataRepository {
        private final ClusterMetadataRepository delegate;
        private final String topicName;
        private final AtomicBoolean failTerminalWrite = new AtomicBoolean(true);

        private FailTerminalDeleteOnceRepository(
                ClusterMetadataRepository delegate, String topicName) {
            this.delegate = delegate;
            this.topicName = topicName;
        }

        @Override
        public VersionedClusterMetadata read() {
            return delegate.read();
        }

        @Override
        public VersionedClusterMetadata read(Duration timeout) {
            return delegate.read(timeout);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata) {
            return delegate.initialize(metadata);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata, Duration timeout) {
            return delegate.initialize(metadata, timeout);
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
            return compareAndSetInternal(expectedRevision, metadata, null);
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata metadata, Duration timeout) {
            return compareAndSetInternal(expectedRevision, metadata, timeout);
        }

        private UpdateResult compareAndSetInternal(
                long expectedRevision, ClusterMetadata metadata, Duration timeout) {
            ClusterMetadata.TopicTombstone tombstone = metadata.topicTombstones.get(topicName);
            if (tombstone != null
                    && "DELETED".equals(tombstone.state)
                    && failTerminalWrite.compareAndSet(true, false)) {
                return new UpdateResult(ErrorCode.CONTROLLER_NOT_READY, delegate.read());
            }
            return timeout == null
                    ? delegate.compareAndSet(expectedRevision, metadata)
                    : delegate.compareAndSet(expectedRevision, metadata, timeout);
        }
    }

    private static final class BudgetRecordingRepository implements ClusterMetadataRepository {
        private final ClusterMetadataRepository delegate;
        private final List<Duration> budgets = new ArrayList<>();
        private boolean budgetRequired;

        private BudgetRecordingRepository(ClusterMetadataRepository delegate) {
            this.delegate = delegate;
        }

        private void requireBudgetedWrites() {
            budgetRequired = true;
        }

        private List<Duration> budgets() {
            return List.copyOf(budgets);
        }

        @Override
        public VersionedClusterMetadata read() {
            return delegate.read();
        }

        @Override
        public VersionedClusterMetadata read(Duration timeout) {
            return delegate.read(timeout);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata) {
            return delegate.initialize(metadata);
        }

        @Override
        public UpdateResult initialize(ClusterMetadata metadata, Duration timeout) {
            return delegate.initialize(metadata, timeout);
        }

        @Override
        public UpdateResult compareAndSet(long expectedRevision, ClusterMetadata metadata) {
            if (budgetRequired) {
                throw new IllegalStateException("metadata CAS did not receive a request budget");
            }
            return delegate.compareAndSet(expectedRevision, metadata);
        }

        @Override
        public UpdateResult compareAndSet(
                long expectedRevision, ClusterMetadata metadata, Duration timeout) {
            budgets.add(timeout);
            return delegate.compareAndSet(expectedRevision, metadata, timeout);
        }
    }

    private static final class BlockingPollController extends ClusterController {
        private final AtomicInteger tombstoneReads = new AtomicInteger();
        private final CountDownLatch blockedPoll = new CountDownLatch(1);
        private final CountDownLatch releasePoll = new CountDownLatch(1);
        private volatile boolean blockPoll;

        private BlockingPollController(ClusterMetadataRepository repository) {
            super(repository);
        }

        private void blockSecondTombstoneRead() {
            tombstoneReads.set(0);
            blockPoll = true;
        }

        private boolean awaitBlockedPoll(long timeout, TimeUnit unit) throws InterruptedException {
            return blockedPoll.await(timeout, unit);
        }

        private void releaseBlockedPoll() {
            releasePoll.countDown();
        }

        @Override
        public synchronized ClusterMetadata.TopicTombstone getTopicTombstone(String topicName) {
            if (blockPoll && tombstoneReads.incrementAndGet() == 2) {
                blockedPoll.countDown();
                try {
                    releasePoll.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.getTopicTombstone(topicName);
        }
    }
}

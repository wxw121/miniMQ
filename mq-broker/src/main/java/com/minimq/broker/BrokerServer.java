package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.BrokerLeaseMonitor;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.auth.OperatorAuthorizer;
import com.minimq.broker.audit.FailoverAuditLog;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.replication.ReplicaFetcher;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.replication.ReplicaStateReporter;
import com.minimq.broker.admin.TopicDeletionReconciler;
import com.minimq.broker.admin.TopicAdminService;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.SwitchableClusterMetadataRepository;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.metrics.BrokerRuntimeMetricsSnapshot;
import com.minimq.broker.status.BrokerStatusService;
import com.minimq.broker.netty.NettyTransport;
import com.minimq.broker.netty.NettyTransportFactory;
import com.minimq.broker.resilience.RequestThrottler;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.storage.LogManagerConfig;
import com.minimq.storage.LogManager;
import com.minimq.storage.CompactionPolicy;
import com.minimq.storage.LogMaintenanceConfig;
import com.minimq.storage.RetentionPolicy;
import com.minimq.storage.SegmentedLogConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker 网络服务入口，负责启动 Netty、注册 Controller，并暴露 MiniMQ 协议处理链。
 */
public class BrokerServer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);
    private static final long STARTUP_BARRIER_TIMEOUT_MS = 15_000L;

    private final BrokerConfig config;
    private final long startedAtEpochMs;
    private final FetchTransferMetrics fetchTransferMetrics = new FetchTransferMetrics();
    private final BrokerRuntimeMetrics runtimeMetrics;
    private final RequestThrottler requestThrottler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private NettyTransport transport;
    private ChannelFuture channelFuture;
    private PartitionManager partitionManager;
    private ClusterController clusterController;
    private ReplicaFetcher replicaFetcher;
    private ReplicaStateReporter replicaStateReporter;
    private MaintenanceAdminService maintenanceAdminService;
    private OperatorAuthorizer operatorAuthorizer;
    private final BrokerAvailabilityGuard availabilityGuard = new BrokerAvailabilityGuard();
    private BrokerLeaseService brokerLeaseService;
    private BrokerLeaseMonitor brokerLeaseMonitor;
    private ControllerRuntime controllerRuntime;
    private ControllerRole controllerRole;
    private BrokerStatusService brokerStatusService;
    private TopicDeletionReconciler topicDeletionReconciler;
    private TopicAdminService topicAdminService;
    private final FailoverAuditLog failoverAuditLog;
    private boolean stopped;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
        this.requestThrottler = new RequestThrottler(config.maxInFlightRequests());
        this.startedAtEpochMs = System.currentTimeMillis();
        this.runtimeMetrics = new BrokerRuntimeMetrics(() -> startedAtEpochMs);
        this.failoverAuditLog = new FailoverAuditLog(
                config.brokerId(), System::currentTimeMillis, startedAtEpochMs);
    }

    public FetchTransferMetricsSnapshot fetchTransferMetrics() {
        return fetchTransferMetrics.snapshot();
    }

    public BrokerRuntimeMetricsSnapshot runtimeMetrics() {
        return runtimeMetrics.snapshot();
    }

    public long localHighWatermark(String topic, int partition) {
        return partitionManager.highWatermark(topic, partition);
    }

    public int localLeaderBrokerId(String topic, int partition) {
        return clusterController.getLeaderBrokerId(topic, partition);
    }

    public BrokerAvailabilityGuard.State availabilityState() {
        return availabilityGuard.state();
    }

    public long brokerEpoch() {
        return brokerLeaseService != null ? brokerLeaseService.brokerEpoch() : -1L;
    }

    public void start() throws InterruptedException {
        MetadataStore metadataStore = new MetadataStore(config.dataDir(), config.recoverCorruptMetadata());
        boolean etcdRuntime = !config.usesLegacyControllerMode();
        SwitchableClusterMetadataRepository switchableRepository = etcdRuntime
                ? new SwitchableClusterMetadataRepository(metadataStore) : null;
        clusterController = new ClusterController(
                etcdRuntime ? switchableRepository : metadataStore,
                config.controllerBrokerLeaseTimeoutMs(),
                failoverAuditLog);
        controllerRole = ControllerRole.legacy(config);
        if (etcdRuntime) {
            controllerRuntime = new ControllerRuntime(
                    config,
                    clusterController,
                    switchableRepository,
                    this::activateControllerMonitor,
                    failoverAuditLog);
            controllerRole = controllerRuntime;
        }
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        ReplicaStateManager replicaStateManager = new ReplicaStateManager();
        LogManager logManager = new LogManager(
                config.dataDir(),
                new LogManagerConfig(
                        config.flushPolicy(),
                        config.checkpointEveryNAppends(),
                        new SegmentedLogConfig(
                                config.segmentMaxBytes(),
                                config.sparseIndexIntervalRecords()),
                        LogMaintenanceConfig.of(
                                RetentionPolicy.of(config.retentionMs(), config.retentionBytes()),
                                config.compactionEnabled()
                                        ? CompactionPolicy.enabled(
                                                config.compactionMaxSegments(),
                                                config.compactionMaxBytes())
                                        : CompactionPolicy.disabled(),
                                config.maintenanceEveryNAppends(),
                                config.maintenanceWindowStartHour(),
                                config.maintenanceWindowEndHour()),
                        config.maxScheduledRecords()));
        partitionManager = new PartitionManager(logManager);
        topicDeletionReconciler = new TopicDeletionReconciler(
                config, clusterController, partitionManager, availabilityGuard, controllerRole);
        topicAdminService = new TopicAdminService(
                clusterController, config.topicDeleteTimeoutMs());
        operatorAuthorizer = OperatorAuthorizer.fromConfig(config);
        maintenanceAdminService = new MaintenanceAdminService(operatorAuthorizer, partitionManager);
        replicaStateReporter = new ReplicaStateReporter(
                config, clusterController, availabilityGuard, controllerRole);
        replicaFetcher = new ReplicaFetcher(
                config,
                clusterController,
                partitionManager,
                availabilityGuard,
                controllerRole,
                replicaStateReporter);
        brokerLeaseService = new BrokerLeaseService(
                config, clusterController, availabilityGuard, controllerRole, failoverAuditLog);
        brokerLeaseService.holdActivation();
        brokerStatusService = new BrokerStatusService(
                config,
                clusterController,
                availabilityGuard,
                controllerRole,
                System::currentTimeMillis,
                startedAtEpochMs);
        if (config.controller()) {
            brokerLeaseMonitor = new BrokerLeaseMonitor(
                    clusterController,
                    config.brokerHeartbeatIntervalMs(),
                    config.leaderActivationTimeoutMs(),
                    controllerRole::isActive,
                    failoverAuditLog);
        }

        transport = new NettyTransportFactory().create(config.networkTransportMode());
        bossGroup = transport.bossGroup();
        workerGroup = transport.workerGroup();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(transport.serverChannelClass())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new MqDecoder())
                                .addLast(new MqRequestHandler(
                                        config,
                                        clusterController,
                                        groupCoordinator,
                                        partitionManager,
                                        replicaStateManager,
                                        fetchTransferMetrics,
                                        replicaStateReporter,
                                        availabilityGuard,
                                        maintenanceAdminService,
                                        controllerRole,
                                        operatorAuthorizer,
                                        runtimeMetrics,
                                        requestThrottler,
                                        brokerStatusService,
                                        failoverAuditLog,
                                        topicAdminService))
                                .addLast(new MqEncoder());
                    }
                });

        try {
            channelFuture = bootstrap.bind(config.host(), config.port()).sync();
            if (controllerRuntime != null) {
                controllerRuntime.start();
                controllerRuntime.awaitInitialMetadata(STARTUP_BARRIER_TIMEOUT_MS);
                initializeEtcdBrokerLease();
            } else {
                brokerLeaseService.initialize();
                if (brokerLeaseMonitor != null) {
                    brokerLeaseMonitor.start();
                }
            }
            brokerLeaseService.start();
            awaitInitialTopicReconciliation();
            brokerLeaseService.activateAfterStartupBarrier();
            topicDeletionReconciler.start();
            replicaFetcher.start();
        } catch (InterruptedException failure) {
            cleanupAfterFailedStart(failure);
            throw failure;
        } catch (Exception failure) {
            cleanupAfterFailedStart(failure);
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new IllegalStateException("Broker startup failed", failure);
        } catch (Error failure) {
            cleanupAfterFailedStart(failure);
            throw failure;
        }
        log.info("Broker {} started on {}:{} controller={}", config.brokerId(), config.host(), config.port(),
                controllerRole.isActive());
    }

    private void initializeEtcdBrokerLease() throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(STARTUP_BARRIER_TIMEOUT_MS);
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                brokerLeaseService.initialize();
                return;
            } catch (RuntimeException failure) {
                lastFailure = failure;
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("Controller discovery timed out during Broker registration", lastFailure);
    }

    private void awaitInitialTopicReconciliation() throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(STARTUP_BARRIER_TIMEOUT_MS);
        while (true) {
            topicDeletionReconciler.reconcileOnce(deadline);
            if (availabilityGuard.hasStickyStorageFence()) {
                throw new IllegalStateException("Local topic deletion reconciliation failed");
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Initial topic deletion reconciliation timed out");
            }
            if (topicDeletionReconciler.isCaughtUp()) {
                return;
            }
            long remainingNanos = deadline - System.nanoTime();
            long sleepMillis = Math.min(
                    100L,
                    Math.max(1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            Thread.sleep(sleepMillis);
        }
    }

    private synchronized void activateControllerMonitor() {
        if (brokerLeaseMonitor == null) {
            brokerLeaseMonitor = new BrokerLeaseMonitor(
                    clusterController, config.brokerHeartbeatIntervalMs(),
                    config.leaderActivationTimeoutMs(), controllerRole::isActive,
                    failoverAuditLog);
            brokerLeaseMonitor.start();
        }
    }

    private void cleanupAfterFailedStart(Throwable failure) {
        try {
            stop();
        } catch (InterruptedException cleanupFailure) {
            Thread.currentThread().interrupt();
            failure.addSuppressed(cleanupFailure);
        }
    }

    public synchronized void stop() throws InterruptedException {
        if (stopped) {
            return;
        }
        if (brokerLeaseService != null) {
            brokerLeaseService.close();
        }
        if (controllerRuntime != null) {
            controllerRuntime.close();
        }
        if (brokerLeaseMonitor != null) {
            brokerLeaseMonitor.close();
        }
        if (replicaFetcher != null) {
            replicaFetcher.close();
        }
        if (topicDeletionReconciler != null) {
            topicDeletionReconciler.close();
        }
        if (topicAdminService != null) {
            topicAdminService.close();
        }
        if (replicaStateReporter != null) {
            replicaStateReporter.close();
        }
        if (channelFuture != null) {
            channelFuture.channel().close().sync();
        }
        if (maintenanceAdminService != null) {
            maintenanceAdminService.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().sync();
        }
        if (partitionManager != null) {
            partitionManager.close();
        }
        stopped = true;
    }
}

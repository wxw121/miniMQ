package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.BrokerLeaseMonitor;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.replication.ReplicaFetcher;
import com.minimq.broker.replication.ReplicaStateManager;
import com.minimq.broker.replication.ReplicaLeoReporter;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.broker.metadata.SwitchableClusterMetadataRepository;
import com.minimq.broker.metrics.BrokerRuntimeMetrics;
import com.minimq.broker.metrics.BrokerRuntimeMetricsSnapshot;
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

    private final BrokerConfig config;
    private final FetchTransferMetrics fetchTransferMetrics = new FetchTransferMetrics();
    private final BrokerRuntimeMetrics runtimeMetrics = new BrokerRuntimeMetrics();
    private final RequestThrottler requestThrottler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private NettyTransport transport;
    private ChannelFuture channelFuture;
    private PartitionManager partitionManager;
    private ClusterController clusterController;
    private ReplicaFetcher replicaFetcher;
    private ReplicaLeoReporter replicaLeoReporter;
    private MaintenanceAdminService maintenanceAdminService;
    private final BrokerAvailabilityGuard availabilityGuard = new BrokerAvailabilityGuard();
    private BrokerLeaseService brokerLeaseService;
    private BrokerLeaseMonitor brokerLeaseMonitor;
    private ControllerRuntime controllerRuntime;
    private ControllerRole controllerRole;
    private boolean stopped;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
        this.requestThrottler = new RequestThrottler(config.maxInFlightRequests());
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
                config.controllerBrokerLeaseTimeoutMs());
        controllerRole = ControllerRole.legacy(config);
        if (etcdRuntime) {
            controllerRuntime = new ControllerRuntime(
                    config, clusterController, switchableRepository, this::activateControllerMonitor);
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
                                config.maintenanceWindowEndHour())));
        partitionManager = new PartitionManager(logManager);
        maintenanceAdminService = new MaintenanceAdminService(config.adminToken(), partitionManager);
        replicaFetcher = new ReplicaFetcher(
                config, clusterController, partitionManager, availabilityGuard, controllerRole);
        replicaLeoReporter = new ReplicaLeoReporter(
                config, clusterController, availabilityGuard, controllerRole);
        brokerLeaseService = new BrokerLeaseService(
                config, clusterController, availabilityGuard, controllerRole);
        if (config.controller()) {
            brokerLeaseMonitor = new BrokerLeaseMonitor(
                    clusterController,
                    config.brokerHeartbeatIntervalMs(),
                    config.leaderActivationTimeoutMs(),
                    controllerRole::isActive);
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
                                        replicaLeoReporter,
                                        availabilityGuard,
                                        maintenanceAdminService,
                                        controllerRole,
                                        runtimeMetrics,
                                        requestThrottler))
                                .addLast(new MqEncoder());
                    }
                });

        try {
            channelFuture = bootstrap.bind(config.host(), config.port()).sync();
            if (controllerRuntime != null) {
                controllerRuntime.start();
                initializeEtcdBrokerLease();
            } else {
                brokerLeaseService.initialize();
                if (brokerLeaseMonitor != null) {
                    brokerLeaseMonitor.start();
                }
            }
            brokerLeaseService.start();
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
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
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

    private synchronized void activateControllerMonitor() {
        if (brokerLeaseMonitor == null) {
            brokerLeaseMonitor = new BrokerLeaseMonitor(
                    clusterController, config.brokerHeartbeatIntervalMs());
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
        if (replicaLeoReporter != null) {
            replicaLeoReporter.close();
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

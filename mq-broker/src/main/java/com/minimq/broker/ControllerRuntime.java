package com.minimq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minimq.broker.audit.FailoverAuditEventType;
import com.minimq.broker.audit.FailoverAuditLog;
import com.minimq.broker.coordination.ControllerElectionService;
import com.minimq.broker.coordination.ControllerIdentity;
import com.minimq.broker.coordination.EtcdClientFactory;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.metadata.EtcdClusterMetadataRepository;
import com.minimq.broker.metadata.MetadataWatchService;
import com.minimq.broker.metadata.SwitchableClusterMetadataRepository;
import com.minimq.common.error.ErrorCode;
import io.etcd.jetcd.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the production etcd election, fenced repository and monotonic metadata watch. */
public final class ControllerRuntime implements ControllerRole, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControllerRuntime.class);

    private final BrokerConfig config;
    private final ClusterController controller;
    private final SwitchableClusterMetadataRepository repository;
    private final Client client;
    private final EtcdControllerCoordinationStore coordination;
    private final ControllerElectionService election;
    private final MetadataWatchService watch;
    private final Function<com.minimq.broker.coordination.ControllerCoordinationStore.Leadership,
            com.minimq.broker.metadata.ClusterMetadataRepository> repositoryFactory;
    private final Runnable activationListener;
    private final FailoverAuditLog auditLog;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private volatile boolean active;

    public ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            Runnable activationListener) {
        this(config, controller, repository, activationListener, null);
    }

    public ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            Runnable activationListener,
            FailoverAuditLog auditLog) {
        this.config = config;
        this.controller = controller;
        this.repository = repository;
        this.activationListener = activationListener;
        this.auditLog = auditLog;
        this.client = EtcdClientFactory.create(config.etcdEndpoints());
        this.coordination = new EtcdControllerCoordinationStore(client, config.clusterId());
        this.election = new ControllerElectionService(
                coordination, config.brokerId(), config.host(), config.port(),
                config.controllerSessionTtlSeconds());
        EtcdClusterMetadataRepository reader = new EtcdClusterMetadataRepository(
                client.getKVClient(), config.clusterId(), new byte[] {0}, new ObjectMapper());
        this.watch = new MetadataWatchService(
                reader, client.getWatchClient(), config.clusterId(), controller::installMetadata);
        this.repositoryFactory = leadership -> new EtcdClusterMetadataRepository(
                client.getKVClient(), config.clusterId(), leadership);
        this.thread = new Thread(this::campaign, "controller-election-" + config.brokerId());
        this.thread.setDaemon(true);
    }

    public ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            FailoverAuditLog auditLog,
            Runnable activationListener) {
        this(config, controller, repository, activationListener, auditLog);
    }

    ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            Client client,
            EtcdControllerCoordinationStore coordination,
            ControllerElectionService election,
            MetadataWatchService watch,
            Function<com.minimq.broker.coordination.ControllerCoordinationStore.Leadership,
                    com.minimq.broker.metadata.ClusterMetadataRepository> repositoryFactory,
            Runnable activationListener) {
        this(config, controller, repository, client, coordination, election, watch,
                repositoryFactory, activationListener, null);
    }

    ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            Client client,
            EtcdControllerCoordinationStore coordination,
            ControllerElectionService election,
            MetadataWatchService watch,
            Function<com.minimq.broker.coordination.ControllerCoordinationStore.Leadership,
                    com.minimq.broker.metadata.ClusterMetadataRepository> repositoryFactory,
            Runnable activationListener,
            FailoverAuditLog auditLog) {
        this.config = config;
        this.controller = controller;
        this.repository = repository;
        this.client = client;
        this.coordination = coordination;
        this.election = election;
        this.watch = watch;
        this.repositoryFactory = repositoryFactory;
        this.activationListener = activationListener;
        this.auditLog = auditLog;
        this.thread = new Thread(this::campaign, "controller-election-" + config.brokerId());
        this.thread.setDaemon(true);
    }

    public void start() {
        watch.start();
        thread.start();
    }

    public long awaitInitialMetadata(long timeoutMs) throws InterruptedException {
        try {
            return watch.initialLoad().toCompletableFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("Initial metadata load timed out", timeout);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Initial metadata load failed", cause);
        }
    }

    private void campaign() {
        while (!closed.get()) {
            try {
                campaignOnce();
                while (!closed.get()
                        && election.state() == com.minimq.broker.coordination.ControllerState.ACTIVE) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException failure) {
                if (!closed.get()) {
                    log.error("Controller runtime failed on broker {}; retrying election",
                            config.brokerId(), failure);
                }
            } finally {
                active = false;
                election.close();
            }
            if (!closed.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    void campaignOnce() {
            var leadership = election.campaign();
            var fenced = repositoryFactory.apply(leadership);
            var shared = fenced.read();
            if (shared.revision() == 0) {
                if (!config.initializeEmptyCluster() && !config.initializeClusterFromLocal()) {
                    throw new IllegalStateException(
                            "etcd metadata is empty; explicit cluster initialization is required");
                }
                var initialized = fenced.initialize(controller.snapshot());
                if (initialized.errorCode() != ErrorCode.NONE) {
                    throw new IllegalStateException("failed to initialize etcd metadata: "
                            + initialized.errorCode());
                }
            }
            repository.switchTo(fenced);
            controller.installMetadata(fenced.read());
            controller.updateMetadata(metadata -> {
                metadata.controllerEpoch = leadership.identity().controllerEpoch();
                return null;
            });
            controller.beginRecovery(config.controllerFailoverGraceMs());
            election.activate();
            active = true;
            audit(FailoverAuditEventType.CONTROLLER_TAKEOVER,
                    config.brokerId(), null, null, "inactive", "active",
                    "controller epoch " + leadership.identity().controllerEpoch());
            activationListener.run();
    }

    @Override
    public boolean isActive() {
        return active && election.state() == com.minimq.broker.coordination.ControllerState.ACTIVE;
    }

    @Override
    public ControllerIdentity currentLeader() {
        return coordination.currentLeader();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        active = false;
        thread.interrupt();
        client.close();
        watch.close();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        election.close();
        coordination.close();
    }

    private void audit(
            FailoverAuditEventType eventType,
            Integer brokerId,
            String topicName,
            Integer partitionId,
            String oldValue,
            String newValue,
            String detail) {
        if (auditLog == null) {
            return;
        }
        try {
            auditLog.append(eventType, brokerId, topicName, partitionId, oldValue, newValue, detail);
        } catch (RuntimeException ignored) {
            // Audit is best-effort and cannot make Controller activation fail.
        }
    }
}

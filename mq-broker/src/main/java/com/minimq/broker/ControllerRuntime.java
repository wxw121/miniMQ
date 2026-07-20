package com.minimq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final Runnable activationListener;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private volatile boolean active;

    public ControllerRuntime(
            BrokerConfig config,
            ClusterController controller,
            SwitchableClusterMetadataRepository repository,
            Runnable activationListener) {
        this.config = config;
        this.controller = controller;
        this.repository = repository;
        this.activationListener = activationListener;
        this.client = EtcdClientFactory.create(config.etcdEndpoints());
        this.coordination = new EtcdControllerCoordinationStore(client, config.clusterId());
        this.election = new ControllerElectionService(
                coordination, config.brokerId(), config.host(), config.port(),
                config.controllerSessionTtlSeconds());
        EtcdClusterMetadataRepository reader = new EtcdClusterMetadataRepository(
                client.getKVClient(), config.clusterId(), new byte[] {0}, new ObjectMapper());
        this.watch = new MetadataWatchService(
                reader, client.getWatchClient(), config.clusterId(), controller::installMetadata);
        this.thread = new Thread(this::campaign, "controller-election-" + config.brokerId());
        this.thread.setDaemon(true);
    }

    public void start() {
        watch.start();
        thread.start();
    }

    private void campaign() {
        try {
            var leadership = election.campaign();
            EtcdClusterMetadataRepository fenced = new EtcdClusterMetadataRepository(
                    client.getKVClient(), config.clusterId(), leadership);
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
            activationListener.run();
            while (!closed.get() && election.state() == com.minimq.broker.coordination.ControllerState.ACTIVE) {
                Thread.sleep(100);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException failure) {
            if (!closed.get()) {
                log.error("Controller runtime failed on broker {}", config.brokerId(), failure);
            }
        } finally {
            active = false;
        }
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
        election.close();
        watch.close();
        coordination.close();
        client.close();
        thread.interrupt();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.minimq.broker;

import com.minimq.broker.coordination.ControllerElectionService;
import com.minimq.broker.coordination.ControllerState;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.support.EtcdTestContainer;
import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.broker.support.TestNetworkClient;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerRequest;
import com.minimq.protocol.message.ProtocolMessages.DiscoverControllerResponse;
import com.minimq.protocol.message.ProtocolMessages.MetadataRequest;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = false)
class ControllerFailoverIT {
    @Container
    static final EtcdTestContainer ETCD = new EtcdTestContainer();

    @Test
    void electsOneControllerThenAdvancesEpochAfterFailover() throws Exception {
        List<Client> clients = new ArrayList<>();
        List<ControllerElectionService> elections = new ArrayList<>();
        List<CompletableFuture<Long>> campaigns = new ArrayList<>();
        try {
            for (int brokerId = 0; brokerId < 3; brokerId++) {
                Client client = ETCD.newClient();
                clients.add(client);
                ControllerElectionService election = new ControllerElectionService(
                        new EtcdControllerCoordinationStore(client, "controller-failover-it"),
                        brokerId, "127.0.0.1", 19090 + brokerId, 3);
                elections.add(election);
                campaigns.add(CompletableFuture.supplyAsync(
                        () -> election.campaign().identity().controllerEpoch()));
            }

            CompletableFuture.anyOf(campaigns.toArray(CompletableFuture[]::new))
                    .get(10, TimeUnit.SECONDS);
            int first = completedIndex(campaigns);
            assertEquals(1, campaigns.stream().filter(CompletableFuture::isDone).count());
            elections.get(first).activate();
            assertEquals(ControllerState.ACTIVE, elections.get(first).state());
            assertEquals(1L, campaigns.get(first).join());

            elections.get(first).close();
            CompletableFuture.anyOf(campaigns.stream()
                            .filter(future -> !future.isDone())
                            .toArray(CompletableFuture[]::new))
                    .get(10, TimeUnit.SECONDS);
            int second = completedIndexExcluding(campaigns, first);
            elections.get(second).activate();
            assertEquals(2L, campaigns.get(second).join());
            assertTrue(elections.get(second).leadership().valid());

            elections.get(second).close();
            int third = 3 - first - second;
            assertEquals(3L, campaigns.get(third).get(10, TimeUnit.SECONDS));
            elections.get(third).activate();
            elections.get(third).close();
        } finally {
            elections.forEach(ControllerElectionService::close);
            clients.forEach(Client::close);
        }
    }

    @Test
    void brokerRuntimeDiscoversAndTakesOverControllerRole() throws Exception {
        String clusterId = "broker-runtime-" + System.nanoTime();
        int port0 = unusedPort();
        int port1 = unusedPort();
        BrokerConfig firstConfig = runtimeConfig(0, port0, clusterId, true);
        BrokerConfig secondConfig = runtimeConfig(1, port1, clusterId, false);
        try (BrokerTestHarness first = BrokerTestHarness.start(firstConfig);
             BrokerTestHarness second = BrokerTestHarness.start(secondConfig);
             TestNetworkClient client = new TestNetworkClient()) {
            client.connect("127.0.0.1", second.port());
            assertEquals(0, discover(client).controller().brokerId());

            first.close();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            DiscoverControllerResponse discovered = discover(client);
            while (discovered.controller().brokerId() != 1 && System.nanoTime() < deadline) {
                Thread.sleep(20);
                discovered = discover(client);
            }
            assertEquals(1, discovered.controller().brokerId());

            var metadata = client.send(ApiKeys.METADATA, new MetadataRequest(""));
            while (metadata.header().errorCode() != com.minimq.common.error.ErrorCode.NONE
                    && System.nanoTime() < deadline) {
                Thread.sleep(20);
                metadata = client.send(ApiKeys.METADATA, new MetadataRequest(""));
            }
            assertEquals(com.minimq.common.error.ErrorCode.NONE, metadata.header().errorCode());
        }
    }

    private static BrokerConfig runtimeConfig(
            int brokerId, int port, String clusterId, boolean initialize) throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "--broker-id", Integer.toString(brokerId),
                "--host", "127.0.0.1",
                "--port", Integer.toString(port),
                "--data-dir", BrokerTestHarness.newDataDir("runtime-" + brokerId).toString(),
                "--cluster-id", clusterId,
                "--etcd-endpoints", ETCD.endpoint(),
                "--controller-session-ttl-seconds", "3",
                "--controller-failover-grace-ms", "2000",
                "--controller-broker-lease-timeout-ms", "1000",
                "--broker-self-fence-timeout-ms", "600",
                "--broker-heartbeat-interval-ms", "200",
                "--leader-activation-timeout-ms", "2000",
                "--admin-token", BrokerTestHarness.ADMIN_TOKEN));
        if (initialize) {
            args.add("--initialize-empty-cluster");
        }
        return BrokerConfig.parse(args.toArray(String[]::new));
    }

    private static DiscoverControllerResponse discover(TestNetworkClient client)
            throws InterruptedException {
        var response = client.send(ApiKeys.DISCOVER_CONTROLLER, new DiscoverControllerRequest());
        assertEquals(com.minimq.common.error.ErrorCode.NONE, response.header().errorCode());
        return (DiscoverControllerResponse) response.body();
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int completedIndex(List<CompletableFuture<Long>> futures) {
        return completedIndexExcluding(futures, -1);
    }

    private static int completedIndexExcluding(List<CompletableFuture<Long>> futures, int excluded) {
        for (int i = 0; i < futures.size(); i++) {
            if (i != excluded && futures.get(i).isDone()) {
                return i;
            }
        }
        throw new AssertionError("no Controller campaign completed");
    }
}

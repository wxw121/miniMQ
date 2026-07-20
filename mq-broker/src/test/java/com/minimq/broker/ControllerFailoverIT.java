package com.minimq.broker;

import com.minimq.broker.coordination.ControllerElectionService;
import com.minimq.broker.coordination.ControllerState;
import com.minimq.broker.coordination.EtcdControllerCoordinationStore;
import com.minimq.broker.support.EtcdTestContainer;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

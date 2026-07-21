package com.minimq.broker;

import org.junit.jupiter.api.Test;

import com.minimq.storage.FlushPolicy;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerConfigTest {

    @Test
    void parsesAllSupportedArguments() {
        BrokerConfig config = BrokerConfig.parse(new String[] {
                "--broker-id", "2",
                "--host", "0.0.0.0",
                "--port", "9094",
                "--data-dir", "./data/broker-2",
                "--cluster-id", "orders-prod",
                "--etcd-endpoints", "http://etcd-1:2379,http://etcd-2:2379",
                "--controller-session-ttl-seconds", "30",
                "--controller-failover-grace-ms", "12000",
                "--leader-activation-timeout-ms", "7000",
                "--initialize-empty-cluster",
                "--recover-corrupt-metadata",
                "--flush-policy", "every-n-appends",
                "--flush-every-n-appends", "5",
                "--checkpoint-every-n-appends", "10",
                "--segment-max-bytes", "4096",
                "--sparse-index-interval-records", "8",
                "--retention-ms", "60000",
                "--retention-bytes", "1048576",
                "--compaction-enabled",
                "--compaction-max-segments", "4",
                "--compaction-max-bytes", "8192",
                "--maintenance-window-start-hour", "9",
                "--maintenance-window-end-hour", "17",
                "--maintenance-every-n-appends", "12",
                "--fetch-transfer-mode", "heap",
                "--fetch-transfer-timeout-ms", "2500",
                "--network-transport", "auto",
                "--max-in-flight-requests", "128",
                "--broker-heartbeat-interval-ms", "100",
                "--broker-self-fence-timeout-ms", "300",
                "--controller-broker-lease-timeout-ms", "500",
                "--admin-token", "ops-secret"
        });

        assertEquals(2, config.brokerId());
        assertEquals("0.0.0.0", config.host());
        assertEquals(9094, config.port());
        assertEquals(Path.of("./data/broker-2"), config.dataDir());
        assertEquals("orders-prod", config.clusterId());
        assertEquals(List.of("http://etcd-1:2379", "http://etcd-2:2379"), config.etcdEndpoints());
        assertEquals(30, config.controllerSessionTtlSeconds());
        assertEquals(12_000, config.controllerFailoverGraceMs());
        assertEquals(7_000, config.leaderActivationTimeoutMs());
        assertTrue(config.initializeEmptyCluster());
        assertEquals(false, config.initializeClusterFromLocal());
        assertTrue(config.recoverCorruptMetadata());
        assertEquals(FlushPolicy.everyNAppends(5), config.flushPolicy());
        assertEquals(10, config.checkpointEveryNAppends());
        assertEquals(4096, config.segmentMaxBytes());
        assertEquals(8, config.sparseIndexIntervalRecords());
        assertEquals(60_000, config.retentionMs());
        assertEquals(1_048_576, config.retentionBytes());
        assertTrue(config.compactionEnabled());
        assertEquals(4, config.compactionMaxSegments());
        assertEquals(8_192, config.compactionMaxBytes());
        assertEquals(9, config.maintenanceWindowStartHour());
        assertEquals(17, config.maintenanceWindowEndHour());
        assertEquals(12, config.maintenanceEveryNAppends());
        assertEquals(FetchTransferMode.HEAP, config.fetchTransferMode());
        assertEquals(2_500, config.fetchTransferTimeoutMs());
        assertEquals(com.minimq.broker.netty.NetworkTransportMode.AUTO, config.networkTransportMode());
        assertEquals(128, config.maxInFlightRequests());
        assertEquals(100, config.brokerHeartbeatIntervalMs());
        assertEquals(300, config.brokerSelfFenceTimeoutMs());
        assertEquals(500, config.controllerBrokerLeaseTimeoutMs());
        assertEquals("ops-secret", config.adminToken());
    }

    @Test
    void defaultsFetchTransferToAutoWithThirtySecondTimeout() {
        BrokerConfig config = new BrokerConfig(
                0, "localhost", 9092, Path.of("./data"), true, "localhost", 9092, false, "ops-secret");
        assertEquals(FetchTransferMode.AUTO, config.fetchTransferMode());
        assertEquals(30_000, config.fetchTransferTimeoutMs());
        assertEquals(com.minimq.broker.netty.NetworkTransportMode.NIO, config.networkTransportMode());
        assertEquals(0, config.maxInFlightRequests());
        assertEquals(1_000, config.brokerHeartbeatIntervalMs());
        assertEquals(3_000, config.brokerSelfFenceTimeoutMs());
        assertEquals(5_000, config.controllerBrokerLeaseTimeoutMs());
    }

    @Test
    void adminTokenIsRequiredAndMustNotBeBlank() {
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(new String[0]));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--admin-token", "   "}));
        assertEquals("ops-secret", BrokerConfig.parse(
                new String[] {"--admin-token", "ops-secret"}).adminToken());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(new String[] {"--unknown"}));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(new String[] {"--port"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--controller-addr", "localhost"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--flush-policy", "sometimes"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--flush-every-n-appends", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--checkpoint-every-n-appends", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--segment-max-bytes", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--sparse-index-interval-records", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--retention-ms", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--retention-bytes", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--compaction-max-segments", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--compaction-max-bytes", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--maintenance-window-start-hour", "24"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {
                        "--maintenance-window-start-hour", "9",
                        "--maintenance-window-end-hour", "-1"
                }));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--maintenance-window-start-hour", "9"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--maintenance-window-end-hour", "17"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--maintenance-every-n-appends", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--fetch-transfer-mode", "mmap"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--fetch-transfer-timeout-ms", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--fetch-transfer-timeout-ms", "-1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--network-transport", "kqueue"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--max-in-flight-requests", "-1"}));
    }

    @Test
    void rejectsInvalidBrokerLeaseTiming() {
        assertInvalidLeaseArgs("--broker-heartbeat-interval-ms", "0");
        assertInvalidLeaseArgs("--broker-self-fence-timeout-ms", "-1");
        assertInvalidLeaseArgs(
                "--broker-heartbeat-interval-ms", "300",
                "--broker-self-fence-timeout-ms", "300");
        assertInvalidLeaseArgs(
                "--broker-self-fence-timeout-ms", "500",
                "--controller-broker-lease-timeout-ms", "500");
    }

    @Test
    void rejectsInvalidControlPlaneConfiguration() {
        assertInvalidLeaseArgs("--cluster-id", "bad/id");
        assertInvalidLeaseArgs("--etcd-endpoints", "   ");
        assertInvalidLeaseArgs("--controller-session-ttl-seconds", "0");
        assertInvalidLeaseArgs(
                "--controller-failover-grace-ms", "5000",
                "--controller-session-ttl-seconds", "5");
        assertInvalidLeaseArgs(
                "--controller-broker-lease-timeout-ms", "5000",
                "--controller-failover-grace-ms", "4999");
        assertInvalidLeaseArgs("--leader-activation-timeout-ms", "0");
        assertInvalidLeaseArgs("--initialize-empty-cluster", "--initialize-cluster-from-local");
        assertInvalidLeaseArgs("--controller");
        assertInvalidLeaseArgs("--controller-addr", "localhost:9092");
    }

    private static void assertInvalidLeaseArgs(String... leaseArgs) {
        String[] args = new String[leaseArgs.length + 2];
        System.arraycopy(leaseArgs, 0, args, 0, leaseArgs.length);
        args[args.length - 2] = "--admin-token";
        args[args.length - 1] = "ops-secret";
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(args));
    }
}

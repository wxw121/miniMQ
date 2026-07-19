package com.minimq.broker;

import org.junit.jupiter.api.Test;

import com.minimq.storage.FlushPolicy;

import java.nio.file.Path;

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
                "--controller",
                "--recover-corrupt-metadata",
                "--controller-addr", "localhost:9092",
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
                "--admin-token", "ops-secret"
        });

        assertEquals(2, config.brokerId());
        assertEquals("0.0.0.0", config.host());
        assertEquals(9094, config.port());
        assertEquals(Path.of("./data/broker-2"), config.dataDir());
        assertTrue(config.controller());
        assertTrue(config.recoverCorruptMetadata());
        assertEquals("localhost", config.controllerHost());
        assertEquals(9092, config.controllerPort());
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
        assertEquals("ops-secret", config.adminToken());
    }

    @Test
    void defaultsFetchTransferToAutoWithThirtySecondTimeout() {
        BrokerConfig config = new BrokerConfig(
                0, "localhost", 9092, Path.of("./data"), true, "localhost", 9092, false, "ops-secret");
        assertEquals(FetchTransferMode.AUTO, config.fetchTransferMode());
        assertEquals(30_000, config.fetchTransferTimeoutMs());
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
    }
}

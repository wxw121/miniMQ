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
                "--sparse-index-interval-records", "8"
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
    }
}

package com.minimq.broker;

import org.junit.jupiter.api.Test;

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
                "--controller-addr", "localhost:9092"
        });

        assertEquals(2, config.brokerId());
        assertEquals("0.0.0.0", config.host());
        assertEquals(9094, config.port());
        assertEquals(Path.of("./data/broker-2"), config.dataDir());
        assertTrue(config.controller());
        assertTrue(config.recoverCorruptMetadata());
        assertEquals("localhost", config.controllerHost());
        assertEquals(9092, config.controllerPort());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(new String[] {"--unknown"}));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse(new String[] {"--port"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerConfig.parse(new String[] {"--controller-addr", "localhost"}));
    }
}

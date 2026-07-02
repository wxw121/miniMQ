package com.minimq.common.model;

import com.minimq.common.error.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CommonModelTest {

    @Test
    void recordsExposeConstructorValues() {
        BrokerInfo broker = new BrokerInfo(1, "localhost", 9092);
        TopicPartition topicPartition = new TopicPartition("orders", 2);
        byte[] key = new byte[] {1, 2};
        byte[] value = new byte[] {3, 4};
        Record record = new Record(7L, key, value);

        assertEquals(1, broker.brokerId());
        assertEquals("localhost", broker.host());
        assertEquals(9092, broker.port());
        assertEquals("orders", topicPartition.topic());
        assertEquals(2, topicPartition.partition());
        assertEquals(7L, record.offset());
        assertArrayEquals(key, record.key());
        assertArrayEquals(value, record.value());
    }

    @Test
    void errorCodeRoundTripsKnownAndUnknownCodes() {
        for (ErrorCode code : ErrorCode.values()) {
            assertEquals(code, ErrorCode.fromCode(code.code));
        }
        assertEquals(ErrorCode.INVALID_REQUEST, ErrorCode.fromCode((short) 999));
    }
}

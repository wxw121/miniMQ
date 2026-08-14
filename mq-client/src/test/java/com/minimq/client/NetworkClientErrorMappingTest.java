package com.minimq.client;

import com.minimq.client.exception.MiniMqServerException;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetworkClientErrorMappingTest {
    @Test
    void preservesEveryV4ErrorCode() throws Exception {
        try (NetworkClient client = new NetworkClient("v4-error-mapping")) {
            for (ErrorCode errorCode : List.of(
                    ErrorCode.FORBIDDEN,
                    ErrorCode.UNKNOWN_BROKER,
                    ErrorCode.STALE_TOMBSTONE,
                    ErrorCode.NOT_ENOUGH_REPLICAS,
                    ErrorCode.REQUEST_TIMED_OUT)) {
                MiniMqServerException exception = assertThrows(
                        MiniMqServerException.class,
                        () -> client.ensureSuccess(
                                new MqDecoder.DecodedResponse(
                                        new MqResponseHeader(41, errorCode),
                                        null),
                                "V4 request"));

                assertEquals(errorCode, exception.errorCode());
                assertEquals("V4 request failed: " + errorCode, exception.getMessage());
            }
        }
    }
}

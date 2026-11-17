package com.minimq.client;

import com.minimq.client.exception.MiniMqServerException;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.MqResponseHeader;
import com.minimq.protocol.message.ProtocolMessages.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class ProducerBatchLifecycleTest {
    @Test
    void flushWaitsForBatchAlreadyDispatchedByLinger() throws Exception {
        assertWaitsForLinger(false, ErrorCode.NONE);
    }

    @Test
    void closeWaitsForBatchAlreadyDispatchedByLinger() throws Exception {
        assertWaitsForLinger(true, ErrorCode.NONE);
    }

    @Test
    void flushPropagatesFailureOfBatchAlreadyDispatchedByLinger() throws Exception {
        assertWaitsForLinger(false, ErrorCode.INVALID_REQUEST);
    }

    @Test
    void closePropagatesFailureOfBatchAlreadyDispatchedByLinger() throws Exception {
        assertWaitsForLinger(true, ErrorCode.INVALID_REQUEST);
    }

    private void assertWaitsForLinger(boolean closing, ErrorCode error) throws Exception {
        BlockingProducer producer = new BlockingProducer(error);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var result = producer.send(new ProducerRecord("orders", 0, null, new byte[]{1}));
            assertTrue(producer.entered.await(5, TimeUnit.SECONDS));
            CountDownLatch started = new CountDownLatch(1);
            var completion = executor.submit(() -> {
                started.countDown();
                if (closing) {
                    producer.close();
                } else {
                    producer.flush();
                }
                return null;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> completion.get(100, TimeUnit.MILLISECONDS),
                    "flush/close must wait for the blocked batch");
            producer.release.countDown();
            if (error == ErrorCode.NONE) {
                completion.get(5, TimeUnit.SECONDS);
                assertEquals(7L, result.offset());
            } else {
                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> completion.get(5, TimeUnit.SECONDS));
                assertEquals(error, assertInstanceOf(MiniMqServerException.class, failure.getCause()).errorCode());
                assertEquals(error, assertThrows(MiniMqServerException.class, result::offset).errorCode());
            }
        } finally {
            producer.release.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            producer.close();
        }
    }

    private static final class BlockingProducer extends Producer {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final ErrorCode error;

        private BlockingProducer(ErrorCode error) {
            super("127.0.0.1:19092", "batch-lifecycle", new ProducerConfig(50, 5));
            this.error = error;
        }

        @Override
        protected MetadataResponse fetchMetadata(String topicName) {
            return new MetadataResponse(List.of(new BrokerMetadata(0, "127.0.0.1", 19092)),
                    List.of(new TopicMetadata("orders", List.of(new PartitionMetadata(0, 0)))));
        }

        @Override
        protected MqDecoder.DecodedResponse sendProduce(BrokerMetadata leader, ProduceRequest request)
                throws InterruptedException {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for test to release produce");
            }
            return new MqDecoder.DecodedResponse(new MqResponseHeader(1, error),
                    error == ErrorCode.NONE ? new ProduceResponse("orders", 0, 7L) : null);
        }
    }
}

package com.minimq.client;

import com.minimq.broker.support.BrokerTestHarness;
import com.minimq.protocol.message.ProtocolMessages.BrokerMetadata;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerClientCacheTest {

    @Test
    void producerClosesTheLosingClientWhenAnotherThreadWinsTheCache() throws Exception {
        CountDownLatch enteredConnect = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean discarded = new AtomicBoolean();
        try (Producer producer = new Producer("127.0.0.1:1", "race-producer", id -> new NetworkClient(id) {
            @Override
            public void connect(String host, int port) throws InterruptedException {
                enteredConnect.countDown();
                await(proceed);
            }

            @Override
            public void close() {
                discarded.set(true);
            }
        })) {
            NetworkClient winner = invokeProducerBrokerClientRace(producer, enteredConnect, proceed);
            assertTrue(discarded.get());
            assertSame(winner, cachedClient(producer));
        }
    }

    @Test
    void producerIgnoresCloseFailureOnTheLosingClient() throws Exception {
        CountDownLatch enteredConnect = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        try (Producer producer = new Producer("127.0.0.1:1", "race-producer-close", id -> new NetworkClient(id) {
            @Override
            public void connect(String host, int port) throws InterruptedException {
                enteredConnect.countDown();
                await(proceed);
            }

            @Override
            public void close() {
                throw new IllegalStateException("losing close failed");
            }
        })) {
            NetworkClient winner = invokeProducerBrokerClientRace(producer, enteredConnect, proceed);
            assertSame(winner, cachedClient(producer));
        }
    }

    @Test
    void consumerClosesTheLosingClientWhenAnotherThreadWinsTheCache() throws Exception {
        CountDownLatch enteredConnect = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean discarded = new AtomicBoolean();
        try (BrokerTestHarness controller = BrokerTestHarness.startController(
                     BrokerTestHarness.newDataDir("consumer-client-cache"));
             Consumer consumer = new Consumer(
                     controller.bootstrapServers(),
                     "cache-group",
                     "cache-consumer",
                     id -> new NetworkClient(id) {
                         @Override
                         public void connect(String host, int port) throws InterruptedException {
                             enteredConnect.countDown();
                             await(proceed);
                         }

                         @Override
                         public void connect(String host, int port, Duration timeout)
                                 throws InterruptedException {
                             connect(host, port);
                         }

                         @Override
                         public void close() {
                             discarded.set(true);
                         }
                     })) {
            NetworkClient winner = invokeConsumerBrokerClientRace(consumer, enteredConnect, proceed, null);
            assertTrue(discarded.get());
            assertSame(winner, cachedClient(consumer));
        }
    }

    @Test
    void consumerAbortsTheLosingClientWhenThePollDeadlineIsSet() throws Exception {
        CountDownLatch enteredConnect = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean aborted = new AtomicBoolean();
        try (BrokerTestHarness controller = BrokerTestHarness.startController(
                     BrokerTestHarness.newDataDir("consumer-client-cache-deadline"));
             Consumer consumer = new Consumer(
                     controller.bootstrapServers(),
                     "cache-deadline-group",
                     "cache-deadline-consumer",
                     id -> new NetworkClient(id) {
                         @Override
                         public void connect(String host, int port) throws InterruptedException {
                             enteredConnect.countDown();
                             await(proceed);
                         }

                         @Override
                         public void connect(String host, int port, Duration timeout)
                                 throws InterruptedException {
                             connect(host, port);
                         }

                         @Override
                         void abortCandidate(Duration timeout) {
                             aborted.set(true);
                         }
                     })) {
            NetworkClient winner = invokeConsumerBrokerClientRace(
                    consumer,
                    enteredConnect,
                    proceed,
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
            assertTrue(aborted.get());
            assertSame(winner, cachedClient(consumer));
        }
    }

    private static NetworkClient invokeProducerBrokerClientRace(
            Producer producer,
            CountDownLatch enteredConnect,
            CountDownLatch proceed) throws Exception {
        Method method = Producer.class.getDeclaredMethod(
                "brokerClient", BrokerMetadata.class);
        method.setAccessible(true);
        return invokeBrokerClientRace(producer, method, enteredConnect, proceed, null);
    }

    private static NetworkClient invokeConsumerBrokerClientRace(
            Consumer consumer,
            CountDownLatch enteredConnect,
            CountDownLatch proceed,
            Long deadlineNanos) throws Exception {
        Method method = Consumer.class.getDeclaredMethod(
                "brokerClient", BrokerMetadata.class, Long.class);
        method.setAccessible(true);
        return invokeBrokerClientRace(consumer, method, enteredConnect, proceed, deadlineNanos);
    }

    private static NetworkClient invokeBrokerClientRace(
            Object client,
            Method method,
            CountDownLatch enteredConnect,
            CountDownLatch proceed,
            Long deadlineNanos) throws Exception {
        BrokerMetadata leader = new BrokerMetadata(0, "127.0.0.1", 9092);
        NetworkClient winner = new NetworkClient("winner");
        Map<Integer, NetworkClient> clients = clients(client);
        AtomicReference<NetworkClient> returned = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread racing = new Thread(() -> {
            try {
                Object result = deadlineNanos == null && method.getParameterCount() == 1
                        ? method.invoke(client, leader)
                        : method.invoke(client, leader, deadlineNanos);
                returned.set((NetworkClient) result);
            } catch (InvocationTargetException ex) {
                failure.set(ex.getCause());
            } catch (Throwable ex) {
                failure.set(ex);
            }
        }, "broker-client-race");
        racing.start();
        assertTrue(enteredConnect.await(5, TimeUnit.SECONDS));
        clients.put(0, winner);
        proceed.countDown();
        racing.join(TimeUnit.SECONDS.toMillis(5));
        if (failure.get() != null) {
            throw new AssertionError("broker client race failed", failure.get());
        }
        assertSame(winner, returned.get());
        return winner;
    }

    private static NetworkClient cachedClient(Object client) throws Exception {
        return clients(client).get(0);
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, NetworkClient> clients(Object client) throws Exception {
        Field field = client.getClass().getDeclaredField("brokerClients");
        field.setAccessible(true);
        return (Map<Integer, NetworkClient>) field.get(client);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("timed out waiting to continue connect");
        }
    }
}

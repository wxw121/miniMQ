package com.minimq.broker.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class ControllerConnectionProxy implements AutoCloseable {
    private final String targetHost;
    private final int targetPort;
    private final ServerSocket listener;
    private final ExecutorService relays;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean disconnected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    private ControllerConnectionProxy(String targetHost, int targetPort, ServerSocket listener) {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.listener = listener;
        AtomicInteger relayIds = new AtomicInteger();
        this.relays = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "controller-proxy-relay-" + relayIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        this.acceptThread = new Thread(this::acceptLoop, "controller-proxy-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public static ControllerConnectionProxy start(String targetHost, int targetPort) throws IOException {
        ServerSocket listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        return new ControllerConnectionProxy(targetHost, targetPort, listener);
    }

    public int port() {
        return listener.getLocalPort();
    }

    public void disconnect() {
        disconnected.set(true);
        closeOpenSockets();
    }

    public void reconnect() {
        if (closed.get()) {
            throw new IllegalStateException("Controller proxy is closed");
        }
        disconnected.set(false);
    }

    public boolean disconnected() {
        return disconnected.get();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket brokerSocket = listener.accept();
                if (disconnected.get()) {
                    closeSocket(brokerSocket);
                    continue;
                }
                openRelay(brokerSocket);
            } catch (SocketException failure) {
                if (!closed.get()) {
                    throw new IllegalStateException("Controller proxy listener failed", failure);
                }
                return;
            } catch (IOException failure) {
                if (!closed.get()) {
                    throw new IllegalStateException("Controller proxy relay failed", failure);
                }
                return;
            }
        }
    }

    private void openRelay(Socket brokerSocket) throws IOException {
        Socket controllerSocket = new Socket();
        boolean connected = false;
        try {
            controllerSocket.connect(new InetSocketAddress(targetHost, targetPort));
            if (disconnected.get() || closed.get()) {
                return;
            }
            openSockets.add(brokerSocket);
            openSockets.add(controllerSocket);
            connected = true;
            relays.execute(() -> copy(brokerSocket, controllerSocket));
            relays.execute(() -> copy(controllerSocket, brokerSocket));
        } finally {
            if (!connected) {
                closeSocket(brokerSocket);
                closeSocket(controllerSocket);
            }
        }
    }

    private void copy(Socket source, Socket target) {
        try {
            source.getInputStream().transferTo(target.getOutputStream());
        } catch (IOException ignored) {
            // Socket closure is the expected signal for disconnect and shutdown.
        } finally {
            closeSocket(source);
            closeSocket(target);
        }
    }

    private void closeOpenSockets() {
        for (Socket socket : openSockets.toArray(Socket[]::new)) {
            closeSocket(socket);
        }
    }

    private void closeSocket(Socket socket) {
        openSockets.remove(socket);
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort close; the peer direction also closes the relay pair.
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        disconnected.set(true);
        listener.close();
        closeOpenSockets();
        relays.shutdownNow();
        acceptThread.join(TimeUnit.SECONDS.toMillis(5));
        if (!relays.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Controller proxy relay threads did not terminate");
        }
    }
}

package com.minimq.broker.coordination;

public interface ControllerCoordinationStore extends AutoCloseable {
    Leadership campaign(int brokerId, String host, int port, int ttlSeconds);

    ControllerIdentity currentLeader();

    @Override
    void close();

    interface Leadership extends AutoCloseable {
        ControllerIdentity identity();

        /** Exact bytes installed in the leader key and required by metadata CAS. */
        byte[] identityBytes();

        boolean valid();

        @Override
        void close();
    }
}

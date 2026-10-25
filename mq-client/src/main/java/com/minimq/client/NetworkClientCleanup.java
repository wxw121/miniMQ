package com.minimq.client;

import java.time.Duration;

final class NetworkClientCleanup {
    private NetworkClientCleanup() {
    }

    static boolean suppressCloseFailure(NetworkClient client, Throwable primaryFailure) {
        try {
            client.abortCandidate();
            return false;
        } catch (InterruptedException closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
            return true;
        } catch (RuntimeException | Error closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
            return false;
        }
    }

    static boolean suppressCloseFailure(
            NetworkClient client, Throwable primaryFailure, Duration timeout) {
        try {
            client.abortCandidate(timeout);
            return false;
        } catch (InterruptedException closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
            return true;
        } catch (RuntimeException | Error closeFailure) {
            primaryFailure.addSuppressed(closeFailure);
            return false;
        }
    }

    static boolean hasSuppressedInterruption(Throwable failure) {
        for (Throwable suppressed : failure.getSuppressed()) {
            if (suppressed instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}

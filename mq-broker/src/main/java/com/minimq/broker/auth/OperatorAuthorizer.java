package com.minimq.broker.auth;

import com.minimq.broker.BrokerConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public final class OperatorAuthorizer {
    private final List<Entry> entries;

    private OperatorAuthorizer(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    public static OperatorAuthorizer fromConfig(BrokerConfig config) {
        List<Entry> entries = new ArrayList<>();
        for (BrokerConfig.OperatorCredential credential : config.operatorCredentials()) {
            entries.add(new Entry(
                    credential.token().getBytes(StandardCharsets.UTF_8),
                    EnumSet.copyOf(credential.scopes())));
        }
        entries.add(new Entry(
                config.adminToken().getBytes(StandardCharsets.UTF_8),
                EnumSet.allOf(OperatorScope.class)));
        return new OperatorAuthorizer(entries);
    }

    public static OperatorAuthorizer forAdminToken(String adminToken) {
        return new OperatorAuthorizer(List.of(new Entry(
                adminToken.getBytes(StandardCharsets.UTF_8),
                EnumSet.allOf(OperatorScope.class))));
    }

    public Decision authorize(String suppliedCredential, OperatorScope requiredScope) {
        Decision authenticated = authenticate(suppliedCredential);
        if (!authenticated.authenticated()) {
            return authenticated;
        }
        return new Decision(
                true,
                authenticated.scopes().contains(requiredScope),
                authenticated.scopes());
    }

    public Decision authenticateForAuthStatus(String suppliedCredential) {
        Decision authenticated = authenticate(suppliedCredential);
        if (!authenticated.authenticated()) {
            return authenticated;
        }
        return new Decision(true, true, authenticated.scopes());
    }

    private Decision authenticate(String suppliedCredential) {
        if (suppliedCredential == null) {
            return Decision.unauthenticated();
        }
        byte[] supplied = suppliedCredential.getBytes(StandardCharsets.UTF_8);
        Set<OperatorScope> matchedScopes = null;
        for (Entry entry : entries) {
            if (constantTimeEquals(entry.tokenBytes(), supplied)) {
                matchedScopes = entry.scopes();
            }
        }
        return matchedScopes == null
                ? Decision.unauthenticated()
                : new Decision(true, false, matchedScopes);
    }

    public static boolean constantTimeEqualsUtf8(String expected, String supplied) {
        if (expected == null || supplied == null) {
            return false;
        }
        return constantTimeEquals(
                expected.getBytes(StandardCharsets.UTF_8),
                supplied.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean constantTimeEquals(byte[] expected, byte[] supplied) {
        int diff = expected.length ^ supplied.length;
        if (expected.length > BrokerConfig.MAX_OPERATOR_CREDENTIAL_BYTES
                || supplied.length > BrokerConfig.MAX_OPERATOR_CREDENTIAL_BYTES) {
            diff |= 1;
        }
        for (int i = 0; i < BrokerConfig.MAX_OPERATOR_CREDENTIAL_BYTES; i++) {
            int expectedByte = i < expected.length ? expected[i] & 0xff : 0;
            int suppliedByte = i < supplied.length ? supplied[i] & 0xff : 0;
            diff |= expectedByte ^ suppliedByte;
        }
        return diff == 0;
    }

    private record Entry(byte[] tokenBytes, Set<OperatorScope> scopes) {
        private Entry {
            tokenBytes = tokenBytes.clone();
            scopes = Set.copyOf(scopes);
        }
    }

    public record Decision(boolean authenticated, boolean authorized, Set<OperatorScope> scopes) {
        public Decision {
            scopes = scopes == null ? Set.of() : Set.copyOf(scopes);
        }

        private static Decision unauthenticated() {
            return new Decision(false, false, Set.of());
        }
    }
}

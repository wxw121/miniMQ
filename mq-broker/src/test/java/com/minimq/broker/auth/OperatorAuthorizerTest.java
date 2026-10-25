package com.minimq.broker.auth;

import com.minimq.broker.BrokerConfig;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorAuthorizerTest {

    @Test
    void distinguishesMissingUnknownWrongScopeAndRightScopeCredentials() {
        OperatorAuthorizer authorizer = OperatorAuthorizer.fromConfig(BrokerConfig.parse(new String[] {
                "--operator-credential", "topic-token=TOPIC_ADMIN",
                "--admin-token", "admin-token"
        }));

        OperatorAuthorizer.Decision missing = authorizer.authorize(null, OperatorScope.TOPIC_ADMIN);
        assertFalse(missing.authenticated());
        assertFalse(missing.authorized());
        assertEquals(Set.of(), missing.scopes());

        OperatorAuthorizer.Decision unknown = authorizer.authorize("wrong-token", OperatorScope.TOPIC_ADMIN);
        assertFalse(unknown.authenticated());
        assertFalse(unknown.authorized());
        assertEquals(Set.of(), unknown.scopes());

        OperatorAuthorizer.Decision wrongScope = authorizer.authorize("topic-token", OperatorScope.MAINTENANCE);
        assertTrue(wrongScope.authenticated());
        assertFalse(wrongScope.authorized());
        assertEquals(Set.of(OperatorScope.TOPIC_ADMIN), wrongScope.scopes());

        OperatorAuthorizer.Decision rightScope = authorizer.authorize("topic-token", OperatorScope.TOPIC_ADMIN);
        assertTrue(rightScope.authenticated());
        assertTrue(rightScope.authorized());
        assertEquals(Set.of(OperatorScope.TOPIC_ADMIN), rightScope.scopes());
    }

    @Test
    void authenticatesAnyConfiguredCredentialForAuthStatus() {
        OperatorAuthorizer authorizer = OperatorAuthorizer.fromConfig(BrokerConfig.parse(new String[] {
                "--operator-credential", "topic-token=TOPIC_ADMIN",
                "--admin-token", "admin-token"
        }));

        OperatorAuthorizer.Decision unknown = authorizer.authenticateForAuthStatus("wrong-token");
        assertFalse(unknown.authenticated());
        assertFalse(unknown.authorized());
        assertEquals(Set.of(), unknown.scopes());

        OperatorAuthorizer.Decision scoped = authorizer.authenticateForAuthStatus("topic-token");
        assertTrue(scoped.authenticated());
        assertTrue(scoped.authorized());
        assertEquals(Set.of(OperatorScope.TOPIC_ADMIN), scoped.scopes());
    }

    @Test
    void legacyAdminTokenAuthorizesAllOperatorScopes() {
        OperatorAuthorizer authorizer = OperatorAuthorizer.fromConfig(BrokerConfig.parse(new String[] {
                "--operator-credential", "topic-token=TOPIC_ADMIN",
                "--admin-token", "admin-token"
        }));

        for (OperatorScope scope : OperatorScope.values()) {
            OperatorAuthorizer.Decision decision = authorizer.authorize("admin-token", scope);
            assertTrue(decision.authenticated(), scope.name());
            assertTrue(decision.authorized(), scope.name());
            assertEquals(Set.of(OperatorScope.STATUS, OperatorScope.TOPIC_ADMIN,
                    OperatorScope.MAINTENANCE, OperatorScope.BROKER_OFFLINE), decision.scopes());
        }
    }

    @Test
    void authorizationDecisionsDoNotExposeSuppliedCredentialText() {
        OperatorAuthorizer authorizer = OperatorAuthorizer.fromConfig(BrokerConfig.parse(new String[] {
                "--operator-credential", "topic-token=TOPIC_ADMIN",
                "--admin-token", "admin-token"
        }));

        OperatorAuthorizer.Decision decision = authorizer.authenticateForAuthStatus("topic-token");

        assertFalse(decision.toString().contains("topic-token"));
    }

    @Test
    void fixedWidthUtf8ComparisonRequiresMatchingBytesAndLength() {
        assertTrue(OperatorAuthorizer.constantTimeEqualsUtf8("admin-token", "admin-token"));
        assertFalse(OperatorAuthorizer.constantTimeEqualsUtf8("admin-token", "admin-token-suffix"));
        assertFalse(OperatorAuthorizer.constantTimeEqualsUtf8("admin-token-suffix", "admin-token"));
        assertFalse(OperatorAuthorizer.constantTimeEqualsUtf8("admin-token", null));
    }
}

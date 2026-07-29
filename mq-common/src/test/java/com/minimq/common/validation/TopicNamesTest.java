package com.minimq.common.validation;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicNamesTest {

    @Test
    void acceptsPortableTopicNames() {
        assertDoesNotThrow(() -> TopicNames.requireValid("orders.retry-v2_1"));
    }

    @Test
    void rejectsNamesThatCanEscapeStoragePaths() {
        for (String invalid : new String[] {
                "", " ", ".", "..", "../orders", "..\\orders", "a/b", "a\\b",
                "orders\nnext", "a".repeat(250),
                "CON", "con.events", "orders."
        }) {
            assertThrows(IllegalArgumentException.class, () -> TopicNames.requireValid(invalid));
        }
        assertFalse(TopicNames.isValid(null));
        assertTrue(TopicNames.isValid("AZaz09._-"));
    }

    @Test
    void acceptsPortableLegacyReferencesWithoutRelaxingNewTopicCreation() {
        String legacyName = "订单.É";

        assertEquals(legacyName, TopicNames.requireValidReference(legacyName));
        assertTrue(TopicNames.isValidReference(legacyName));
        assertFalse(TopicNames.isValid(legacyName));
    }

    @Test
    void rejectsNonPortableLegacyReferences() {
        String utf8TooLong = "界".repeat(84);
        for (String invalid : new String[] {
                "", " ", ".", "..", "orders.", "orders ",
                "a".repeat(250), utf8TooLong,
                "CON", "con.events", "Lpt9.logs",
                "orders/next", "orders\\next", "C:orders",
                "orders*", "orders?", "orders\"", "orders<",
                "orders>", "orders|", "orders\u0000next", "orders\u007fnext"
        }) {
            assertFalse(TopicNames.isValidReference(invalid), invalid);
            assertThrows(IllegalArgumentException.class,
                    () -> TopicNames.requireValidReference(invalid), invalid);
        }
        assertFalse(TopicNames.isValidReference(null));
    }

    @Test
    void portableStorageIdentityUsesNfcAndLocaleIndependentCaseFolding() {
        String decomposed = "CAF" + Normalizer.normalize("É", Normalizer.Form.NFD);

        assertEquals("café", TopicNames.portableStorageIdentity(decomposed));
        assertEquals(
                TopicNames.portableStorageIdentity("Orders"),
                TopicNames.portableStorageIdentity("orders"));
        assertThrows(IllegalArgumentException.class,
                () -> TopicNames.portableStorageIdentity("NUL"));
    }
}

package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentCatalogStoreTest {

    @Test
    void savesAndLoadsCatalogAtomically(@TempDir Path dir) throws Exception {
        SegmentCatalogStore store = new SegmentCatalogStore(dir);
        SegmentCatalog catalog = new SegmentCatalog(
                3,
                128,
                0,
                List.of(new SegmentDescriptor(0, 1_000), new SegmentDescriptor(128, 2_000)));

        store.save(catalog);

        assertEquals(catalog, store.load().orElseThrow());
        assertFalse(Files.exists(dir.resolve("catalog.json.tmp")));
    }

    @Test
    void rejectsUnorderedOrIncorrectActiveCatalog() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(
                        1,
                        64,
                        0,
                        List.of(new SegmentDescriptor(64, 1), new SegmentDescriptor(0, 1))));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(
                        1,
                        0,
                        0,
                        List.of(new SegmentDescriptor(0, 1), new SegmentDescriptor(64, 2))));
    }

    @Test
    void rejectsCorruptCatalogInsteadOfTreatingItAsMissing(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("catalog.json"), "{not-json");

        IOException failure = assertThrows(IOException.class, () -> new SegmentCatalogStore(dir).load());

        assertTrue(failure.getMessage().contains("catalog.json"));
    }

    @Test
    void distinguishesMissingAndEmptyCatalogFiles(@TempDir Path dir) throws Exception {
        SegmentCatalogStore store = new SegmentCatalogStore(dir);

        assertTrue(store.load().isEmpty());

        Files.write(dir.resolve("catalog.json"), new byte[0]);

        assertThrows(IOException.class, store::load);
    }

    @Test
    void validatesDescriptorAndCatalogBounds() {
        assertThrows(IllegalArgumentException.class, () -> new SegmentDescriptor(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> new SegmentDescriptor(0, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(-1, 0, 0, List.of(new SegmentDescriptor(0, 0))));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCatalog(0, 0, 1, List.of(new SegmentDescriptor(0, 0))));
    }
}

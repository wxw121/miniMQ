package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetentionPlannerTest {

    @Test
    void selectsOnlyOldSealedSegmentsAndKeepsActive() {
        List<SegmentMetadata> segments = List.of(
                new SegmentMetadata(0, 0, 100, false),
                new SegmentMetadata(10, 500, 100, false),
                new SegmentMetadata(20, 2_000, 100, true));

        List<Long> deleted = RetentionPlanner.segmentsToDelete(
                segments,
                RetentionPolicy.of(1_000, 0),
                2_100);

        assertEquals(List.of(0L, 10L), deleted);
    }

    @Test
    void sizeRetentionStopsAfterDeletingOldestSealedSegments() {
        List<SegmentMetadata> segments = List.of(
                new SegmentMetadata(0, 0, 400, false),
                new SegmentMetadata(10, 0, 400, false),
                new SegmentMetadata(20, 0, 400, true));

        List<Long> deleted = RetentionPlanner.segmentsToDelete(
                segments,
                RetentionPolicy.of(0, 500),
                1_000);

        assertEquals(List.of(0L, 10L), deleted);
    }

    @Test
    void disabledPolicyDeletesNothing() {
        List<SegmentMetadata> segments = List.of(
                new SegmentMetadata(0, 0, 400, false),
                new SegmentMetadata(10, 0, 400, true));

        assertEquals(List.of(), RetentionPlanner.segmentsToDelete(
                segments,
                RetentionPolicy.disabled(),
                1_000));
    }
}

package com.minimq.common.assignor;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeAssignorTest {

    @Test
    void assignsTwoMembersThreePartitions() {
        Map<String, List<Integer>> assignment = RangeAssignor.assign(List.of("c0", "c1"), 3);
        assertEquals(List.of(0, 1), assignment.get("c0"));
        assertEquals(List.of(2), assignment.get("c1"));
    }

    @Test
    void fullAssignmentScalesBeyondFourKilobytes() {
        Map<String, List<Integer>> assignment = Map.of(
                "member-0", IntStream.range(0, 1_200).boxed().collect(Collectors.toList()));

        byte[] encoded = AssignmentSerde.encodeFullAssignment(assignment);

        assertEquals(assignment, AssignmentSerde.decodeFullAssignment(encoded));
    }

    @Test
    void partitionAssignmentRoundTrips() {
        List<Integer> partitions = List.of(0, 2, 4);

        byte[] encoded = AssignmentSerde.encodePartitions(partitions);

        assertEquals(partitions, AssignmentSerde.decodePartitions(encoded));
    }

    @Test
    void rejectsMalformedAssignmentBytes() {
        assertThrows(IllegalArgumentException.class, () -> AssignmentSerde.decodePartitions(new byte[] {0, 0, 0, 2}));
        assertThrows(IllegalArgumentException.class, () -> AssignmentSerde.decodeFullAssignment(new byte[] {0, 0, 0, 1}));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentSerde.decodeFullAssignment(new byte[] {0, 0, 0, 1, 0, 2, 'a'}));
    }

    @Test
    void rejectsAssignmentsBeyondProtocolLimits() {
        List<Integer> tooManyPartitions = Collections.nCopies(17_000, 1);

        assertThrows(IllegalArgumentException.class, () -> AssignmentSerde.encodePartitions(tooManyPartitions));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentSerde.encodeFullAssignment(Map.of("member-0", tooManyPartitions)));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentSerde.encodeFullAssignment(Map.of("x".repeat(70_000), List.of(1))));
    }

    @Test
    void decodesEmptyAssignments() {
        assertEquals(List.of(), AssignmentSerde.decodePartitions(null));
        assertEquals(List.of(), AssignmentSerde.decodePartitions(new byte[0]));
        assertEquals(Map.of(), AssignmentSerde.decodeFullAssignment(null));
        assertEquals(Map.of(), AssignmentSerde.decodeFullAssignment(new byte[0]));
    }
}

package com.minimq.common.assignor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Range 分区分配器，按成员顺序把连续分区范围平均分给消费组成员。
 */
public final class RangeAssignor {
    private RangeAssignor() {
    }

    // Range 策略按成员顺序切连续区间，成员数量变化时分配结果也会随之重算。
    public static Map<String, List<Integer>> assign(List<String> members, int partitionCount) {
        List<String> sortedMembers = new ArrayList<>(members);
        Collections.sort(sortedMembers);
        Map<String, List<Integer>> assignment = new HashMap<>();
        for (String member : sortedMembers) {
            assignment.put(member, new ArrayList<>());
        }
        if (sortedMembers.isEmpty() || partitionCount <= 0) {
            return assignment;
        }
        int perMember = (int) Math.ceil((double) partitionCount / sortedMembers.size());
        int partition = 0;
        for (String member : sortedMembers) {
            for (int i = 0; i < perMember && partition < partitionCount; i++) {
                assignment.get(member).add(partition++);
            }
        }
        return assignment;
    }
}

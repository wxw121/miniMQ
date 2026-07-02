package com.minimq.common.assignor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费组分配结果的二进制编解码工具，用于在 leader 和成员之间传递分区归属。
 */
public final class AssignmentSerde {
    private static final int MAX_ASSIGNMENT_BYTES = 0xFFFF;
    private static final int MAX_ASSIGNMENT_ITEMS = 10_000;

    private AssignmentSerde() {
    }

    public static byte[] encodePartitions(List<Integer> partitions) {
        if (partitions.size() > (MAX_ASSIGNMENT_BYTES - Integer.BYTES) / Integer.BYTES) {
            throw new IllegalArgumentException("Too many partitions in assignment: " + partitions.size());
        }
        ByteBuffer buffer = ByteBuffer.allocate(4 + partitions.size() * 4);
        buffer.putInt(partitions.size());
        for (int partition : partitions) {
            buffer.putInt(partition);
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static List<Integer> decodePartitions(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return List.of();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int count = readCount(buffer, "partition assignment");
        requireRemaining(buffer, count * Integer.BYTES, "partition assignment");
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(buffer.getInt());
        }
        return partitions;
    }

    // 完整分配按 memberId 写入，SyncGroup 时各成员再取出自己的分区列表。
    public static byte[] encodeFullAssignment(Map<String, List<Integer>> assignment) {
        var entries = assignment.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();
        int size = Integer.BYTES;
        for (Map.Entry<String, List<Integer>> entry : entries) {
            byte[] memberBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            if (memberBytes.length > 0xFFFF) {
                throw new IllegalArgumentException("Member id is too long: " + memberBytes.length);
            }
            List<Integer> partitions = entry.getValue() != null ? entry.getValue() : List.of();
            size += Short.BYTES + memberBytes.length + Integer.BYTES + partitions.size() * Integer.BYTES;
            if (size > MAX_ASSIGNMENT_BYTES) {
                throw new IllegalArgumentException("Assignment is too large: " + size);
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(entries.size());
        for (Map.Entry<String, List<Integer>> entry : entries) {
            byte[] memberBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            List<Integer> partitions = entry.getValue() != null ? entry.getValue() : List.of();
            buffer.putShort((short) memberBytes.length);
            buffer.put(memberBytes);
            buffer.putInt(partitions.size());
            for (int partition : partitions) {
                buffer.putInt(partition);
            }
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static Map<String, List<Integer>> decodeFullAssignment(byte[] bytes) {
        Map<String, List<Integer>> result = new HashMap<>();
        if (bytes == null || bytes.length == 0) {
            return result;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int memberCount = readCount(buffer, "full assignment members");
        for (int i = 0; i < memberCount; i++) {
            requireRemaining(buffer, Short.BYTES, "member id length");
            int memberLen = buffer.getShort() & 0xFFFF;
            requireRemaining(buffer, memberLen, "member id");
            byte[] memberBytes = new byte[memberLen];
            buffer.get(memberBytes);
            String memberId = new String(memberBytes, StandardCharsets.UTF_8);
            int partitionCount = readCount(buffer, "member partitions");
            requireRemaining(buffer, partitionCount * Integer.BYTES, "member partitions");
            List<Integer> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                partitions.add(buffer.getInt());
            }
            result.put(memberId, partitions);
        }
        return result;
    }

    private static int readCount(ByteBuffer buffer, String fieldName) {
        requireRemaining(buffer, Integer.BYTES, fieldName + " count");
        int count = buffer.getInt();
        if (count < 0 || count > MAX_ASSIGNMENT_ITEMS) {
            throw new IllegalArgumentException("Invalid " + fieldName + " count: " + count);
        }
        return count;
    }

    private static void requireRemaining(ByteBuffer buffer, int bytes, String fieldName) {
        if (bytes < 0 || buffer.remaining() < bytes) {
            throw new IllegalArgumentException("Malformed " + fieldName + ": need " + bytes
                    + " bytes, only " + buffer.remaining() + " available");
        }
    }
}

package com.minimq.protocol;

import com.minimq.common.error.ErrorCode;
import com.minimq.protocol.message.ProtocolMessages.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V4ProtocolContractTest {
    @Test
    void apiKeysRemainAppendOnlyAndContiguous() throws IllegalAccessException {
        Map<String, Integer> expected = new TreeMap<>();
        expected.put("CREATE_TOPIC", 0);
        expected.put("METADATA", 1);
        expected.put("PRODUCE", 2);
        expected.put("FETCH", 3);
        expected.put("JOIN_GROUP", 4);
        expected.put("SYNC_GROUP", 5);
        expected.put("HEARTBEAT", 6);
        expected.put("OFFSET_COMMIT", 7);
        expected.put("OFFSET_FETCH", 8);
        expected.put("REGISTER_BROKER", 9);
        expected.put("REPLICA_FETCH", 10);
        expected.put("MARK_BROKER_OFFLINE", 11);
        expected.put("UPDATE_REPLICA_LEO", 12);
        expected.put("MAINTENANCE_REPORT", 13);
        expected.put("MAINTENANCE_RUN", 14);
        expected.put("MAINTENANCE_METRICS", 15);
        expected.put("BROKER_HEARTBEAT", 16);
        expected.put("DISCOVER_CONTROLLER", 17);
        expected.put("LEADER_READY", 18);
        expected.put("RUNTIME_METRICS", 19);
        expected.put("OPS_STATUS", 20);
        expected.put("GROUP_STATUS", 21);
        expected.put("LIST_TOPICS", 22);
        expected.put("DESCRIBE_TOPIC", 23);
        expected.put("DELETE_TOPIC", 24);
        expected.put("INCREASE_PARTITIONS", 25);
        expected.put("FAILOVER_AUDIT", 26);
        expected.put("AUTH_STATUS", 27);
        expected.put("CONFIGURE_TOPIC_RELIABILITY", 28);
        expected.put("MARK_BROKER_OFFLINE_ADMIN", 29);
        expected.put("CREATE_TOPIC_ADMIN", 30);
        expected.put("PRODUCE_V2", 31);
        expected.put("REPLICA_FETCH_V2", 32);
        expected.put("TOPIC_DELETE_ACK", 33);
        expected.put("FETCH_V2", 34);
        expected.put("METADATA_V2", 35);
        expected.put("RUNTIME_METRICS_V2", 36);
        expected.put("UPDATE_REPLICA_STATE_V2", 37);

        Map<String, Integer> actual = publicShortConstants(ApiKeys.class);

        assertEquals(expected, actual);
        assertEquals(
                IntStream.rangeClosed(0, 37).boxed().collect(Collectors.toCollection(TreeSet::new)),
                new TreeSet<>(actual.values()));
    }

    @Test
    void errorCodesRemainAppendOnlyAndContiguous() {
        Map<String, Integer> expected = new TreeMap<>();
        expected.put("NONE", 0);
        expected.put("UNKNOWN_TOPIC", 1);
        expected.put("UNKNOWN_PARTITION", 2);
        expected.put("NOT_LEADER", 3);
        expected.put("INVALID_REQUEST", 4);
        expected.put("GROUP_NOT_FOUND", 5);
        expected.put("REBALANCE_IN_PROGRESS", 6);
        expected.put("UNAUTHORIZED", 7);
        expected.put("MAINTENANCE_BUSY", 8);
        expected.put("BROKER_FENCED", 9);
        expected.put("BROKER_ID_IN_USE", 10);
        expected.put("CONTROLLER_FENCED", 11);
        expected.put("NOT_CONTROLLER", 12);
        expected.put("STALE_LEADER_EPOCH", 13);
        expected.put("LEADER_NOT_READY", 14);
        expected.put("CONTROLLER_NOT_READY", 15);
        expected.put("THROTTLED", 16);
        expected.put("FORBIDDEN", 17);
        expected.put("UNKNOWN_BROKER", 18);
        expected.put("STALE_TOMBSTONE", 19);

        Map<String, Integer> actual = Arrays.stream(ErrorCode.values())
                .collect(Collectors.toMap(Enum::name, code -> (int) code.code, (a, b) -> a, TreeMap::new));

        assertEquals(expected, actual);
        assertEquals(
                IntStream.rangeClosed(0, 19).boxed().collect(Collectors.toCollection(TreeSet::new)),
                new TreeSet<>(actual.values()));
        assertEquals(ErrorCode.FORBIDDEN, ErrorCode.fromCode((short) 17));
        assertEquals(ErrorCode.UNKNOWN_BROKER, ErrorCode.fromCode((short) 18));
        assertEquals(ErrorCode.STALE_TOMBSTONE, ErrorCode.fromCode((short) 19));
    }

    @Test
    void v4RequestRecordsFreezeCanonicalFieldOrder() {
        assertRecordComponents(OpsStatusRequest.class,
                "credential", "brokerLimit", "partitionLimit");
        assertRecordComponents(GroupStatusRequest.class,
                "credential", "groupId", "topicFilter", "memberLimit", "partitionLimit");
        assertRecordComponents(ListTopicsRequest.class, "credential", "limit");
        assertRecordComponents(DescribeTopicRequest.class,
                "credential", "topicName", "partitionLimit", "includeDeleted");
        assertRecordComponents(DeleteTopicRequest.class, "credential", "topicName");
        assertRecordComponents(IncreasePartitionsRequest.class,
                "credential", "topicName", "targetPartitionCount");
        assertRecordComponents(FailoverAuditRequest.class, "credential", "limit");
        assertRecordComponents(AuthStatusRequest.class, "credential");
        assertRecordComponents(ConfigureTopicReliabilityRequest.class,
                "credential",
                "sourceTopicName",
                "retryTopicSupplied",
                "retryTopicName",
                "clearRetryTopic",
                "deadLetterTopicSupplied",
                "deadLetterTopicName",
                "clearDeadLetterTopic");
        assertRecordComponents(MarkBrokerOfflineAdminRequest.class,
                "credential", "brokerId");
        assertRecordComponents(CreateTopicAdminRequest.class,
                "credential", "topicName", "numPartitions", "replicationFactor");
        assertRecordComponents(ProduceV2Request.class,
                "topicName", "partitionId", "leaderEpoch", "records");
        assertRecordComponents(ReplicaFetchV2Request.class,
                "clusterCredential",
                "topicName",
                "partitionId",
                "replicaBrokerId",
                "brokerEpoch",
                "controllerEpoch",
                "leaderEpoch",
                "fetchOffset",
                "maxBytes");
        assertRecordComponents(TopicDeleteAckRequest.class,
                "clusterCredential",
                "topicName",
                "tombstoneVersion",
                "brokerId",
                "brokerEpoch",
                "controllerEpoch");
        assertRecordComponents(FetchV2Request.class,
                "topicName", "partitionId", "leaderEpoch", "fetchOffset", "maxBytes");
        assertRecordComponents(MetadataV2Request.class,
                "topicFilter", "topicLimit", "partitionLimit");
        assertRecordComponents(RuntimeMetricsV2Request.class,
                "credential", "counterLimit", "latencyLimit", "lagLimit");
        assertRecordComponents(UpdateReplicaStateV2Request.class,
                "clusterCredential",
                "topicName",
                "partitionId",
                "replicaBrokerId",
                "brokerEpoch",
                "controllerEpoch",
                "leaderEpoch",
                "physicalLogEndOffset",
                "visibleLogEndOffset",
                "nextVisibleAtEpochMs");
    }

    @Test
    void v4ResponseRecordsFreezeCanonicalFieldOrderAndRemainUnique() {
        assertRecordComponents(OpsStatusResponse.class,
                "targetBrokerId",
                "startedAtEpochMs",
                "snapshotAtEpochMs",
                "metadataVersion",
                "metadataFormatVersion",
                "role",
                "brokerEpoch",
                "controllerEpoch",
                "transportMode",
                "maintenanceState",
                "brokers",
                "partitions");
        assertRecordComponents(GroupStatusResponse.class,
                "groupId", "topicName", "generationId", "members", "partitionLags");
        assertRecordComponents(ListTopicsResponse.class, "topicNames");
        assertRecordComponents(DescribeTopicResponse.class,
                "topicName",
                "metadataVersion",
                "active",
                "retryTopic",
                "deadLetterTopic",
                "partitions",
                "tombstone");
        assertRecordComponents(DeleteTopicResponse.class,
                "topicName",
                "tombstoneState",
                "tombstoneVersion",
                "deletedAtEpochMs",
                "requiredBrokerIds",
                "acknowledgedBrokerIds",
                "pendingBrokerIds");
        assertRecordComponents(IncreasePartitionsResponse.class,
                "topicName", "previousPartitionCount", "newPartitionCount", "metadataVersion");
        assertRecordComponents(FailoverAuditResponse.class,
                "brokerId", "brokerStartedAtEpochMs", "overwrittenCount", "events");
        assertRecordComponents(AuthStatusResponse.class, "targetBrokerId", "scopes");
        assertRecordComponents(ConfigureTopicReliabilityResponse.class,
                "sourceTopicName", "retryTopicName", "deadLetterTopicName", "metadataVersion");
        assertRecordComponents(MarkBrokerOfflineAdminResponse.class,
                "brokerId", "metadataVersion");
        assertRecordComponents(CreateTopicAdminResponse.class, "topicName", "metadataVersion");
        assertRecordComponents(ProduceV2Response.class,
                "topicName", "partitionId", "baseOffset");
        assertRecordComponents(ReplicaFetchV2Response.class,
                "topicName",
                "partitionId",
                "physicalHighWatermark",
                "visibleHighWatermark",
                "nextVisibleAtEpochMs",
                "leaderEpoch",
                "leaderLogEndOffset",
                "records");
        assertRecordComponents(TopicDeleteAckResponse.class);
        assertRecordComponents(FetchV2Response.class,
                "topicName",
                "partitionId",
                "physicalHighWatermark",
                "visibleHighWatermark",
                "nextVisibleAtEpochMs",
                "records");
        assertRecordComponents(MetadataV2Response.class,
                "brokers", "topics", "controller", "metadataVersion", "metadataFormatVersion");
        assertRecordComponents(RuntimeMetricsV2Response.class,
                "startedAtEpochMs",
                "snapshotAtEpochMs",
                "uptimeMs",
                "requestsByApiKey",
                "responsesByErrorCode",
                "latencyByApiKey",
                "throttledByApiKey",
                "produceBytes",
                "fetchBytes",
                "auditOverwrittenCount",
                "lagMetrics");
        assertRecordComponents(UpdateReplicaStateV2Response.class);

        var responseTypes = List.of(
                OpsStatusResponse.class,
                GroupStatusResponse.class,
                ListTopicsResponse.class,
                DescribeTopicResponse.class,
                DeleteTopicResponse.class,
                IncreasePartitionsResponse.class,
                FailoverAuditResponse.class,
                AuthStatusResponse.class,
                ConfigureTopicReliabilityResponse.class,
                MarkBrokerOfflineAdminResponse.class,
                CreateTopicAdminResponse.class,
                ProduceV2Response.class,
                ReplicaFetchV2Response.class,
                TopicDeleteAckResponse.class,
                FetchV2Response.class,
                MetadataV2Response.class,
                RuntimeMetricsV2Response.class,
                UpdateReplicaStateV2Response.class);
        assertEquals((long) responseTypes.size(), responseTypes.stream().distinct().count());
        assertNotSame(TopicDeleteAckResponse.class, UpdateReplicaStateV2Response.class);
    }

    @Test
    void v4ReusableItemRecordsFreezeCanonicalFieldOrder() {
        assertRecordComponents(V4BoundedList.class, "total", "truncated", "items");
        assertRecordComponents(OpsBrokerInfo.class,
                "brokerId", "host", "port", "state", "brokerEpoch", "leaseDeadlineEpochMs");
        assertRecordComponents(OpsPartitionInfo.class,
                "topicName",
                "partitionId",
                "leaderId",
                "leaderEpoch",
                "physicalHighWatermark",
                "visibleHighWatermark",
                "nextVisibleAtEpochMs",
                "readiness",
                "frozen",
                "replicaIds",
                "isrIds");
        assertRecordComponents(GroupMemberInfo.class,
                "memberId", "active", "lastHeartbeatEpochMs", "assignments");
        assertRecordComponents(GroupAssignmentInfo.class, "topicName", "partitionId");
        assertRecordComponents(PartitionLagInfo.class,
                "topicName",
                "partitionId",
                "committedOffset",
                "commitState",
                "physicalHighWatermark",
                "physicalLag",
                "visibleHighWatermark",
                "visibleLag",
                "nextVisibleAtEpochMs",
                "readiness",
                "frozen");
        assertRecordComponents(TopicSummary.class,
                "topicName", "partitionCount", "replicationFactor", "retryTopic", "deadLetterTopic");
        assertRecordComponents(TopicPartitionInfo.class,
                "partitionId", "leaderId", "leaderEpoch", "replicaIds", "isrIds", "readiness", "frozen");
        assertRecordComponents(TopicTombstoneInfo.class,
                "state",
                "tombstoneVersion",
                "deletedAtEpochMs",
                "requiredBrokerIds",
                "acknowledgedBrokerIds",
                "pendingBrokerIds");
        assertRecordComponents(AuditEventInfo.class,
                "sequence",
                "timestampEpochMs",
                "eventType",
                "brokerId",
                "topicName",
                "partitionId",
                "oldValue",
                "newValue",
                "detail");
        assertRecordComponents(ScheduledProduceRecord.class,
                "key", "value", "deliverAtEpochMs");
        assertRecordComponents(ScheduledReplicaRecord.class,
                "offset", "key", "value", "deliverAtEpochMs");
        assertRecordComponents(FetchedRecord.class, "offset", "key", "value");
        assertRecordComponents(RuntimeMetricCount.class, "apiKey", "count");
        assertRecordComponents(RuntimeErrorCount.class, "errorCode", "count");
        assertRecordComponents(RuntimeLatencyV2Summary.class,
                "apiKey", "count", "totalNanos", "maxMillis");
    }

    private static Map<String, Integer> publicShortConstants(Class<?> type) throws IllegalAccessException {
        Map<String, Integer> constants = new TreeMap<>();
        for (Field field : type.getFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && field.getType() == short.class) {
                constants.put(field.getName(), (int) field.getShort(null));
            }
        }
        return constants;
    }

    private static void assertRecordComponents(Class<?> type, String... expectedNames) {
        assertTrue(type.isRecord(), type.getName() + " must remain a record");
        assertArrayEquals(
                expectedNames,
                Arrays.stream(type.getRecordComponents())
                        .map(component -> component.getName())
                        .toArray(String[]::new));
    }
}

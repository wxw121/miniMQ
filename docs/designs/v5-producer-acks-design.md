# V5.0 Producer Acknowledgements And Group Hardening Design

Date: 2026-08-14
Status: Implemented in source. Latest full `mvn clean verify` is [test-report-2026-08-14-v5.0.0-reassign-preferred.md](../test-report-2026-08-14-v5.0.0-reassign-preferred.md) (785 tests). The previous same-day acks + min.insync + leave pass remains in [test-report-2026-08-14-v5.0.0-acks-minisr-leave.md](../test-report-2026-08-14-v5.0.0-acks-minisr-leave.md). The acks-only slice remains in [test-report-2026-08-14-v5.0.0-producer-acks.md](../test-report-2026-08-14-v5.0.0-producer-acks.md).

## Goal

V4 replicated partitions and tracked ISR log-end offsets, but `PRODUCE` / `PRODUCE_V2` returned as soon as the Leader appended locally. V5 adds an explicit acknowledgement policy so a producer can wait until the current ISR has the record, without introducing transactions or exactly-once semantics.

The same version also hardens produce waits and consumer-group membership: `acks=ALL` runs off the Netty event loop, topics can require a minimum ISR size, members can leave, and session timeout is per member.

V4 已经有副本和 ISR LEO，但生产在 Leader 本地追加后立即返回。V5 增加明确的确认策略，让 Producer 可以等到当前 ISR 拥有该记录。本版本不包含事务或 exactly-once。同一版本还把 `ALL` 等待移出 Netty event loop，并补上 `min.insync.replicas`、`LEAVE_GROUP` 和按成员 session timeout。

## Scope

1. Two ack modes: `LEADER` (1) and `ALL` / ISR (`-1`).
2. Optional trailing `acks` and `ackTimeoutMs` on `PRODUCE` (2) and `PRODUCE_V2` (31).
3. Decode accepts the V4 body (no trailing fields) and defaults to `LEADER` + 5000 ms.
4. Encode always writes the current fields.
5. `ALL` waits until `ReplicaStateManager.replicationHighWatermark` is at least the Leader log-end offset after the append.
6. Append-only error codes `NOT_ENOUGH_REPLICAS=20` and `REQUEST_TIMED_OUT=21`.
7. Broker-scoped produce-ack executor (4 daemon threads, shared across connections) so `ALL` waits do not block the Netty event loop.
8. Topic-level `min.insync.replicas` (default 1, must be `1..replicationFactor`) on `CREATE_TOPIC` / `CREATE_TOPIC_ADMIN`, exposed as an optional trailing field on `DESCRIBE_TOPIC`.
9. `LEAVE_GROUP` (key 38). Leave is allowed during rebalance and is idempotent (`NONE` for unknown or already-left members). Empty groups keep committed offsets. Heartbeat after leave is still `GROUP_NOT_FOUND`.
10. Optional trailing `sessionTimeoutMs` on `JOIN_GROUP` (default 10000, range 1000..300000). Eviction uses that member's timeout. `Consumer.close()` sends leave.
11. `ALTER_TOPIC_CONFIG` (key 39) changes `min.insync.replicas` after create (`1..replicationFactor`).
12. `REASSIGN_PARTITIONS` (key 40) replaces a partition replica list. The new set must overlap the current ISR so leadership stays clean. New replicas start out of ISR until they catch up.
13. `ELECT_PREFERRED_LEADERS` (key 41) moves leadership to `replicaBrokerIds[0]` when that replica is in the ISR.

## Non-Goals

- `acks=0` (no response). MiniMQ stays request/response.
- Changing `TopicSummary` / `METADATA_V2` layout. Followers in etcd mode get `min.insync.replicas` from the metadata document; legacy `METADATA_V2` sync keeps a previously observed value or defaults to 1.
- Transactions, idempotent producers, or exactly-once.
- Unclean election.
- Changing FETCH visibility or consumer offset rules.

## Ack Modes

| Code | Name | When the Leader responds |
| ---: | --- | --- |
| 1 | `LEADER` | After the local append (V4 behavior). Default. |
| -1 | `ALL` | After every current ISR replica's LEO is at least the post-append Leader LEO, or the wait times out. |

`ackTimeoutMs` is `1..60000`, default `5000`. Other `acks` values are `INVALID_REQUEST`.

`ALL` with an empty ISR, or with `isr.size() < min.insync.replicas`, returns `NOT_ENOUGH_REPLICAS` and does not wait. A single-broker ISR that meets `min.insync.replicas` is equivalent to `LEADER` after the local append is visible.

A timeout or `NOT_ENOUGH_REPLICAS` does **not** roll back the local append. The record may already be on the Leader. The client sees the error and may retry; the retry is a second record unless the application deduplicates.

超时或 ISR 不足**不会**回滚本地追加。记录可能已在 Leader 上。客户端看到错误后重试会再写一条，除非应用自己去重。

## Wire Format

Frame version stays 1. Field order of the existing produce body is unchanged; V5 appends two trailing fields.

当前 `PRODUCE` body：

```text
topicName (str16)
partitionId (i32)
leaderEpoch (i64)
records (count + key/value bytes)
acks (i16)            // optional on decode
ackTimeoutMs (i32)    // optional on decode
```

当前 `PRODUCE_V2` body keeps its V4 prefix (topic, partition, epoch, bounded scheduled records) and appends the same two fields.

Missing trailing fields default to `acks=1`, `ackTimeoutMs=5000`. A body that includes `acks` but omits `ackTimeoutMs` uses the default timeout. Encode never emits the V4 shape.

A V4 client that omits the fields keeps Leader-ack behavior. A V5 client talking to a V4 Broker is unsupported for `ALL` (the V4 Broker rejects trailing bytes). Mixed V4/V5 Broker clusters remain unsupported, same as V3/V4.

`CREATE_TOPIC` and `CREATE_TOPIC_ADMIN` append optional `minInSyncReplicas` (i32, default 1). `JOIN_GROUP` appends optional `sessionTimeoutMs` (i32, default 10000). `DESCRIBE_TOPIC` responses append optional `minInSyncReplicas` (i32, default 1). `LEAVE_GROUP` is a new key (38) with `groupId`, `memberId`, `generationId` and an empty success body. `ALTER_TOPIC_CONFIG` (39), `REASSIGN_PARTITIONS` (40), and `ELECT_PREFERRED_LEADERS` (41) are append-only `TOPIC_ADMIN` APIs after `LEAVE_GROUP`. Nested replica/partition lists use the existing item-count encoding (max 10000), not `V4BoundedList`.

```text
ALTER_TOPIC_CONFIG request:  credential, topicName, minInSyncReplicas (i32)
ALTER_TOPIC_CONFIG response: topicName, minInSyncReplicas, metadataVersion
REASSIGN_PARTITIONS request: credential, topicName, assignmentCount, {partitionId, replicaCount, replicaIds…}…
REASSIGN_PARTITIONS response: topicName, metadataVersion, changedCount, changedPartitionIds…
ELECT_PREFERRED_LEADERS request: credential, topicName, partitionCount (0=all), partitionIds…
ELECT_PREFERRED_LEADERS response: topicName, metadataVersion, electedCount, electedPartitionIds…
```

## Broker Behavior

After a successful local append and the existing local LEO / replica-state report:

1. `LEADER` returns `ProduceResponse` / `ProduceV2Response` immediately on the request thread.
2. `ALL` reads `ClusterController.getIsrBrokerIds` and the topic's `min.insync.replicas`. Empty ISR or `isr.size() < minInSync` → `NOT_ENOUGH_REPLICAS`.
3. Otherwise the wait is submitted to the broker-scoped produce-ack executor. The Netty event loop is not blocked for `ackTimeoutMs`.
4. The waiter uses the **current** ISR and aborts if this node is no longer leader (`NOT_LEADER`) or live ISR size drops below `min.insync.replicas` (`NOT_ENOUGH_REPLICAS`). ISR is rechecked every 50 ms. Success is `replicationHighWatermark(topic, partition, leaderLeo, liveIsr, leaderId) >= leaderLeo`.
5. Timeout → `REQUEST_TIMED_OUT` with no response body. Shutdown/rejection of the executor is treated the same way.

`leaderLeo` is the partition `highWatermark()` after the append (next offset). That is the same value already reported as the Leader LEO today.

`ReplicaStateManager.replicationHighWatermark` must not drop the election-time LEO on the first post-election write computation. Clearing that marker would treat later unreported ISR followers as caught up, so `acks=ALL` would return success before the record is on the ISR. The marker is cleared only after every current follower has reported a LEO.

## Client Behavior

`Producer.send(record)` keeps `LEADER` + 5000 ms. Overloads accept `ProduceAcks` and an optional timeout. The same fields are written on `PRODUCE_V2` retry/DLQ paths when the caller asked for `ALL`.

CLI `produce` gains `--acks leader|all` and optional `--ack-timeout-ms`.
CLI `create-topic` gains `--min-insync-replicas`.
CLI `consume` gains `--session-timeout-ms`. Heartbeat interval is `max(100, sessionTimeoutMs / 3)`.
CLI `alter-topic-config --min-insync-replicas`, `reassign-partitions --assignment partitionId:brokerId[,brokerId...]`, and `elect-preferred-leaders [--partition id]`.

## Compatibility

| Client | Broker | Result |
| --- | --- | --- |
| V4 produce (no trailing fields) | V5 | Decodes as `LEADER`, same as today |
| V5 `LEADER` | V5 | Current encode, no wait |
| V5 `ALL` | V5 | Waits for current ISR that meets `min.insync.replicas` |
| V5 `ALL` | V4 | V4 rejects trailing bytes (`INVALID_REQUEST`) |
| V4 join (no session timeout) | V5 | Decodes as 10000 ms |
| V4 create-topic (no min ISR) | V5 | Decodes as `min.insync.replicas=1` |

Keys 0-19 keep original field order. New fields follow the same optional-trailing pattern as `HEARTBEAT` / `OFFSET_COMMIT`. `LEAVE_GROUP=38` is append-only after `UPDATE_REPLICA_STATE_V2=37`. `ALTER_TOPIC_CONFIG=39`, `REASSIGN_PARTITIONS=40`, and `ELECT_PREFERRED_LEADERS=41` follow `LEAVE_GROUP`.

## Follow-ups (not V5.0)

- Optional unclean election.
- Transactional produce / exactly-once research.
- Optional experimental LSM state-store module.
- Cooperative rebalance / static membership.
- Record headers or compression.

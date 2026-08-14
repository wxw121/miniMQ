# V5.0 Producer Acknowledgements Design

Date: 2026-08-14
Status: Implemented in source. Full `mvn clean verify` is recorded in [test-report-2026-08-14-v5.0.0-producer-acks.md](../test-report-2026-08-14-v5.0.0-producer-acks.md). Dedicated executor for `ALL` waits is still a follow-up.

## Goal

V4 replicated partitions and tracked ISR log-end offsets, but `PRODUCE` / `PRODUCE_V2` returned as soon as the Leader appended locally. V5 adds an explicit acknowledgement policy so a producer can wait until the current ISR has the record, without introducing transactions or exactly-once semantics.

V4 已经有副本和 ISR LEO，但生产在 Leader 本地追加后立即返回。V5 增加明确的确认策略，让 Producer 可以等到当前 ISR 拥有该记录。本版本不包含事务或 exactly-once。

## Scope

1. Two ack modes: `LEADER` (1) and `ALL` / ISR (`-1`).
2. Optional trailing `acks` and `ackTimeoutMs` on `PRODUCE` (2) and `PRODUCE_V2` (31).
3. Decode accepts the V4 body (no trailing fields) and defaults to `LEADER` + 5000 ms.
4. Encode always writes the current fields.
5. `ALL` waits until `ReplicaStateManager.replicationHighWatermark` is at least the Leader log-end offset after the append.
6. Append-only error codes `NOT_ENOUGH_REPLICAS=20` and `REQUEST_TIMED_OUT=21`.

## Non-Goals

- `acks=0` (no response). MiniMQ stays request/response.
- `min.insync.replicas`. `ALL` waits for the current ISR, whatever its size.
- Transactions, idempotent producers, or exactly-once.
- Unclean election.
- Changing FETCH visibility or consumer offset rules.

## Ack Modes

| Code | Name | When the Leader responds |
| ---: | --- | --- |
| 1 | `LEADER` | After the local append (V4 behavior). Default. |
| -1 | `ALL` | After every current ISR replica's LEO is at least the post-append Leader LEO, or the wait times out. |

`ackTimeoutMs` is `1..60000`, default `5000`. Other `acks` values are `INVALID_REQUEST`.

`ALL` with an empty ISR returns `NOT_ENOUGH_REPLICAS` and does not wait. A single-broker ISR is equivalent to `LEADER`.

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

## Broker Behavior

After a successful local append and the existing local LEO / replica-state report:

1. `LEADER` returns `ProduceResponse` / `ProduceV2Response` immediately.
2. `ALL` reads `ClusterController.getIsrBrokerIds`. Empty ISR → `NOT_ENOUGH_REPLICAS`.
3. Otherwise wait on `ReplicaStateManager` until `replicationHighWatermark(topic, partition, leaderLeo, isr, leaderId) >= leaderLeo` or `ackTimeoutMs` elapses.
4. Timeout → `REQUEST_TIMED_OUT` with no response body.

`leaderLeo` is the partition `highWatermark()` after the append (next offset). That is the same value already reported as the Leader LEO today.

The wait must not block a Netty event-loop thread for the full timeout. Stage 1 may wait inline only in unit tests that use a short timeout; a dedicated executor is the next hardening step. Tests use a short timeout and an injectable or directly updated `ReplicaStateManager`.

`ReplicaStateManager.replicationHighWatermark` must not drop the election-time LEO on the first post-election write computation. Clearing that marker would treat later unreported ISR followers as caught up, so `acks=ALL` would return success before the record is on the ISR. The marker is cleared only after every current follower has reported a LEO.

## Client Behavior

`Producer.send(record)` keeps `LEADER` + 5000 ms. Overloads accept `ProduceAcks` and an optional timeout. The same fields are written on `PRODUCE_V2` retry/DLQ paths when the caller asked for `ALL`.

CLI `produce` gains `--acks leader|all` and optional `--ack-timeout-ms`.

## Compatibility

| Client | Broker | Result |
| --- | --- | --- |
| V4 produce (no trailing fields) | V5 | Decodes as `LEADER`, same as today |
| V5 `LEADER` | V5 | Current encode, no wait |
| V5 `ALL` | V5 | Waits for current ISR |
| V5 `ALL` | V4 | V4 rejects trailing bytes (`INVALID_REQUEST`) |

Keys 0-19 keep original field order. `PRODUCE` follows the same optional-trailing pattern as `HEARTBEAT` / `OFFSET_COMMIT`.

## Follow-ups (not V5.0)

- Move `acks=ALL` waits off the Netty event loop onto a bounded dedicated executor.
- Configurable `min.insync.replicas`.
- Consumer-group leave and configurable session timeout.
- Optional unclean election.
- Transactional produce / exactly-once research.

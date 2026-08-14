# V5.0 Producer Acknowledgements And Group Hardening Implementation Plan

Date: 2026-08-14
Status: Implemented and verified. Latest evidence: [test-report-2026-08-14-v5.0.0-reassign-preferred.md](../test-report-2026-08-14-v5.0.0-reassign-preferred.md) (785 tests, line ≥ 90%). The previous acks + min.insync + leave pass remains in [test-report-2026-08-14-v5.0.0-acks-minisr-leave.md](../test-report-2026-08-14-v5.0.0-acks-minisr-leave.md). The acks-only slice remains in [test-report-2026-08-14-v5.0.0-producer-acks.md](../test-report-2026-08-14-v5.0.0-producer-acks.md).

## Objective

Implement the V5 contract in [v5-producer-acks-design.md](../designs/v5-producer-acks-design.md): produce acks, dedicated ISR wait executor, `min.insync.replicas`, `LEAVE_GROUP`, and per-member session timeout. Do not implement transactions.

## Execution Rules

- Keep protocol frame version 1 and original field order for existing produce bytes.
- Error codes and API keys stay append-only. New codes are 20 and 21. New API keys are `LEAVE_GROUP=38`, `ALTER_TOPIC_CONFIG=39`, `REASSIGN_PARTITIONS=40`, and `ELECT_PREFERRED_LEADERS=41`.
- Bound every wait (`ackTimeoutMs` max 60_000, session timeout max 300_000).
- Do not lower the JaCoCo line ≥ 90% gate.
- Use `.mvn/settings.xml` for Maven commands.
- Close brokers, clients, executors, and event loops started by tests.

## Stage 1: Protocol And Wait Primitive

- Append `NOT_ENOUGH_REPLICAS=20` and `REQUEST_TIMED_OUT=21`.
- Add `ProduceAcks` (`LEADER=1`, `ALL=-1`) with default timeout 5000.
- Extend `ProduceRequest` and `ProduceV2Request` with `acks` and `ackTimeoutMs`. Convenience constructors keep V4 call sites on Leader-ack defaults.
- Decode optional trailing fields; encode always writes them.
- `ReplicaStateManager.awaitReplicationHighWatermark` waits until the current ISR HW meets the required Leader LEO, or times out.
- Tests: legacy produce decode, invalid acks, waiter success/timeout, V4 error-code numbers unchanged.

## Stage 2: Broker Produce Path

- After append, `LEADER` returns immediately.
- `ALL` with empty ISR or `isr.size() < min.insync.replicas` returns `NOT_ENOUGH_REPLICAS`.
- Otherwise wait on the produce-ack executor, then `NONE` or `REQUEST_TIMED_OUT`.
- Do not roll back the append on timeout.
- Keep the wait off the Netty event loop.

## Stage 3: Client And CLI

- `Producer.send` overloads accept `ProduceAcks` and timeout.
- Default send path remains `LEADER`.
- CLI `produce --acks leader|all` and `--ack-timeout-ms`.

## Stage 4: min.insync, leave, session timeout

- Optional trailing `minInSyncReplicas` on create-topic APIs and describe-topic responses. Do not change `TopicSummary`.
- `LEAVE_GROUP=38`; leave ignores generation; empty groups keep offsets.
- Optional trailing `sessionTimeoutMs` on `JOIN_GROUP`; eviction and `GROUP_STATUS` alive bits use the per-member value.
- Consumer heartbeat interval scales with session timeout; `close()` sends leave.
- CLI `--min-insync-replicas` and `--session-timeout-ms`.

## Stage 4b: Topic config, reassignment, preferred leader

- `ALTER_TOPIC_CONFIG` updates `min.insync.replicas` when `1..min(partition RF)`.
- `REASSIGN_PARTITIONS` replaces replica lists; require ISR overlap; new replicas join ISR only after catching up.
- `ELECT_PREFERRED_LEADERS` promotes `replicaBrokerIds[0]` when it is in the ISR.
- CLI: `alter-topic-config`, `reassign-partitions --assignment`, `elect-preferred-leaders`.

## Stage 5: Docs And Version

- Maven version `5.0.0`.
- Update roadmap, README, design.md, architecture.md, docs/README.md.
- Record verification in a dated test report when `mvn clean verify` is run. Latest: [test-report-2026-08-14-v5.0.0-reassign-preferred.md](../test-report-2026-08-14-v5.0.0-reassign-preferred.md).

## Dependency Order

Stage 2 depends on Stage 1. Stage 3 depends on Stage 1 encode. Stage 4 can proceed after Stage 1 protocol helpers. Stage 5 can start in parallel with Stage 1.

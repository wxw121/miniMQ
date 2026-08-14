# V5.0 Producer Acknowledgements And Group Hardening Implementation Plan

Date: 2026-08-14
Status: Implemented. Acks-only verification recorded. Expansion (ack executor, min.insync, leave, session timeout) lands in the same 5.0.0 tree.

## Objective

Implement the V5 contract in [v5-producer-acks-design.md](../designs/v5-producer-acks-design.md): produce acks, dedicated ISR wait executor, `min.insync.replicas`, `LEAVE_GROUP`, and per-member session timeout. Do not implement transactions.

## Execution Rules

- Keep protocol frame version 1 and original field order for existing produce bytes.
- Error codes and API keys stay append-only. New codes are 20 and 21. New API key is `LEAVE_GROUP=38`.
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

## Stage 5: Docs And Version

- Maven version `5.0.0`.
- Update roadmap, README, design.md, architecture.md, docs/README.md.
- Record verification in a dated test report when `mvn clean verify` is run.

## Dependency Order

Stage 2 depends on Stage 1. Stage 3 depends on Stage 1 encode. Stage 4 can proceed after Stage 1 protocol helpers. Stage 5 can start in parallel with Stage 1.

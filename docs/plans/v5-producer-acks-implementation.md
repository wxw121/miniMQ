# V5.0 Producer Acknowledgements Implementation Plan

Date: 2026-08-14
Status: In progress.

## Objective

Implement the V5 produce-ack contract in [v5-producer-acks-design.md](../designs/v5-producer-acks-design.md): protocol fields, ISR wait, client/CLI defaults, and documentation. Do not implement transactions.

## Execution Rules

- Keep protocol frame version 1 and original field order for existing produce bytes.
- Error codes and API keys stay append-only. New codes are 20 and 21 only.
- Bound every wait (`ackTimeoutMs` max 60_000).
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
- `ALL` with empty ISR returns `NOT_ENOUGH_REPLICAS`.
- Otherwise wait, then `NONE` or `REQUEST_TIMED_OUT`.
- Do not roll back the append on timeout.
- Keep the wait off the Netty event loop when a dedicated executor is available; Stage 1 may wait inline only in unit tests that use a short timeout.

## Stage 3: Client And CLI

- `Producer.send` overloads accept `ProduceAcks` and timeout.
- Default send path remains `LEADER`.
- CLI `produce --acks leader|all` and `--ack-timeout-ms`.

## Stage 4: Docs And Version

- Maven version `5.0.0`.
- Update roadmap, README, design.md, architecture.md, docs/README.md.
- Record verification in a dated test report when `mvn clean verify` is run.

## Dependency Order

Stage 2 depends on Stage 1. Stage 3 depends on Stage 1 encode. Stage 4 can start in parallel with Stage 1.

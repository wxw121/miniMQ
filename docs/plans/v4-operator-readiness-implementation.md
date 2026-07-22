# V4.0 Operator Readiness Implementation Plan

Date: 2026-07-21
Status: Implementation plan only. Start coding after reviewing `docs/designs/v4-operator-readiness-design.md`.

## Objective

Deliver V4.0 as one consolidated major version focused on operations readiness, diagnostics, consumer lag, topic administration, local multi-node demonstration, scoped operator permissions, and practical application-reliability features.

## Execution Policy

- Use small, verifiable tasks and keep resource use bounded.
- If a blocking issue remains unresolved after five bounded attempts, document it in a dated known-issues file and continue with the next independent task.
- Close every broker, client, executor, Netty event loop, file handle, and Docker resource started during tests or demos.
- Prefer focused tests first, then broaden to module-level tests, then full Maven verification when practical.

## Task 1: Release Gate Baseline

**Files**

- Modify: `docs/testing.md`
- Modify or create: `docs/known-issues-*.md`
- Modify or create: V4.0 test report under `docs/`

**Steps**

- [ ] Re-run focused V3.0.1 failover-read stabilization tests.
- [ ] Check Docker API access before any real-etcd command.
- [ ] If Docker is accessible, run `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify`.
- [ ] If Docker is still blocked after five bounded attempts, record the blocker and continue V4.0 work without marking the real-etcd gate complete.

## Task 2: Broker And Cluster Operations Status

**Files**

- Modify: `mq-protocol/src/main/java/com/minimq/protocol/ApiKeys.java`
- Modify: `mq-protocol/src/main/java/com/minimq/protocol/message/ProtocolMessages.java`
- Modify: `mq-protocol/src/main/java/com/minimq/protocol/ProtocolSerdes.java`
- Modify: `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`
- Modify: `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`
- Test: protocol, embedded broker, and CLI tests

**Steps**

- [ ] Add read-only status request/response records with bounded partition and metric lists.
- [ ] Route the API through read-only operator authorization.
- [ ] Render `ops-status` with broker state, controller role, broker epoch, metadata version, transport mode, runtime metrics, maintenance state, leader readiness, ISR, and frozen partitions.
- [ ] Verify unauthorized access, malformed payloads, empty cluster state, and multi-partition output.

## Task 3: Consumer Group Status And Lag

**Files**

- Modify: `mq-broker/src/main/java/com/minimq/broker/controller/GroupCoordinator.java`
- Modify: protocol messages and serdes
- Modify: `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`
- Test: group coordinator, embedded broker, client, and CLI tests

**Steps**

- [ ] Expose members, assignment, committed offsets, high watermarks, and lag.
- [ ] Add `group-status --group <id> [--topic <name>]`.
- [ ] Verify empty groups, inactive members, retained/compacted offsets, and lag after produce/consume/commit.

## Task 4: Topic Administration Completeness

**Files**

- Modify: controller metadata and protocol paths
- Modify: `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`
- Test: controller, metadata, embedded broker, and CLI tests

**Steps**

- [ ] Add list-topics and describe-topic.
- [ ] Add delete-topic with explicit operator permission and safe metadata cleanup.
- [ ] Add increase-partitions without shrinking or reassigning existing partitions.
- [ ] Verify client metadata refresh and stale-route behavior after metadata changes.

## Task 5: Failover Audit And Metrics

**Files**

- Modify: controller, broker lease, leader activation, metadata watch, and metrics classes
- Modify: protocol messages and CLI
- Test: focused controller/failover/metrics tests

**Steps**

- [ ] Record bounded audit entries for controller takeover, broker lease expiry, self-fencing, leader transition, ISR changes, stale-epoch rejection, and leader-readiness transitions.
- [ ] Add `failover-audit` CLI output with newest-first ordering and event limits.
- [ ] Extend runtime metrics with produce/fetch rates, bytes in/out, throttling, latency summaries, and lag snapshots.

## Task 6: Local Multi-Node Demonstration

**Files**

- Modify or create scripts under `scripts/`
- Modify: `README.md`
- Modify: `docs/testing.md`
- Test: bounded local harness tests

**Steps**

- [ ] Add a local three-broker harness with bounded startup and shutdown.
- [ ] Provide a repeatable flow: create topic, produce, consume, stop current leader, observe failover, produce/fetch again.
- [ ] Ensure every process started by scripts has a documented stop path.

## Task 7: Scoped Operator Permissions

**Files**

- Modify: broker auth checks, config, protocol responses, and CLI parsing
- Test: authorization and backward-compatibility tests

**Steps**

- [ ] Preserve the existing admin-token behavior as the compatibility mode.
- [ ] Add scoped local permissions for status, topic admin, maintenance, and broker-offline operations.
- [ ] Verify token redaction in CLI output and logs.

## Task 8: Retry Topic, Dead Letter Topic, And Optional Delay

**Files**

- Modify: broker/client/demo paths as needed
- Test: protocol, broker, client, and CLI tests

**Steps**

- [ ] Add explicit retry-topic and dead-letter-topic metadata links.
- [ ] Add CLI commands or flags to route failed records to retry or dead-letter topics.
- [ ] Add delayed-message support only after Tasks 1-7 are complete and resource headroom is acceptable.
- [ ] Verify restart behavior, idempotent recovery, and ordinary consumer compatibility.

## Task 9: Documentation And Release Evidence

**Files**

- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/design.md`
- Modify: `docs/architecture.md`
- Modify: `docs/testing.md`
- Modify: `docs/roadmap.md`
- Create: V4.0 test report under `docs/`

**Steps**

- [ ] Update API tables, CLI examples, architecture diagrams, testing guide, and roadmap from the actual implementation.
- [ ] Record all focused and full verification commands with exit codes.
- [ ] Keep future LSM, transaction, and large-scale soak work as design-first deferred items.

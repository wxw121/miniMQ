# MiniMQ Roadmap

This roadmap summarizes the completed baseline, the next consolidated major version, and deferred future work.

本路线图说明当前完成状态、下一次集中大版本规划，以及后续延后事项。

## 1. Current State

MiniMQ has completed the V3.3-V3.5 production-hardening snapshot on top of the V3.0.0 Controller HA baseline.

Current implemented capabilities include:

- Java 17 Maven multi-module project with `mq-common`, `mq-storage`, `mq-protocol`, `mq-broker`, `mq-client`, and `mq-demo`.
- Custom Netty binary protocol and protocol serdes tests.
- WAL-backed segmented append-only log, sparse index, retention, compaction, checkpoints, recovery, and zero-copy fetch.
- Producer and Consumer SDKs with metadata refresh and leader-aware retry behavior.
- Consumer group assignment, offset commit/fetch, and group coordination basics.
- Broker replication, ISR tracking, leader epoch fencing, automatic failover, broker leases, and frozen-partition handling.
- etcd-backed Controller election and metadata persistence for the HA path.
- Maintenance admin APIs, runtime metrics, deterministic resilience tests, and transport selection for NIO/auto/epoll.
- CLI and Web demo flows for local produce/consume validation.

## 2. Remaining Gate

The most important remaining validation gate is environment-dependent real-etcd Failsafe verification for Controller HA.

Current policy:

- Check Docker API access before running any `-Petcd-it` command.
- If Docker access is available, run the real-etcd Failsafe profile and record fresh results.
- If the same environment blocker remains unresolved after five bounded attempts, document it in a known-issues file and continue with independent work.
- Do not mark the real-etcd gate complete until the focused Failsafe tests run successfully.

## 3. Next Major Version: V4.0 Operator Readiness

V4.0 is the next consolidated major release. It folds the previously separate release-gate, multi-node operations, consumer-lag, topic-admin, failover-audit, metrics, local-demo, scoped-permission, retry-topic, dead-letter-topic, and optional delayed-message work into one major version.

Authoritative planning documents:

- [V4.0 Operator Readiness Design](designs/v4-operator-readiness-design.md)
- [V4.0 Operator Readiness Implementation Plan](plans/v4-operator-readiness-implementation.md)

### V4.0 Scope

| Area | Capability |
| --- | --- |
| Release gate | Recheck focused HA/failover evidence and attempt real-etcd verification when Docker is accessible. |
| Operations status | Add `ops-status` for broker state, controller role, leader readiness, ISR, metadata version, runtime metrics, maintenance state, and frozen partitions. |
| Consumer lag | Add `group-status` for group members, assignment, committed offsets, high watermarks, and lag. |
| Topic admin | Add list, describe, delete, and increase-partitions flows. |
| Failover audit | Record bounded controller takeover, lease expiry, self-fencing, leader transition, ISR, stale epoch, and leader readiness events. |
| Metrics | Add throughput, bytes in/out, latency, throttling, and lag snapshots. |
| Local demo | Provide a bounded three-broker demo and cleanup path. |
| Permissions | Preserve admin-token compatibility while adding scoped operator permissions. |
| Reliability extensions | Add retry-topic and dead-letter-topic support; include delayed messages only after the operations surfaces are complete. |

### V4.0 Non-Goals

- Transactions and exactly-once semantics.
- Replacing the default segmented-log write path with an LSM state store.
- Unbounded chaos or soak tests in ordinary developer runs.
- Requiring native epoll for correctness.

## 4. Future Version Notes

Future versions are not urgent until V4.0 is complete and validated. They should start with design and implementation documents before code work.

Recommended future topics:

1. Optional experimental LSM state-store module isolated from the broker default write path.
2. Transactional produce and exactly-once research.
3. Larger benchmark automation and long-running soak tests.
4. Multi-tenant authentication and ACL integration beyond local scoped tokens.
5. More complete client ergonomics such as batching, pause/resume, and auto-commit options.

## 5. Recommended Next Tasks

1. Review `docs/designs/v4-operator-readiness-design.md`.
2. Execute `docs/plans/v4-operator-readiness-implementation.md` task by task.
3. Keep resource use bounded and close all brokers, clients, event loops, executors, and Docker resources started by tests or demos.
4. Record unresolved blockers after five bounded attempts instead of repeatedly retrying the same blocked path.
5. Synchronize `README.md`, `docs/README.md`, `docs/design.md`, `docs/architecture.md`, `docs/testing.md`, and test reports from the actual implementation.

## 6. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Docker/Testcontainers unavailable | Real-etcd Failsafe verification cannot run locally. | Check Docker first; record environment blocker after bounded attempts; keep deterministic local tests passing. |
| V4.0 scope grows too broad | Resource use and review complexity increase. | Keep tasks ordered; finish operations/status/lag before optional delayed messages. |
| Operator APIs leak unbounded data | Large clusters can create large responses. | Use explicit limits, counts, and pagination-style bounded lists. |
| Topic deletion is unsafe | Data or metadata may be removed unexpectedly. | Require scoped permission, explicit CLI command, tests, and clear documentation. |
| Metrics become inconsistent | Operators may misread system state. | Define snapshot semantics and verify with deterministic tests. |

## 7. Definition Of Done

A V4.0 feature is done only when:

1. Source code respects module boundaries.
2. Protocol changes include serdes round-trip and malformed-input tests.
3. Broker behavior includes focused unit or embedded-channel tests.
4. CLI behavior includes command parsing and output tests.
5. Documentation and examples are synchronized.
6. Resource cleanup is verified for tests and demos that start brokers, clients, executors, event loops, or Docker resources.
7. Verification commands and exit codes are recorded in a dated test report.

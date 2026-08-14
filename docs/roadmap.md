# MiniMQ Roadmap

This roadmap summarizes the completed baseline, the current V4 Operator Readiness implementation, and deferred future work.

本路线图说明当前完成状态、V4 Operator Readiness 实现快照，以及后续延后事项。

## 1. Current State

MiniMQ 5.0.0 is open. V4 Operator Readiness is the completed baseline; V5 adds producer acknowledgement policies on top of existing ISR replication.

Current implemented capabilities include:

- Java 17 Maven multi-module project with `mq-common`, `mq-storage`, `mq-protocol`, `mq-broker`, `mq-client`, and `mq-demo`.
- Custom Netty binary protocol and protocol serdes tests.
- WAL-backed segmented append-only log, sparse index, retention, compaction, checkpoints, recovery, zero-copy fetch, delayed-message scheduling, and visible-prefix fetch semantics.
- Producer and Consumer SDKs with metadata refresh and leader-aware retry behavior.
- Consumer group assignment, offset commit/fetch, and group coordination basics.
- Broker replication, ISR tracking, leader epoch fencing, automatic failover, broker leases, and frozen-partition handling.
- etcd-backed Controller election and metadata persistence for the HA path.
- Maintenance admin APIs, runtime metrics, deterministic resilience tests, and transport selection for NIO/auto/epoll.
- V4 operator-readiness APIs for ops status, group status, topic admin, failover audit, auth status, reliability links, metadata V2, metrics V2, and replica-state V2 reporting.
- Retry and dead-letter links are explicit topic metadata; delayed retry produce uses PRODUCE_V2 with deliverAtEpochMs.
- Scheduled delayed records are bounded by --max-scheduled-records with default 100000; setting it to 0 disables admission of new future delayed records.
- CLI and Web operator flows for status, group lag, Topic administration, audit, auth, runtime metrics, reliability routing, delayed produce, and the existing produce/consume validation.
- Bounded three-Broker start/stop/failover scripts with PID/start-time ownership, readiness deadlines, marker-protected recursive cleanup, and full-ISR failover preconditions.

## 2. Remaining Gate

The most important remaining validation gate is environment-dependent real-etcd Failsafe verification for Controller HA.

Current policy:

- Check Docker API access before running any `-Petcd-it` command.
- If Docker access is available, run the real-etcd Failsafe profile and record fresh results.
- If the same environment blocker remains unresolved after five bounded attempts, document it in a known-issues file and continue with independent work.
- Do not mark the real-etcd gate complete until the focused Failsafe tests run successfully.

## 3. Current Major Version: V5.0 Producer Acknowledgements

V5.0 adds `LEADER` and `ALL` (current-ISR) produce acknowledgements. Design and plan:

- [V5.0 Producer Acknowledgements Design](designs/v5-producer-acks-design.md)
- [V5.0 Producer Acknowledgements Implementation Plan](plans/v5-producer-acks-implementation.md)

Default produce remains Leader-local, matching V4. `ALL` waits until the current ISR high watermark meets the post-append Leader LEO, or returns `REQUEST_TIMED_OUT` / `NOT_ENOUGH_REPLICAS`. Transactions and exactly-once stay out of scope.

## 3a. Previous Major Version: V4.0 Operator Readiness

V4.0 is now the current consolidated operator-readiness implementation, not a documentation-only plan. It folds the previously separate release-gate, multi-node operations, consumer-lag, topic-admin, failover-audit, metrics, local-demo, scoped-permission, retry-topic, dead-letter-topic, and required delayed-message work into one major version.

Authoritative planning documents:

- [V4.0 Operator Readiness Design](designs/v4-operator-readiness-design.md)
- [V4.0 Operator Readiness Implementation Plan](plans/v4-operator-readiness-implementation.md)

### V4.0 Implemented Scope

| Area | Capability |
| --- | --- |
| Release gate | Keep focused HA/failover evidence and real-etcd verification policy tied to Docker availability. |
| Operations status | OPS_STATUS reports broker state, controller role, leader readiness, ISR, metadata version, runtime metrics, maintenance state, and frozen partitions through bounded responses. |
| Consumer lag | GROUP_STATUS reports group members, assignment, committed offsets, high watermarks, lag, and delayed visibility timestamps. |
| Topic admin | List, describe, delete, increase-partitions, admin create-topic, and delete acknowledgement APIs are present. |
| Failover audit | Bounded audit events cover controller takeover, lease expiry, self-fencing, leader transition, ISR, stale epoch, and leader readiness events. |
| Metrics | RUNTIME_METRICS_V2 adds bounded throughput, bytes in/out, latency, throttling, and lag snapshots without replacing key 19 compatibility. |
| Permissions | Admin-token compatibility is preserved while scoped operator credentials authorize V4 operator APIs. |
| Reliability extensions | Retry-topic and dead-letter-topic links are explicit metadata; delayed messages are implemented through PRODUCE_V2, WAL type 8, visible-prefix reads, and bounded Consumer waits. |
| Compatibility | V3 clients remain compatible with unchanged key 0-19 layouts; mixed V3/V4 Broker clusters are unsupported. |

Topic deletion is a durable `DELETING -> DELETED` workflow. Deleted names remain reserved and cannot
be recreated. Retry/DLQ targets are operator-selected names rather than inferred naming conventions.

### V4.0 Non-Goals

- Transactions and exactly-once semantics.
- Replacing the default segmented-log write path with an LSM state store.
- Unbounded chaos or soak tests in ordinary developer runs.
- Requiring native epoll for correctness.

## 4. Future Version Notes

After V5.0 acks land, later versions should start with design documents before code work.

Recommended later topics:

1. Configurable `min.insync.replicas`.
2. Consumer-group leave and configurable session timeout.
3. Optional unclean election (default off).
4. Transactional produce and exactly-once research.
5. Optional experimental LSM state-store module isolated from the broker default write path.
6. Multi-tenant authentication and ACL integration beyond local scoped tokens.

## 5. Recommended Next Tasks

1. Finish V5 produce-ack verification (`mvn clean verify`) and record a dated 5.0.0 test report.
2. Move `acks=ALL` waits off the Netty event loop onto a bounded dedicated executor.
3. Keep resource use bounded and close all brokers, clients, event loops, executors, and Docker resources started by tests or demos.
4. Record unresolved Docker/real-etcd blockers after five bounded attempts instead of repeatedly retrying the same blocked path.
5. Keep README.md, docs/README.md, docs/design.md, docs/architecture.md, docs/testing.md, and test reports synchronized from source and tests.

## 6. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Docker/Testcontainers unavailable | Real-etcd Failsafe verification cannot run locally. | Check Docker first; record environment blocker after bounded attempts; keep deterministic local tests passing. |
| V4.0 implementation/docs drift | Operators may read old planning language as current behavior. | Treat source and tests as authoritative; update current-state docs when V4 behavior changes. |
| Operator APIs leak unbounded data | Large clusters can create large responses. | Use explicit limits, counts, and pagination-style bounded lists. |
| Topic deletion is unsafe | Data or metadata may be removed unexpectedly. | Require scoped permission, explicit CLI command, tests, and clear documentation. |
| Metrics become inconsistent | Operators may misread system state. | Define snapshot semantics and verify with deterministic tests. |

## 7. Definition Of Done

A V5.0 feature is done only when:

1. Source code respects module boundaries.
2. Protocol changes include serdes round-trip and malformed-input tests.
3. Broker behavior includes focused unit or embedded-channel tests.
4. CLI behavior includes command parsing and output tests.
5. Documentation and examples are synchronized.
6. Resource cleanup is verified for tests and demos that start brokers, clients, executors, event loops, or Docker resources.
7. Verification commands and exit codes are recorded in a dated test report.

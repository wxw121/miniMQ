# V4.0 Operator Readiness Design

Date: 2026-07-21
Status: Design for the next major version after the V3.3-V3.5 production-hardening snapshot.

## Goal

V4.0 consolidates the previously separate release-gate, multi-node operations, consumer-lag, failover-observability, admin-completeness, local-demo, and application-reliability ideas into one major version. The goal is to make MiniMQ easier to operate, diagnose, and demonstrate as a compact distributed message-queue system without replacing the existing append-only segmented-log architecture.

## Scope

1. **Release gate closure**: rerun focused V3.0.0/V3.0.1 failover evidence and attempt the real-etcd Failsafe gate when Docker API access is available.
2. **Broker and cluster operations status**: add authenticated read-only APIs and CLI output for availability, controller role, broker epoch, metadata version, transport mode, runtime metrics, maintenance state, leader readiness, ISR, and frozen partitions.
3. **Consumer group status and lag**: expose group membership, partition assignment, committed offsets, high watermarks, and lag per topic-partition.
4. **Topic administration completeness**: add list, describe, delete, and increase-partitions flows while preserving create-topic compatibility.
5. **Failover audit stream**: record controller takeover, broker lease expiry, self-fencing, leader transition, ISR shrink/expand, stale-epoch rejection, and leader-readiness transitions.
6. **Metrics and throughput visibility**: report produce/fetch request rates, bytes in/out, throttled requests, latency summaries, and per-partition lag in bounded snapshots.
7. **Local multi-node demonstration**: provide a bounded local three-broker harness and a repeatable demo flow for produce, consume, leader failure, failover, and resumed client traffic.
8. **Scoped operator permissions**: evolve the single admin token into scoped local permissions for read-only status, topic administration, maintenance, and broker-offline operations.
9. **Application reliability extensions**: add retry-topic and dead-letter-topic support first; delayed-message support can be included in V4.0 only after the operational surfaces and group lag are complete.

## Non-Goals

- Do not implement transactions or exactly-once semantics in V4.0.
- Do not replace the default broker write path with an LSM state store.
- Do not add unbounded chaos testing to normal developer runs.
- Do not make native epoll required for correctness; NIO remains the safe default.

## Operator API Direction

New admin APIs should be appended after the existing runtime-metrics API. Each API must use bounded lists, explicit counts, and defensive serdes validation.

| Area | API Intent | CLI Command |
| --- | --- | --- |
| Cluster status | Broker/controller/runtime/metadata/partition readiness snapshot | `ops-status` |
| Consumer group status | Members, assignment, offsets, high watermarks, lag | `group-status` |
| Topic admin | List, describe, delete, increase partitions | `list-topics`, `describe-topic`, `delete-topic`, `increase-partitions` |
| Failover audit | Recent bounded transition events | `failover-audit` |
| Permission check | Show effective scoped operator permissions | `auth-status` |

## Data Model Notes

- Lag is computed as `replication-aware high watermark - committed offset` for a group and topic-partition.
- Failover audit entries are best-effort operational evidence, not a durable consensus log.
- Retry and dead-letter topics should use normal MiniMQ topics with explicit naming conventions and metadata links, not a hidden storage path.
- Delayed messages, if included, should be persisted as ordinary records plus scheduling metadata so restart behavior is testable.

## Validation Strategy

V4.0 should be accepted only when focused protocol, broker, client, storage, and demo tests pass for each feature group. Full `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml` should be run before release when local resources allow it. Docker/Testcontainers work remains gated: check Docker access first, run real-etcd tests only when accessible, and record an environment-blocked result after five bounded attempts if it still cannot run.

## Resource-Control Rules

- Use bounded startup waits for every local multi-node test.
- Close brokers, clients, Netty event loops, executors, and Docker resources started by the test or demo.
- Keep benchmark and failover validation opt-in rather than part of the ordinary fast test loop.
- Prefer deterministic embedded-channel tests before real-network tests.

## Future Version Notes

After V4.0 is complete, future versions can be planned as design-first work:

1. Optional experimental LSM state-store module isolated from the broker default write path.
2. Transactional produce and exactly-once research.
3. Larger-scale benchmark automation and longer soak testing.
4. Multi-tenant authentication and ACL integration beyond local scoped tokens.

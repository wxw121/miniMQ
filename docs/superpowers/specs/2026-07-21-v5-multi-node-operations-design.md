# V5 Multi-Node Operations Design

Date: 2026-07-21
Status: Design only; no V5 code is implemented in the V3.0.0 worktree.

## Goal

V5 should turn the current HA learning implementation into a more operator-friendly multi-node system without replacing the existing append-only message log or introducing a new default storage engine.

## Recommended Scope

1. Operational readiness endpoint: add an authenticated broker status view that combines availability state, controller role, broker epoch, metadata version, transport mode, runtime metrics, and maintenance state.
2. Multi-node validation harness: define a repeatable non-Docker local harness first, then a Docker/Testcontainers gate only when Docker API access is available.
3. Failover observability: expose controller takeover events, leader readiness transitions, and rejected stale-epoch requests in a compact audit stream or metrics snapshot.
4. Safe operator actions: keep destructive maintenance and broker-offline operations token-protected, single-target by default, and explicit about dry-run versus mutation.
5. Release evidence: require focused protocol, broker, client, and demo tests plus an environment-specific real-etcd report before calling V5 complete.

## Non-Goals

- Do not replace the message log with LSM storage; V4 LSM remains optional/experimental documentation.
- Do not add unbounded chaos testing to ordinary developer runs.
- Do not require native epoll for correctness; NIO remains the safe default.

## Compatibility

V5 may add new admin APIs after `RUNTIME_METRICS = 19`, but it should not change existing request/response wire layouts. New APIs should follow the bounded-list serdes pattern used by V3.3.
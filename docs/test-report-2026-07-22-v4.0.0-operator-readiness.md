# V4.0.0 Operator Readiness Test Report

Date: 2026-07-22
Verification refreshed: 2026-07-29
Version: 4.0.0
Branch: `dev/v3.0.0-control-plane-ha`

## Scope

This report closes the local V4.0.0 operator-readiness gate after the code-review fixes. It covers protocol and metadata compatibility, topic deletion tombstones, shared request deadlines, client response correlation and cleanup, delayed delivery, operator APIs and CLI/Web behavior, storage recovery, and the per-module JaCoCo gate.

## Full Reactor Verification

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml clean verify
```

Result: passed, exit code 0. The fresh run completed on 2026-07-29 in 1,455.3 seconds.

| Module | Suites | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-common` | 4 | 15 | 0 | 0 | 0 |
| `mq-protocol` | 2 | 52 | 0 | 0 | 0 |
| `mq-storage` | 27 | 163 | 0 | 0 | 0 |
| `mq-broker` | 45 | 321 | 0 | 0 | 0 |
| `mq-client` | 12 | 88 | 0 | 0 | 0 |
| `mq-demo` | 8 | 73 | 0 | 0 | 0 |
| **Total** | **98** | **712** | **0** | **0** | **0** |

## Coverage

| Module | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-common` | 97.92% | 92.96% | 98.40% (`185/188`) | 100.00% | 100.00% |
| `mq-protocol` | 94.97% | 81.70% | 94.92% (`1680/1770`) | 94.27% | 100.00% |
| `mq-storage` | 90.87% | 81.54% | 90.04% (`2287/2540`) | 97.29% | 98.00% |
| `mq-broker` | 90.92% | 76.81% | 90.09% (`5119/5682`) | 94.08% | 98.97% |
| `mq-client` | 90.77% | 74.86% | 90.36% (`778/861`) | 94.12% | 100.00% |
| `mq-demo` | 94.93% | 77.30% | 92.86% (`950/1023`) | 93.85% | 100.00% |
| **Overall** | **92.01%** (`50235/54598`) | **78.72%** (`4061/5159`) | **91.17%** (`10999/12064`) | **94.84%** (`1819/1918`) | **99.34%** (`303/305`) |

Every module passed the configured 90% line-coverage check.

## Code-Review Regression Evidence

- Legacy metadata V2 now round-trips and installs bounded topic tombstones before follower activation.
- Controller and broker network clients correlate pending responses by request id, discard late responses, and release retained buffers during timeout, abort, and close paths.
- Consumer polling rejects negative timeouts, and metadata refresh, repository IO, retries, topic deletion, and broker startup use bounded shared deadlines.
- Topic deletion preserves storage identity across restart, maps uncertain async failures to `CONTROLLER_NOT_READY`, and keeps deleted names reserved.
- Topic deletion serializes new partition opens with deletion, cancels completed waiter deadlines, and skips already-closed segment/index resources during checkpoint flush and shutdown.
- Legacy followers page active topics and tombstones independently beyond 10,000 entries, with cursor SerDes and broker-handler regression coverage.
- Web AUTO mode only falls back to mock before a real external side effect; failures after topic creation or produce are surfaced.
- Regression tests cover compatibility metrics, circuit-breaker transitions, candidate cleanup, invalidation cleanup, delayed produce, stale Controller epochs, metadata routing failures, and coverage-gate stability.

## Environment Gate

The real-etcd Failsafe profile was not run because Docker was unavailable. Five bounded `docker info --format '{{.ServerVersion}}'` checks were made on 2026-07-29; all returned exit code 1. The Docker config file was inaccessible and `//./pipe/docker_engine` did not exist.

Status: local full-reactor gate passed; real-etcd gate remains environment-blocked. Do not mark `ControllerFailoverIT`, `EtcdControllerCoordinationStoreIT`, `LeaderEpochFailoverIT`, or `EtcdClusterMetadataRepositoryIT` as passed until Docker access is restored and fresh Failsafe reports exist.

Next command when Docker is available:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

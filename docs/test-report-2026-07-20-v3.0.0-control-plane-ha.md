# MiniMQ 3.0.0 Control Plane HA Test Report

Date: 2026-07-20
Branch: `dev/v3.0.0-control-plane-ha`

## Result

The full reactor verification passed, including all configured JaCoCo gates. Final
Controller HA sign-off is **blocked**: the real-etcd Failsafe tests did not run because
the profile stopped in Surefire after one non-deterministic broker failover assertion,
and the local Docker API was not accessible to Testcontainers.

| Suite | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| Full reactor Surefire unit and integration tests | 376 | 0 | 0 | 0 |
| `-Petcd-it` Surefire attempt | 163 | 1 | 0 | 0 |
| Failsafe real-etcd integration tests | not run | - | - | - |

The real-etcd suite contains `ControllerFailoverIT`,
`EtcdControllerCoordinationStoreIT`, `LeaderEpochFailoverIT`, and
`EtcdClusterMetadataRepositoryIT`. It was not reached in this environment. Docker
Desktop processes were present, but the sandbox could not access
`npipe:////./pipe/docker_engine`; no standalone etcd or Podman runtime was installed.

## Commands

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result: exit code 0 (`BUILD SUCCESS`), completed in 1,016 seconds. The reactor reported
376 tests, 0 failures, 0 errors, and 0 skipped.

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

Result: exit code 1 before Failsafe. Surefire reported 163 broker tests with one
failure in `ReplicationIntegrationTest.clientFetchAfterFailoverWithoutWaitingForFollowerFetch`
(`expected: NONE but was: INVALID_REQUEST`). The same method passed when isolated and
in a full-class rerun; five consecutive repeated executions also passed during the
initial investigation. A later V3.0.1 focused rerun reproduced the same failure, and
the working tree now contains a deterministic test synchronization fix that waits for
the elected broker's local Leader view plus Controller `leaderReady=true` metadata
before issuing client FETCH.

V3.0.1 focused verification:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Dtest=ReplicationIntegrationTest#clientFetchAfterFailoverWithoutWaitingForFollowerFetch' '-Dsurefire.failIfNoSpecifiedTests=false' '-Djacoco.skip=true' test
```

Result: exit code 0.

## JaCoCo line coverage

| Module | Covered / total lines | Coverage |
| --- | ---: | ---: |
| `mq-common` | 145 / 148 | 97.97% |
| `mq-storage` | 1,782 / 1,975 | 90.23% |
| `mq-protocol` | 642 / 701 | 91.58% |
| `mq-broker` | 2,515 / 2,790 | 90.14% |
| `mq-client` | 380 / 422 | 90.05% |
| `mq-demo` | 476 / 506 | 94.07% |

Every code module meets the required 90% line-coverage gate.

## Regression evidence

- Controller leadership loss now cleans the old lease and re-enters election instead
  of permanently leaving the Broker unable to become Controller again.
- A Leader candidate that misses `leaderActivationTimeoutMs` is frozen without an
  automatic second-candidate or unclean election.
- A keepalive failure no longer prevents bounded election resign and lease revoke.
- Controller and metadata etcd adapters cover successful CAS, conflicting CAS,
  invalid payload, initialization, close, and retry behavior with controlled tests.
- Client bootstrap validation covers malformed endpoints, all-bootstrap failure,
  Controller discovery through a follower, and server error propagation.
- Crash-safe truncation covers reopen, continued append, range validation, no-op, and
  closed-log behavior. Shutdown tolerates a segment already force-closed by a completed
  replacement operation while normal checkpoints continue to reject closed segments.

Expected connection-refused and lease-scan warnings are emitted by fault-injection
tests while peers are deliberately stopped. They do not correspond to test failures.

## Release gate

Do not mark the real-etcd Controller HA gate complete yet. The broker failover timing
issue has a V3.0.1 focused fix and focused passing evidence, but the Failsafe profile
still needs Docker engine access so the real-etcd integration tests can run and be
recorded.

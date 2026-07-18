# V2.3.2 Replica Fetch Connection Pooling Test Report

Date: 2026-07-18

Branch: `dev/v2.3.2-replica-fetch-pooling`

Scope: R3-P1-05 â€?reuse replica-fetch TCP connections and Netty event loops per leader broker.

## Changes Validated

1. `LeaderReplicaClient` can use a caller-owned shared `EventLoopGroup` without shutting it down on `close()`.
2. `ReplicaConnectionPool` caches one connected `LeaderReplicaClient` per `(host, port)` leader key.
3. `ReplicaConnectionPool.invalidate(...)` closes and recreates failed leader clients.
4. `ReplicaFetcher` reuses pooled leader clients across partitions and closes the pool on shutdown.
5. `ReplicaFetcher` invalidates pooled clients on `NOT_LEADER`, timeouts, connection failures, and runtime failures.

## TDD Evidence

| Cycle | RED evidence | GREEN evidence |
| --- | --- | --- |
| Shared event loop | `LeaderReplicaClientTest` failed to compile because `LeaderReplicaClient(NioEventLoopGroup, boolean)` did not exist. | `LeaderReplicaClientTest`: 3 tests, 0 failures, `BUILD SUCCESS`. |
| Connection pool | `ReplicaConnectionPoolTest` failed to compile because `ReplicaConnectionPool` did not exist. | `ReplicaConnectionPoolTest`: 2 tests, 0 failures, `BUILD SUCCESS`. |
| Fetcher pooling | `ReplicaFetcherTest.reusesLeaderConnectionAcrossFetchLoops` failed with `expected: <1> but was: <2>`. | `ReplicaFetcherTest`: 2 tests, 0 failures, `BUILD SUCCESS`. |

## Focused Regression

Command:

```powershell
mvn -pl mq-broker -am "-Dtest=ReplicaFetcherTest,LeaderReplicaClientTest,ReplicaConnectionPoolTest,ReplicationIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false" test -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result:

- `BUILD SUCCESS`
- `Tests run: 12, Failures: 0, Errors: 0, Skipped: 0`
- `MiniMQ Broker`: `SUCCESS [03:04 min]`
- Note: `ReplicationIntegrationTest` still emits expected WARN logs while exercising connection-refused and timeout paths.

## Full Verification

Command:

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result:

- `BUILD SUCCESS`
- `MiniMQ Common`: `SUCCESS`, JaCoCo coverage checks met
- `MiniMQ Storage`: `SUCCESS`, JaCoCo coverage checks met
- `MiniMQ Protocol`: `SUCCESS`, JaCoCo coverage checks met
- `MiniMQ Broker`: `SUCCESS [05:01 min]`, `Tests run: 86, Failures: 0, Errors: 0, Skipped: 0`, JaCoCo coverage checks met
- `MiniMQ Client`: `SUCCESS [02:24 min]`, `Tests run: 24, Failures: 0, Errors: 0, Skipped: 0`, JaCoCo coverage checks met
- `MiniMQ Demo`: `SUCCESS [04:34 min]`, `Tests run: 26, Failures: 0, Errors: 0, Skipped: 0`, JaCoCo coverage checks met
- Total time: `12:14 min`

## Release Notes

- R3-P1-05 is complete in `2.3.2`.
- No public protocol or CLI changes were introduced.
- The next roadmap item remains operator-facing maintenance API / CLI reporting for maintenance metrics.

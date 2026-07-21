# V3.0.1-V3.2 Productionization Test Report

Date: 2026-07-21
Branch: `dev/v3.0.0-control-plane-ha`
Version: `3.2.0-SNAPSHOT`

## Summary

This report closes the resource-controlled verification pass for the combined V3.0.1, V3.1, and V3.2 working-tree release. It intentionally does not claim the real-etcd Failsafe gate as complete because Docker API access is blocked in the current environment.

| Area | Command | Result | Notes |
| --- | --- | --- | --- |
| V3.0.1 failover stabilization | `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Dtest=ReplicationIntegrationTest#clientFetchAfterFailoverWithoutWaitingForFollowerFetch' '-Dsurefire.failIfNoSpecifiedTests=false' '-Djacoco.skip=true' test` | Exit 0 | Output included expected connection-refused warnings while fault-injection stopped/offlined brokers. |
| V3.1/V3.2 focused suite | `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Dtest=BrokerRuntimeMetricsTest,BrokerRuntimeMetricsIntegrationTest,CircuitBreakerTest,RequestThrottlerTest,BrokerAvailabilityGuardTest,ReplicaLeoReporterTest,NettyTransportFactoryTest,BrokerConfigTest,BrokerShutdownTest' '-Dsurefire.failIfNoSpecifiedTests=false' '-Djacoco.skip=true' test` | Exit 0 | Covers runtime metrics, throttling, circuit breaker, degraded mode, transport mode parsing/factory, config, and shutdown smoke coverage. |
| Bounded local broker regression | `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Djacoco.skip=true' test` | Exit 0 | Non-Docker broker module reactor test. Output included expected connection-refused warnings during broker shutdown/fault windows. |
| Docker API availability | `docker version` | Exit 1 | Docker client prints version, but config access and API pipe access are denied. |
| Real-etcd Failsafe gate | `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify` | Not run | Environment-gated; run only when Docker API access is available without reboot-risk actions. |

## Delivered Behavior

### V3.0.1 Stabilization

`ReplicationIntegrationTest#clientFetchAfterFailoverWithoutWaitingForFollowerFetch` now waits for the promoted broker's local leader view and Controller metadata with `leaderReady=true` before issuing the client `FETCH`. The fix keeps the scenario deterministic while preserving the original assertion: client reads should not require an additional follower fetch after failover.

### V3.1 Operational Resilience

The working tree includes:

- `BrokerRuntimeMetrics` and `BrokerRuntimeMetricsSnapshot` for request counts, response counts, latency totals/counts, averages, and throttled request counts.
- `RequestThrottler`, `ThrottleResult`, `ErrorCode.THROTTLED`, and `--max-in-flight-requests`, defaulting to disabled with value `0`.
- `CircuitBreaker` integration in `ReplicaLeoReporter`, returning `CONTROLLER_NOT_READY` while the Controller update path is open-circuited.
- `BrokerAvailabilityGuard.State.STORAGE_DEGRADED`, allowing fetch/metadata/discovery/diagnostic paths while rejecting produce and destructive maintenance with `MAINTENANCE_BUSY`.

### V3.2 Network Transport

The broker now parses `--network-transport nio|auto|epoll` and constructs event-loop/channel classes through `NettyTransportFactory`. NIO remains the default. AUTO falls back to NIO when epoll is unavailable. Explicit EPOLL fails clearly when native support is unavailable.

### V4 Documentation Only

V4 code was not implemented. The design and implementation documents were created here:

- `docs/superpowers/specs/2026-07-20-v4-experimental-lsm-state-store-design.md`
- `docs/superpowers/plans/2026-07-20-v4-experimental-lsm-state-store-implementation.md`

Both documents keep the experimental LSM state store outside the default broker message-log write path.

## Environment-Gated Item

Latest Docker check:

```text
WARNING: Error loading config file: open C:\Users\Wxw\.docker\config.json: Access is denied.
Client:
 Version:           29.5.3
 API version:       1.54
 OS/Arch:           windows/amd64
 Context:           default
permission denied while trying to connect to the docker API at npipe:////./pipe/docker_engine
```

Pending command when Docker API access is available:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

Do not mark the real-etcd Controller HA gate complete until `ControllerFailoverIT`, `EtcdControllerCoordinationStoreIT`, `LeaderEpochFailoverIT`, and `EtcdClusterMetadataRepositoryIT` run successfully and their fresh results are recorded.

## Closeout Notes

The local verification pass avoided Docker/Testcontainers and did not perform reboot-risk actions. The only unresolved item is the environment-gated real-etcd Failsafe run, which remains tracked in `docs/known-issues-2026-07-20-v3.0.1-v3.2.md`.

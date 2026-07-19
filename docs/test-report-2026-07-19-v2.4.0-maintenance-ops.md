# V2.4.0 Maintenance Operations Test Report

Date: 2026-07-19  
Version: `2.4.0`  
Scope: maintenance admin protocol and operator-facing CLI

## Summary

V2.4.0 exposes the existing storage maintenance boundary through broker admin APIs and Demo CLI commands:

1. `MAINTENANCE_REPORT` / API 13 previews local retention and compaction actions.
2. `MAINTENANCE_RUN` / API 14 executes local storage maintenance immediately.
3. `MAINTENANCE_METRICS` / API 15 returns cumulative local maintenance counters.
4. All three APIs require the shared Broker `--admin-token`; invalid credentials return `UNAUTHORIZED`.
5. A broker-scoped single-thread executor runs report/run work outside Netty EventLoops; overlapping destructive runs return `MAINTENANCE_BUSY`.
6. `maintenance-report`, `maintenance-run`, and `maintenance-metrics` require one explicit `--broker` target and render results from that Broker.

Operator contract:

```text
broker ... --admin-token <secret>
maintenance-report --broker 127.0.0.1:9092 --admin-token <secret>
maintenance-run --broker 127.0.0.1:9092 --admin-token <secret>
maintenance-metrics --broker 127.0.0.1:9092 --admin-token <secret>
```

## Verification

Focused verification:

```powershell
mvn -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-protocol,mq-broker,mq-client,mq-demo -am "-Dtest=ProtocolSerdesTest,BrokerConfigTest,MaintenanceAdminServiceTest,EmbeddedBrokerChannelTest,BrokerShutdownTest,NetworkClientTimeoutTest,DemoCliTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Result: `BUILD SUCCESS` in 02:21 min, 56 tests, 0 failures/errors/skips, finished at 2026-07-19T15:51:30+08:00.

Full verification:

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml "-Dlogback.configurationFile=D:/software/cld_project/mq/.worktrees/v2.4.0-maintenance-hardening/.logback-off.xml"
```

Result: `BUILD SUCCESS` in 13:53 min, finished at 2026-07-19T16:06:22+08:00.

## Test Totals

| Metric | Result |
| --- | ---: |
| Tests run | 292 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |
| Overall line coverage | 91.76% |
| Overall instruction coverage | 92.98% |
| Overall branch coverage | 80.62% |
| Overall method coverage | 97.02% |
| Overall class coverage | 100.00% |

## Coverage Added

| Test | Covers |
| --- | --- |
| `ProtocolSerdesTest` | Token-bearing admin API round trips and stable `UNAUTHORIZED` / `MAINTENANCE_BUSY` responses. |
| `MaintenanceAdminServiceTest` | Constant-time authorization, single-run admission, dedicated executor threading, failure recovery, and shutdown. |
| `EmbeddedBrokerChannelTest` | Async handler mapping, correlation IDs, authorization errors, busy admission, and enabled segment actions. |
| `BrokerShutdownTest` | Maintenance executor and port resources are released across broker restart. |
| `NetworkClientTimeoutTest` | Readable API names for maintenance keys 13-15. |
| `DemoCliTest` | Single-target/token validation plus real TCP enabled retention and compaction segment output. |

## Notes

- The full Maven run used a root `.logback-off.xml` file to suppress expected replication failover WARN noise while still running the same full `clean verify` lifecycle and JaCoCo gates.
- Maven shade emitted existing dependency overlap warnings while building `mq-demo-2.4.0.jar`; these did not fail the build.

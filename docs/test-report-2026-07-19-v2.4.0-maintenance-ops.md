# V2.4.0 Maintenance Operations Test Report

Date: 2026-07-19  
Version: `2.4.0`  
Scope: maintenance admin protocol and operator-facing CLI

## Summary

V2.4.0 exposes the existing storage maintenance boundary through broker admin APIs and Demo CLI commands:

1. `MAINTENANCE_REPORT` / API 13 previews local retention and compaction actions.
2. `MAINTENANCE_RUN` / API 14 executes local storage maintenance immediately.
3. `MAINTENANCE_METRICS` / API 15 returns cumulative local maintenance counters.
4. `maintenance-report`, `maintenance-run`, and `maintenance-metrics` render those results from a running broker.

## Verification

Focused verification:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-protocol,mq-broker,mq-demo -am "-Dtest=ProtocolSerdesTest,EmbeddedBrokerChannelTest,DemoCliTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Result: exit code 0.

Full verification:

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result: `BUILD SUCCESS` in 12:44 min, finished at 2026-07-19T11:13:27+08:00.

## Test Totals

| Metric | Result |
| --- | ---: |
| Tests run | 279 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |
| Overall line coverage | 91.60% |
| Overall instruction coverage | 92.75% |
| Overall branch coverage | 80.64% |
| Overall method coverage | 97.19% |
| Overall class coverage | 100.00% |

## Coverage Added

| Test | Covers |
| --- | --- |
| `ProtocolSerdesTest` | Admin API request/response round trips for maintenance report, run, and metrics. |
| `EmbeddedBrokerChannelTest` | Broker handler mapping from admin API keys to `PartitionManager` maintenance entrypoints. |
| `DemoCliTest` | Real TCP CLI output for `maintenance-report`, `maintenance-run`, and `maintenance-metrics`. |

## Notes

- The full Maven run produced expected replication failover warnings while tests intentionally stopped brokers; the reactor still completed successfully.
- Maven shade emitted existing dependency overlap warnings while building `mq-demo-2.4.0.jar`; these did not fail the build.

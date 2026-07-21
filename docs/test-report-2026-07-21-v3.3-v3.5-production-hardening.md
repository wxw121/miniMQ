# V3.3-V3.5 Production Hardening Test Report

Date: 2026-07-21
Version: 3.0.0
Scope: V3.3 runtime metrics export, V3.4 resilience matrix, V3.5 transport-aware fetch benchmark.

## Summary

V3.3-V3.5 is implemented as one bounded production-hardening version. The release adds authenticated runtime metrics over protocol API 19 and the `runtime-metrics` CLI command, deterministic resilience coverage for storage degradation and request throttling, and `benchmark-fetch --network-transport nio|auto|epoll`.

## Verification Evidence

| Area | Command | Result |
| --- | --- | --- |
| Protocol serde | `mvn -q -pl mq-protocol -am '-Djacoco.skip=true' '-Dtest=ProtocolSerdesTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` | Passed, exit 0 |
| Broker runtime metrics + resilience | `mvn -q -pl mq-broker -am '-Djacoco.skip=true' '-Dtest=EmbeddedBrokerChannelTest,RequestThrottlerTest,BrokerRuntimeMetricsIntegrationTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` | Passed, exit 0 |
| Demo CLI + fetch benchmark | `mvn -q -pl mq-demo -am '-Djacoco.skip=true' '-Dtest=DemoCliTest,FetchBenchmarkTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` | Passed, exit 0 |

## Red-Green Notes

- V3.3 protocol red test failed first because `ApiKeys.RUNTIME_METRICS`, `RuntimeMetricsRequest`, and `RuntimeMetricsResponse` did not exist.
- V3.3 broker/CLI red tests failed first because `RUNTIME_METRICS` routed to `INVALID_REQUEST` and `runtime-metrics` was not in usage/dispatch.
- V3.5 benchmark red test failed first because `FetchBenchmark.Config` did not expose `networkTransportMode()` and the constructor lacked the transport field.
- V3.4 was primarily regression hardening; added matrix tests passed against the V3.1/V3.3 implementation, so no broad production fault-injection framework was added.

## Environment Gates

- Real-etcd Failsafe verification remains environment-gated until Docker API access is available. Do not run `-Petcd-it` on this machine until Docker access is fixed.
- No Docker/Testcontainers command was run for this version.

## Files Of Interest

- `mq-protocol/src/main/java/com/minimq/protocol/ApiKeys.java`
- `mq-protocol/src/main/java/com/minimq/protocol/message/ProtocolMessages.java`
- `mq-protocol/src/main/java/com/minimq/protocol/ProtocolSerdes.java`
- `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`
- `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`
- `mq-demo/src/main/java/com/minimq/demo/FetchBenchmark.java`
## Final Verification

| Area | Command | Result |
| --- | --- | --- |
| Focused combined V3.3-V3.5 suite | `mvn -q -pl mq-protocol,mq-broker,mq-demo -am '-Djacoco.skip=true' '-Dtest=ProtocolSerdesTest,EmbeddedBrokerChannelTest,RequestThrottlerTest,BrokerRuntimeMetricsIntegrationTest,DemoCliTest,FetchBenchmarkTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` | Passed, exit 0 |

A broader `mvn test` was intentionally not run in this pass to keep local resource usage bounded. No Docker/Testcontainers gate was run.

## Release Handoff Blocker

The feature branch and commit `03340f8` were pushed to `origin/dev/v3.0.0-control-plane-ha`, but automatic pull-request creation could not be completed in this environment after five bounded attempts:

1. GitHub CLI (`gh`) is not installed.
2. The configured browser-control runtime failed during initialization.
3. The Git credential helper returned an HTTPS/API credential that GitHub rejected with HTTP 401.
4. No alternate installed GitHub client was available.
5. Git Credential Manager device-flow reauthentication did not expose a verification code or complete its callback and was terminated to avoid a lingering process.

Resume by restoring GitHub API authentication or installing/authenticating GitHub CLI, then create a non-draft PR from `dev/v3.0.0-control-plane-ha` to `main`. This is a release-process tooling limitation, not a product verification failure.

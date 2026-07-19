# V2.5.0 Broker Leases And Automatic Failover Test Report

Date: 2026-07-19
Version: `2.5.0`
Scope: Broker leases, epoch fencing, automatic clean failover, frozen partition recovery, and client metadata refresh

## Summary

V2.5.0 keeps one static Controller and closes the normal Broker-failure path:

1. `REGISTER_BROKER` returns a persisted, monotonically increasing `brokerEpoch`; API 16 `BROKER_HEARTBEAT` renews its lease.
2. A Broker self-fences before the Controller lease expires and rejects data, replication, and destructive maintenance work while unavailable.
3. The Controller removes an expired Broker from ISR and elects the highest-LEO online ISR replica, with replica order as the tie-breaker.
4. A partition without an online ISR is persisted as frozen and retains its original Leader ID.
5. The original Broker can recover the frozen partition by returning with the same data directory and a higher epoch.
6. Producer and Consumer invalidate a fenced Broker connection, refresh metadata once, and retry once after `BROKER_FENCED`.

Controller HA, data-plane Leader epochs, unclean election, and automatic failover log truncation remain out of scope.

## Focused Verification

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-protocol -am "-Dtest=ProtocolSerdesTest" "-Dsurefire.failIfNoSpecifiedTests=false" test

mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am "-Dtest=BrokerConfigTest,BrokerAvailabilityGuardTest,BrokerLeaseServiceTest,BrokerLeaseMonitorTest,ClusterControllerTest,MetadataStoreTest,MetadataSyncTest,EmbeddedBrokerChannelTest,ReplicaFetcherTest,ReplicaLeoReporterTest,ReplicationIntegrationTest,BrokerShutdownTest" "-Dsurefire.failIfNoSpecifiedTests=false" test

mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-client -am "-Dtest=ProducerNotLeaderRetryTest,ConsumerNotLeaderRetryTest,ProducerIntegrationTest,ConsumerIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

All three commands exited with code 0. They cover 117 focused tests: 19 protocol, 84 Broker, and 14 client tests.

The preflight deadline fix was verified separately with `BrokerLeaseServiceTest`: both new tests first failed with the old implementation (`200 ms` instead of the remaining `10 ms`, and no bounded close timeout), then all 6 lease service tests passed after connection, heartbeat, and close shared one self-fence budget.

## Full Verification

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result: `BUILD SUCCESS` in 14:09 min, finished at 2026-07-20T02:02:21+08:00. All six code modules passed their per-module JaCoCo line coverage gate.

## Test Totals

| Module | Tests | Failures | Errors | Skipped | Surefire time |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-common` | 10 | 0 | 0 | 0 | 0.067 s |
| `mq-storage` | 114 | 0 | 0 | 0 | 6.892 s |
| `mq-protocol` | 19 | 0 | 0 | 0 | 0.295 s |
| `mq-broker` | 122 | 0 | 0 | 0 | 302.134 s |
| `mq-client` | 28 | 0 | 0 | 0 | 182.159 s |
| `mq-demo` | 30 | 0 | 0 | 0 | 341.387 s |
| **Total** | **323** | **0** | **0** | **0** | **832.934 s** |

## Coverage

| Module | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-common` | 96.96% | 84.38% | 97.90% | 100.00% | 100.00% |
| `mq-storage` | 90.52% | 82.46% | 90.03% | 97.37% | 100.00% |
| `mq-protocol` | 93.66% | 83.24% | 92.29% | 94.00% | 100.00% |
| `mq-broker` | 93.80% | 80.02% | 92.11% | 98.67% | 100.00% |
| `mq-client` | 92.40% | 78.57% | 90.20% | 92.86% | 100.00% |
| `mq-demo` | 96.27% | 77.06% | 94.07% | 96.43% | 100.00% |
| **Overall** | **92.93%** | **81.04%** | **91.62%** | **96.96%** | **100.00%** |

## Key Regression Evidence

| Test | Verified behavior |
| --- | --- |
| `BrokerLeaseServiceTest` | Registration, epoch-confirming heartbeat, self-fence deadline, reconnect recovery, and bounded shutdown. |
| `BrokerLeaseServiceTest.reconnectAndHeartbeatShareRemainingSelfFenceBudget` | Controller reconnect and heartbeat response waits consume one shared deadline instead of resetting the timeout. |
| `BrokerLeaseServiceTest.failedHeartbeatBoundsClientCloseBySelfFenceDeadline` | Failed-client cleanup is bounded by the remaining self-fence budget and fencing is rechecked after cleanup. |
| `BrokerLeaseMonitorTest` | Lease expiry scanning, scan-failure isolation, deterministic scheduling, and cleanup. |
| `ClusterControllerTest` | Epoch monotonicity, stale epoch rejection, clean LEO-aware election, no-ISR freeze, and higher-epoch recovery. |
| `MetadataStoreTest` | Atomic metadata persistence and validation for Broker epochs and frozen partitions. |
| `EmbeddedBrokerChannelTest` | `BROKER_FENCED` admission behavior and protocol error mapping. |
| `ReplicaFetcherTest` / `ReplicaLeoReporterTest` | Fenced replicas stop opening control/data connections and resume only after activation. |
| `ProducerNotLeaderRetryTest` / `ConsumerNotLeaderRetryTest` | Fenced Broker connection invalidation, metadata refresh, one retry, and contextual errors. |
| `ReplicationIntegrationTest` | Real TCP Controller isolation proves old-Leader fencing before election, clean failover, no-ISR freeze, and same-directory higher-epoch recovery. |

## Notes

- `BrokerLeaseMonitorTest` deliberately injects one scan failure and logs `Broker lease scan failed`; the next scan is verified to run.
- Real TCP Controller isolation deliberately resets connections. Netty connection-reset ERROR and replica reconnect WARN logs are expected during these scenarios.
- Maven Shade emitted the existing module descriptor and overlapping resource warnings while building `mq-demo-2.5.0.jar`; the shaded jar and reactor build completed successfully.

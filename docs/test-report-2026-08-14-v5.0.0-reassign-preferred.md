# MiniMQ 5.0.0 `mvn clean verify` report (acks + min.insync + leave + reassign)

**Date:** 2026-08-14
**Command:** `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify`
**Result:** **BUILD SUCCESS** (2026-08-14 02:55:23 UTC)
**Elapsed:** 22 min 19 s
**Git SHA (at verify):** `1913bdb` (`feat: add topic config, partition reassignment, and preferred leader`)
**Quality gate:** per-module JaCoCo **line ≥ 90%** (`mq-parent/pom.xml`)
**This report does not lower that gate.**

This is the **expanded 5.0.0** run after folding topic config, partition reassignment, and preferred-leader election into V5. The previous same-day expanded pass (acks + min.insync + leave) remains in [`docs/test-report-2026-08-14-v5.0.0-acks-minisr-leave.md`](test-report-2026-08-14-v5.0.0-acks-minisr-leave.md). The acks-only slice remains in [`docs/test-report-2026-08-14-v5.0.0-producer-acks.md`](test-report-2026-08-14-v5.0.0-producer-acks.md).

## Result

| Check | Outcome |
| --- | --- |
| `mvn clean verify` | **BUILD SUCCESS** |
| Surefire | **785 tests, 0 failures, 6 skipped** |
| Failsafe | **0 tests run** (Docker-gated `EtcdClusterIT` still not executed) |
| JaCoCo `check` | **passed** all modules (`LINE` ≥ 90%) |
| JaCoCo line (all modules) | **91.52% (11823 / 12918)** |
| JaCoCo instruction (all modules) | **92.36% (53924 / 58383)** |
| JaCoCo branch (all modules) | **79.21% (4423 / 5584)** |
| JaCoCo method (all modules) | **95.14% (1917 / 2015)** |
| JaCoCo class (all modules) | **99.69% (320 / 321)** |

## Surefire by module

| Module | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| mq-common | 20 | 0 | 0 | 0 |
| mq-storage | 170 | 0 | 0 | 0 |
| mq-protocol | 62 | 0 | 0 | 0 |
| mq-broker | 359 | 0 | 0 | 0 |
| mq-client | 95 | 0 | 0 | 0 |
| mq-demo | 79 | 0 | 0 | 6 |
| **Total** | **785** | **0** | **0** | **6** |

Skipped tests are the same Linux-disabled Windows cases as the earlier V5 reports:

- `DemoCliTest` ×5 (`@DisabledOnOs(LINUX)` script/ownership cases)
- `WindowsNativeArgumentQuotingTest` ×1

Relative to the acks + min.insync + leave report (**781** tests): **+4** broker tests covering alter-config, reassignment, preferred-leader election, and rejection paths (frozen, offline, lagging replica, recovering).

## JaCoCo by module (`LINE` is the gate)

Parsed from each module's `target/site/jacoco/jacoco.xml` report-level counters after this verify (the same totals the HTML index shows). CSV per-class `LINE_*` sums can differ slightly because inner-class rows are attributed differently.

| Module | Instruction | Branch | Line | Method | Class |
| --- | --- | --- | --- | --- | --- |
| mq-common | 97.84% (1223/1250) | 92.90% (157/169) | **97.73% (215/220)** | 97.22% (35/36) | 100.00% (12/12) |
| mq-protocol | 94.75% (8545/9018) | 81.74% (461/564) | **94.73% (1889/1994)** | 94.10% (271/288) | 100.00% (118/118) |
| mq-storage | 90.93% (10583/11639) | 81.69% (1004/1229) | **90.10% (2338/2595)** | 97.57% (401/411) | 98.00% (49/50) |
| mq-broker | 91.57% (24859/27147) | 77.34% (2235/2890) | **90.75% (5534/6098)** | 94.74% (864/912) | 100.00% (101/101) |
| mq-client | 90.88% (3479/3828) | 76.23% (279/366) | **90.09% (818/908)** | 93.85% (168/179) | 100.00% (27/27) |
| mq-demo | 95.16% (5235/5501) | 78.42% (287/366) | **93.29% (1029/1103)** | 94.18% (178/189) | 100.00% (13/13) |
| **Overall** | **92.36% (53924/58383)** | **79.21% (4423/5584)** | **91.52% (11823/12918)** | **95.14% (1917/2015)** | **99.69% (320/321)** |

`mq-storage` class coverage is 49/50 because `PartitionLog` remains uncovered in the report. `BrokerMain` is excluded from the JaCoCo agent (`mq-broker/pom.xml`) and does not appear in the broker class totals.

Six modules are at or above the **line ≥ 90%** gate. The gate was not lowered. Branch coverage is still below 80% on broker (77.34%) and client (76.23%); the gate does not enforce branch.

## What this verify covered

- Previous V5 slice: `LEADER` / `ALL` produce acks, dedicated produce-ack executor, `min.insync.replicas`, `LEAVE_GROUP`, per-member session timeout
- `ALTER_TOPIC_CONFIG` (apiKey **39**): change `min.insync.replicas` after create (`1..min(partition RF)`); unchanged values do not bump metadata version
- `REASSIGN_PARTITIONS` (apiKey **40**): replace a partition replica list with ISR overlap (no unclean election); lagging new replicas stay out of ISR
- `ELECT_PREFERRED_LEADERS` (apiKey **41**): promote `replicaBrokerIds[0]` when that replica is online and in ISR; empty partition list means all partitions
- CLI: `alter-topic-config`, `reassign-partitions --assignment`, `elect-preferred-leaders`
- Rejection paths: frozen partitions, offline replicas, unknown broker/partition/topic, recovering controller, preferred replica not in ISR
- `METADATA_V2` / `TopicSummary` layouts **unchanged**

## What this verify did **not** cover

- Failsafe `EtcdClusterIT` (Docker-gated; this environment has no Docker)
- `acks=0`, transactions / EOS, unclean leader election, LSM storage, mixed V4/V5 broker clusters, cooperative rebalance, static membership, record headers, compression (out of scope)
- Cross-version mixed-cluster produce (`acks=all` against a V4 broker)
- Chromium operator-console rerun; Web path remains `MiniMqWebAppTest`

## Environment notes

- JDK 21 VM compiling `--release 17`
- No Docker; do not treat etcd Failsafe ITs as passed
- Root POM still has both maven-resources-plugin 3.3.1 (`overwrite=true`) and Surefire 3.2.5

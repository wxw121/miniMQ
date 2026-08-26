# MiniMQ 5.0.0 `mvn clean verify` report (acks + min.insync + leave-group)

**Date:** 2026-08-14
**Command:** `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify`
**Result:** **BUILD SUCCESS** (2026-08-14 01:40:34 UTC)
**Elapsed:** 22 min 14 s
**Git SHA (at verify):** `e850d76` (`feat: expand V5 with min.insync, leave-group, and ack executor`)
**Quality gate:** per-module JaCoCo **line ≥ 90%** (`mq-parent/pom.xml`)
**This report does not lower that gate.**

This is the **expanded 5.0.0** run after adding `min.insync.replicas`, `LEAVE_GROUP`, per-member session timeout, and the dedicated produce-ack executor. The earlier acks-only run is in [`docs/test-report-2026-08-14-v5.0.0-producer-acks.md`](test-report-2026-08-14-v5.0.0-producer-acks.md).

## Result

| Check | Outcome |
| --- | --- |
| `mvn clean verify` | **BUILD SUCCESS** |
| Surefire | **781 tests, 0 failures, 6 skipped** |
| Failsafe | **0 tests run** (Docker-gated `EtcdClusterIT` still not executed) |
| JaCoCo `check` | **passed** all modules (`LINE` ≥ 90%) |
| JaCoCo line (all modules) | **91.34% (11395 / 12475)** |
| JaCoCo instruction (all modules) | **92.20% (51968 / 56364)** |
| JaCoCo branch (all modules) | **79.13% (4228 / 5343)** |
| JaCoCo method (all modules) | **95.06% (1885 / 1983)** |
| JaCoCo class (all modules) | **99.68% (309 / 310)** |

## Surefire by module

| Module | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| mq-common | 20 | 0 | 0 | 0 |
| mq-storage | 170 | 0 | 0 | 0 |
| mq-protocol | 62 | 0 | 0 | 0 |
| mq-broker | 355 | 0 | 0 | 0 |
| mq-client | 95 | 0 | 0 | 0 |
| mq-demo | 79 | 0 | 0 | 6 |
| **Total** | **781** | **0** | **0** | **6** |

Skipped tests are the same Linux-disabled Windows cases as the acks-only report:

- `DemoCliTest` ×5 (`@DisabledOnOs(LINUX)` script/ownership cases)
- `WindowsNativeArgumentQuotingTest` ×1

Relative to the acks-only report (**767** tests): **+14** tests covering min.insync, leave-group, per-member session timeout, and the produce-ack executor.

## JaCoCo by module (`LINE` is the gate)

Parsed from each module's `target/site/jacoco/jacoco.xml` report-level counters after this verify (the same totals the HTML index shows). CSV per-class `LINE_*` sums can differ slightly because inner-class rows are attributed differently.

| Module | Instruction | Branch | Line | Method | Class |
| --- | --- | --- | --- | --- | --- |
| mq-common | 97.84% (1223/1250) | 92.90% (157/169) | **97.73% (215/220)** | 97.22% (35/36) | 100.00% (12/12) |
| mq-protocol | 94.87% (8007/8440) | 82.50% (415/503) | **94.79% (1766/1863)** | 93.86% (260/277) | 100.00% (111/111) |
| mq-storage | 90.93% (10583/11639) | 81.69% (1004/1229) | **90.10% (2338/2595)** | 97.57% (401/411) | 98.00% (49/50) |
| mq-broker | 91.25% (23743/26019) | 77.04% (2104/2731) | **90.43% (5281/5840)** | 94.64% (848/896) | 100.00% (97/97) |
| mq-client | 90.88% (3479/3828) | 76.23% (279/366) | **90.09% (818/908)** | 93.85% (168/179) | 100.00% (27/27) |
| mq-demo | 95.08% (4933/5188) | 77.97% (269/345) | **93.14% (977/1049)** | 94.02% (173/184) | 100.00% (13/13) |
| **Overall** | **92.20% (51968/56364)** | **79.13% (4228/5343)** | **91.34% (11395/12475)** | **95.06% (1885/1983)** | **99.68% (309/310)** |

`mq-storage` class coverage is 49/50 because `PartitionLog` remains uncovered in the report. `BrokerMain` is excluded from the JaCoCo agent (`mq-broker/pom.xml`) and does not appear in the broker class totals.

Six modules are at or above the **line ≥ 90%** gate. The gate was not lowered. Branch coverage is still below 80% on broker (77.04%) and client (76.23%); the gate does not enforce branch.

## What this verify covered

- `acks=1` / `acks=all` produce path, including `ALL` wait on the dedicated `minimq-produce-ack-*` executor
- Immediate `NOT_ENOUGH_REPLICAS` when ISR is empty or `isr.size() < min.insync.replicas`
- Optional trailing `min.insync.replicas` on `CREATE_TOPIC` / `CREATE_TOPIC_ADMIN`; compact constructors; ClusterController `minInSync ≤ replicationFactor`
- `LEAVE_GROUP` (apiKey **38**): leave regardless of generation; empty-group offset retention
- Per-member `JOIN_GROUP.sessionTimeoutMs` (optional trailing field; omitted decode → 10000)
- Consumer `close()` best-effort leave; heartbeat interval `max(100, sessionTimeoutMs / 3)`
- CLI: `create-topic --min-insync-replicas`, `consume --session-timeout-ms`
- Protocol golden hex for `CREATE_TOPIC` with default minIsr=1
- `METADATA_V2` / `TopicSummary` layouts **unchanged**; etcd watch remains the source of truth for min.insync

## What this verify did **not** cover

- Failsafe `EtcdClusterIT` (Docker-gated; this environment has no Docker)
- `acks=0`, transactions / EOS, unclean leader election, LSM storage, mixed V4/V5 broker clusters (out of scope)
- Cross-version mixed-cluster produce (`acks=all` against a V4 broker)
- Chromium operator-console rerun; Web path remains `MiniMqWebAppTest`

## Environment notes

- JDK 21 VM compiling `--release 17`
- No Docker; do not treat etcd Failsafe ITs as passed
- Root POM still has both maven-resources-plugin 3.3.1 (`overwrite=true`) and Surefire 3.2.5

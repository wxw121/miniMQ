# MiniMQ 5.0.0 `mvn clean verify` report (produce concurrency)

**Date:** 2026-08-14
**Command:** `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify`
**Result:** **BUILD SUCCESS** (2026-08-14 07:18:42 UTC)
**Elapsed:** 15 min 25 s
**Git SHA (at verify):** `3edefbf` (`test: cover Producer batch acks overloads and retry paths`)
**Quality gate:** per-module JaCoCo **line ≥ 90%** (`mq-parent/pom.xml`)
**This report does not lower that gate.**

This run covers the produce-concurrency work: per-partition `LogManager` appends, broker `appendBatch` PRODUCE, shared client event loops, `Producer.send(List)`, sendfile metrics at queue time, and partition flush under the log lock. The previous same-day V5 review-fix pass remains in [`docs/test-report-2026-08-14-v5.0.0-review-fixes.md`](test-report-2026-08-14-v5.0.0-review-fixes.md).

## Result

| Check | Outcome |
| --- | --- |
| `mvn clean verify` | **BUILD SUCCESS** |
| Surefire | **810 tests, 0 failures, 6 skipped** |
| Failsafe | **0 tests run** (Docker-gated `EtcdClusterIT` still not executed) |
| JaCoCo `check` | **passed** all modules (`LINE` ≥ 90%) |
| JaCoCo line (all modules) | **91.82% (12114 / 13193)** |
| JaCoCo instruction (all modules) | **92.69% (55180 / 59534)** |
| JaCoCo branch (all modules) | **79.40% (4532 / 5708)** |
| JaCoCo method (all modules) | **95.51% (1959 / 2051)** |
| JaCoCo class (all modules) | **99.69% (323 / 324)** |

## Surefire by module

| Module | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| mq-common | 20 | 0 | 0 | 0 |
| mq-storage | 176 | 0 | 0 | 0 |
| mq-protocol | 62 | 0 | 0 | 0 |
| mq-broker | 367 | 0 | 0 | 0 |
| mq-client | 106 | 0 | 0 | 0 |
| mq-demo | 79 | 0 | 0 | 6 |
| **Total** | **810** | **0** | **0** | **6** |

Skipped tests are the same Linux-disabled Windows cases as the earlier V5 reports:

- `DemoCliTest` ×5 (`@DisabledOnOs(LINUX)` script/ownership cases)
- `WindowsNativeArgumentQuotingTest` ×1

Relative to the review-fix report (**792** tests): **+18** (`mq-storage` concurrent append / `appendBatch` / reservation paths, `mq-broker` sendfile-at-queue metrics, `mq-client` shared event loops plus batch produce retries).

## JaCoCo by module (`LINE` is the gate)

Parsed from each module's `target/site/jacoco/jacoco.xml` report-level counters after this verify (the same totals the HTML index shows). CSV per-class `LINE_*` sums can differ slightly because inner-class rows are attributed differently.

| Module | Instruction | Branch | Line | Method | Class |
| --- | --- | --- | --- | --- | --- |
| mq-common | 97.84% (1223/1250) | 92.90% (157/169) | **97.73% (215/220)** | 97.22% (35/36) | 100.00% (12/12) |
| mq-protocol | 94.75% (8545/9018) | 81.74% (461/564) | **94.73% (1889/1994)** | 94.10% (271/288) | 100.00% (118/118) |
| mq-storage | 91.93% (10896/11852) | 82.11% (1037/1263) | **91.03% (2424/2663)** | 98.32% (409/416) | 98.00% (49/50) |
| mq-broker | 91.64% (25062/27348) | 77.33% (2255/2916) | **90.85% (5577/6139)** | 94.78% (872/920) | 100.00% (102/102) |
| mq-client | 92.16% (3999/4339) | 77.54% (321/414) | **91.08% (939/1031)** | 95.88% (186/194) | 100.00% (29/29) |
| mq-demo | 95.25% (5455/5727) | 78.80% (301/382) | **93.37% (1070/1146)** | 94.42% (186/197) | 100.00% (13/13) |
| **Overall** | **92.69% (55180/59534)** | **79.40% (4532/5708)** | **91.82% (12114/13193)** | **95.51% (1959/2051)** | **99.69% (323/324)** |

`mq-storage` class coverage is 49/50 because `PartitionLog` remains uncovered in the report. `BrokerMain` is excluded from the JaCoCo agent (`mq-broker/pom.xml`) and does not appear in the broker class totals.

Six modules are at or above the **line ≥ 90%** gate. The gate was not lowered.

## What this verify covered

- Per-partition `LogManager` append / `appendBatch` / `replicateAt` (no manager-wide lock on the WAL + disk hit path)
- Partition log flush under the partition lock; WAL flush after release; lock order remains log → WAL
- `DeliverySchedule` in-flight future-slot reservations for concurrent delayed produces
- Broker `PRODUCE` uses `appendBatch` and returns the batch **base** offset
- Produce-ack pool size `max(4, CPU cores)`
- AUTO sendfile FETCH metrics counted when the write is queued (not only on the completion listener)
- Process-wide shared client `NioEventLoopGroup` with refcount
- Thread-safe `Producer` metadata cache and `send(List<ProducerRecord>)` one request per partition
- `ControllerAwareNetworkClient.send` no longer holds `stateLock` while waiting on the wire
- `METADATA_V2` / `TopicSummary` layouts **unchanged**

## What this verify did **not** cover

- Failsafe `EtcdClusterIT` (Docker-gated; this environment has no Docker)
- `acks=0`, transactions / EOS, unclean leader election, LSM storage, mixed V4/V5 broker clusters, cooperative rebalance, static membership, record headers, compression (out of scope)
- Client-side linger accumulator (still out of scope; batching is explicit `send(List)`)
- Chromium operator-console rerun; Web path remains `MiniMqWebAppTest`

## Environment notes

- JDK 21 VM compiling `--release 17`
- No Docker; do not treat etcd Failsafe ITs as passed
- Root POM still has both maven-resources-plugin 3.3.1 (`overwrite=true`) and Surefire 3.2.5

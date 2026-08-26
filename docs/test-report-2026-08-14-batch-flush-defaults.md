# MiniMQ 5.0.0 `mvn clean verify` report (auto-batch, every-n flush, default 4 partitions)

**Date:** 2026-08-14
**Command:** `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify`
**Result:** **BUILD SUCCESS** (2026-08-14 09:02:58 UTC)
**Elapsed:** 15 min 35 s
**Git SHA (at verify):** `cdaa377` (`fix: keep ProduceResult JSON-serializable for the web console`)
**Quality gate:** per-module JaCoCo **line ≥ 90%** (`mq-parent/pom.xml`)
**This report does not lower that gate.**

This run covers default client auto-batch (`batchSize=50`, `lingerMs=5`), default storage flush `everyNAppends(100)`, and web/FlowRequest default partitions **4**. The same-day produce-concurrency pass remains in [`docs/test-report-2026-08-14-produce-concurrency.md`](test-report-2026-08-14-produce-concurrency.md). Existing V5 reports are not overwritten.

## Result

| Check | Outcome |
| --- | --- |
| `mvn clean verify` | **BUILD SUCCESS** |
| Surefire | **823 tests, 0 failures, 6 skipped** |
| Failsafe | **0 tests run** (Docker-gated `EtcdClusterIT` still not executed) |
| JaCoCo `check` | **passed** all modules (`LINE` ≥ 90%) |
| JaCoCo line (all modules) | **91.78% (12273 / 13372)** |
| JaCoCo instruction (all modules) | **92.67% (55829 / 60243)** |
| JaCoCo branch (all modules) | **79.52% (4579 / 5758)** |
| JaCoCo method (all modules) | **95.58% (1990 / 2082)** |
| JaCoCo class (all modules) | **99.70% (328 / 329)** |

## Surefire by module

| Module | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| mq-common | 20 | 0 | 0 | 0 |
| mq-storage | 176 | 0 | 0 | 0 |
| mq-protocol | 62 | 0 | 0 | 0 |
| mq-broker | 367 | 0 | 0 | 0 |
| mq-client | 119 | 0 | 0 | 0 |
| mq-demo | 79 | 0 | 0 | 6 |
| **Total** | **823** | **0** | **0** | **6** |

Skipped tests are the same Linux-disabled Windows cases as earlier V5 reports:

- `DemoCliTest` ×5 (`@DisabledOnOs(LINUX)` script/ownership cases)
- `WindowsNativeArgumentQuotingTest` ×1

Relative to the produce-concurrency report (**810** tests): **+13** (`mq-client` linger / batch-size / pending `send(List)` / ProducerConfig / ProduceResult getters, plus a real-broker linger IT).

## JaCoCo by module (`LINE` is the gate)

Parsed from each module's `target/site/jacoco/jacoco.xml` report-level counters after this verify.

| Module | Instruction | Branch | Line | Method | Class |
| --- | --- | --- | --- | --- | --- |
| mq-common | 97.84% (1223/1250) | 92.90% (157/169) | **97.73% (215/220)** | 97.22% (35/36) | 100.00% (12/12) |
| mq-protocol | 94.75% (8545/9018) | 81.74% (461/564) | **94.73% (1889/1994)** | 94.10% (271/288) | 100.00% (118/118) |
| mq-storage | 91.94% (10899/11855) | 82.11% (1037/1263) | **91.03% (2425/2664)** | 98.32% (410/417) | 98.00% (49/50) |
| mq-broker | 91.62% (25057/27348) | 77.43% (2258/2916) | **90.80% (5574/6139)** | 94.78% (872/920) | 100.00% (102/102) |
| mq-client | 92.17% (4650/5045) | 78.66% (365/464) | **90.98% (1100/1209)** | 96.43% (216/224) | 100.00% (34/34) |
| mq-demo | 95.25% (5455/5727) | 78.80% (301/382) | **93.37% (1070/1146)** | 94.42% (186/197) | 100.00% (13/13) |
| **Overall** | **92.67% (55829/60243)** | **79.52% (4579/5758)** | **91.78% (12273/13372)** | **95.58% (1990/2082)** | **99.70% (328/329)** |

Six modules are at or above the **line ≥ 90%** gate. The gate was not lowered.

## What this verify covered

- Default `FlushPolicy.everyNAppends(100)` for `LogManagerConfig.defaults()` and `BrokerConfig.parse` / compact constructor (`--flush-policy always` still available)
- Client `send(record)` enqueue + linger (`batchSize=50`, `lingerMs=5`); `offset()` / `flush()` / `close()` drain pending batches
- Sequential `send(); send(); send(); r0.offset()` shares one Produce per partition; `send().offset()` remains a one-record round-trip
- Real-broker linger delivery without an explicit `flush()` before poll
- Web / `FlowRequest` default partitions **4**
- `ProduceResult` JavaBean getters so delayed/retry/DLQ HTTP responses still serialize
- `METADATA_V2` / `TopicSummary` layouts **unchanged**

## What this verify did **not** cover

- Failsafe `EtcdClusterIT` (Docker-gated; this environment has no Docker)
- `acks=0`, transactions / EOS, linger-as-Kafka-only-without-offset-wait as a blocking `send().get()`, WAL sharding (out of scope)

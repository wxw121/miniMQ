# V2.1.1 Retention And Compaction Closeout Test Report

Date: 2026-07-15
Branch: `dev/v2.1.1-retention-compaction`
Version: `2.1.1-SNAPSHOT`

## Scope

V2.1.1 closes the segmented-log maintenance track:

1. Sealed-segment retention by time and/or total sealed bytes.
2. Sealed-segment compaction by latest non-null key while preserving null-key records and logical offsets.
3. Atomic catalog publication with startup cleanup for interrupted `.cleaned`, `.obsolete`, and temporary catalog files.
4. Read-snapshot protection so in-flight fetches can finish before old segment files are physically deleted.
5. LogManager maintenance wiring, append-count trigger support, checkpointing, and WAL replay behavior after retained offsets have moved below `logStartOffset`.
6. Broker CLI wiring for retention, compaction, and maintenance cadence options.

Replicas, service discovery, circuit breakers, Epoll, mmap/sendfile zero-copy, and LSM storage remain out of scope for V2.1.1 and stay on later productionization milestones.

## Full Verification

Command:

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result:

| Metric | Value |
| --- | ---: |
| Maven result | BUILD SUCCESS |
| Finished at | 2026-07-15T17:49:48+08:00 |
| Total time | 07:34 min |
| Tests run | 189 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |

## Test Counts By Module

| Module | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| `mq-common` | 10 | 0 | 0 | 0 |
| `mq-storage` | 85 | 0 | 0 | 0 |
| `mq-protocol` | 15 | 0 | 0 | 0 |
| `mq-broker` | 34 | 0 | 0 | 0 |
| `mq-client` | 24 | 0 | 0 | 0 |
| `mq-demo` | 21 | 0 | 0 | 0 |
| Total | 189 | 0 | 0 | 0 |

## JaCoCo Coverage

| Module | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| Overall | 93.03% | 80.60% | 91.97% | 97.70% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-storage` | 91.70% | 83.74% | 91.48% | 99.58% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-broker` | 94.59% | 77.26% | 92.81% | 97.17% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |

All modules remain above the 90% line-coverage gate enforced by `mvn verify`.

## Notes

- `git push origin dev/v2.1.0-segmented-log` succeeded after retry on 2026-07-15, updating `origin/dev/v2.1.0-segmented-log` from `6346818` to `18f412a`.
- The current local branch for this report is `dev/v2.1.1-retention-compaction`.

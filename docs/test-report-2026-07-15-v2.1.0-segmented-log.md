# V2.1.0 Segmented Log Closeout Test Report

Date: 2026-07-15
Branch: `dev/v2.1.0-segmented-log`
Version: `2.1.0-SNAPSHOT`

## Scope

This report closes the V2.1.0 segmented-log milestone:

- Broker CLI/config wiring for `--segment-max-bytes` and `--sparse-index-interval-records`.
- Segmented partition directories under `{dataDir}/segments/{topic}-{partition}`.
- `catalog.json`, segment `.log` files, sparse `.index` files, and index rebuild.
- Legacy single-file partition log migration and interrupted `.migrating` staging cleanup.
- WAL replay and checkpoint validation on the segmented layout.

Retention and compaction are intentionally not part of V2.1.0. They move to the next V2.1.x milestone.

## Verification

Final full command:

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Final result:

| Metric | Value |
| --- | ---: |
| Tests run | 174 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |
| Build result | SUCCESS |
| Finished at | 2026-07-15T14:29:56+08:00 |
| Total time | 06:48 min |

An earlier full run with a 240 second command timeout reached `mq-demo` and was stopped by the tool timeout, not by Maven failure. The final rerun with a longer timeout completed successfully.

## Module Coverage

| Module | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-broker` | 94.56% | 76.90% | 92.69% | 97.14% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-storage` | 91.28% | 83.68% | 91.36% | 100.00% | 100.00% |
| Overall | 93.00% | 80.05% | 91.94% | 97.66% | 100.00% |

All modules satisfy the configured JaCoCo line coverage gate of at least 90%.

## Storage-Focused Evidence

Targeted checks run during closeout:

```powershell
mvn -pl mq-broker -am '-Dtest=BrokerConfigTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=LogManagerSegmentRecoveryTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=SegmentFileTest,SegmentedPartitionLogTest,SparseOffsetIndexTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=SparseOffsetIndexTest,CheckpointStoreTest,SegmentedPartitionLogTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
```

The added tests cover:

- Segment rolling and cross-segment reads.
- Sparse index reload, replacement, corrupt index rejection, and closed behavior.
- Segment catalog validation and corrupt/missing catalog handling.
- Segment file read-budget behavior, missing segment logs, non-monotonic offsets, corrupt index rebuild, and closed behavior.
- Legacy single-file migration into the segmented layout.
- Interrupted `.migrating` staging cleanup before retrying migration.
- WAL/checkpoint replay against segmented partition logs.
- Broker CLI propagation of segment sizing and sparse-index interval settings into `LogManagerConfig`.

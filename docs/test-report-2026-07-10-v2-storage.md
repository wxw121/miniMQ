# MiniMQ V2.0 Storage Recovery Closeout Report - 2026-07-14

> Version: `2.0.0`
>
> Primary verification command: `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml`
>
> Result: `BUILD SUCCESS` in 6 minutes 54 seconds.

## Executive Summary

V2.0 closes the storage recovery foundation: independent WAL, checkpointed replay,
configurable flush policy, append-count checkpoint cadence, and broker shutdown
storage closure. The final full reactor verification completed successfully after
the broker and client coverage gaps were closed.

| Metric | Result |
| --- | ---: |
| Full reactor test count | 144 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |
| Weighted line coverage | 2158 / 2348 = 91.91% |
| JaCoCo line coverage gate | Every checked module is at least 90% |
| Full reactor verification | `BUILD SUCCESS` |

## Full Reactor Verification

The final verification completed every module, including the downstream demo Web
end-to-end suite that was not reached when earlier coverage checks failed.

| Module | Tests | Line coverage | Result |
| --- | ---: | ---: | --- |
| `mq-common` | 10 | 134 / 137 = 97.81% | PASS |
| `mq-storage` | 40 | 514 / 565 = 90.97% | PASS |
| `mq-protocol` | 15 | 328 / 357 = 91.88% | PASS |
| `mq-broker` | 34 | 572 / 619 = 92.41% | PASS |
| `mq-client` | 24 | 317 / 352 = 90.06% | PASS |
| `mq-demo` | 21 | 293 / 318 = 92.14% | PASS |
| **Total** | **144** | **2158 / 2348 = 91.91%** | **BUILD SUCCESS** |

## Verified V2.0 Storage Scope

- `WriteAheadLogTest` (11): WAL encode/decode, checksum failure, truncated-tail
  recovery, unknown record rejection, validation, and defensive payload copies.
- `CheckpointStoreTest` (6): checkpoint save/load, missing, empty, and corrupt
  fallback, validation, partition keys, and temporary-file cleanup.
- `LogManagerRecoveryTest` (5): replay without a checkpoint, behind-checkpoint
  replay, duplicate replay idempotence, and malformed checkpoint keys.
- `FlushPolicyTest` (5) and `LogManagerFlushPolicyTest` (3): all flush modes,
  invalid settings, checkpoint cadence, concurrent offset allocation, and close
  behavior.
- `FilePartitionLogTest` (10): partition-log corruption recovery, close behavior,
  and the idempotent append path used by replay.
- `BrokerConfigTest` (2): Broker flush and checkpoint CLI parsing and validation.

## Closeout Findings

1. `mq-storage` passes its JaCoCo line gate at 514 / 565 = 90.97%.
2. `LogManager.append` and `checkpoint` are serialized to prevent concurrent
   callers from allocating the same offset.
3. Broker shutdown closes `LogManager`, which checkpoints and closes the WAL and
   partition logs before rejecting further writes.
4. Broker metadata and handler tests plus client timeout, route-validation, and
   stale-cache recovery tests raise `mq-broker` to 92.41% and `mq-client` to
   90.06% without reducing the 90% gate.
5. V2.0 intentionally keeps one WAL file and one partition file per
   topic-partition. Segment rolling, sparse indexes, retention, compaction, and
   zero-copy reads remain V2.1 and V2.2 work.

## Milestone Readiness

- The verified source tree is ready for the V2.0 storage recovery milestone
  commit and the `v2.0.0` tag.
- The `v2.0.0` tag marks this verified release baseline.
- The next development baseline is V2.1 segmented logs, sparse indexes,
  retention, and compaction.

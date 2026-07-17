# MiniMQ V2.2 Read Performance Test Report

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-07-17 |
| OS | Linux 6.12.94+ x86_64 |
| JDK | OpenJDK 21.0.10 |
| Maven | 3.8.7 |
| Branch | `dev/v2.2-read-performance` |
| Project version | `2.2.0` |

## Implemented Scope

- Storage `FetchPlan` / `FileRegionSlice` / metadata-only `scan`
- Cross-segment sparse-index planning with reader leases
- FETCH response prefix encoding for trailing file bytes
- Broker `AUTO`/`HEAP` transfer mode, timeout, and metrics
- Netty `DefaultFileRegion` sendfile writer
- Real TCP integration coverage
- Opt-in `benchmark-fetch` CLI

## Focused Test Results

Command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-storage,mq-protocol,mq-broker,mq-demo -am test
```

Result: exit code 0, zero failures.

Key classes:

- `PartitionLogRecordCodecTest`
- `SegmentedPartitionLogFetchPlanTest`
- `ProtocolSerdesTest`
- `FetchTransferMetricsTest`
- `FetchResponseWriterTest`
- `ZeroCopyFetchIntegrationTest`
- `FetchBenchmarkTest`
- `DemoCliTest`

## Full Reactor Verification

Command:

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result: `BUILD SUCCESS`, 232 tests, 0 failures, 0 errors, 0 skipped. Overall line coverage 91.33%. Every module passed the configured JaCoCo line-coverage check.

## Benchmark Command And Parameters

```powershell
java -jar mq-demo/target/mq-demo-2.2.0.jar benchmark-fetch --records 256 --value-bytes 32768 --fetch-bytes 8388608 --warmup 10 --iterations 50 --segment-bytes 1048576
```

Parameters: 256 records, 32 KiB values, 8 MiB fetch budget, 10 warmups, 50 measured iterations, 1 MiB segments.

## HEAP Result

```text
mode requests bytes MiB/s mean-ms p50-ms p95-ms sendfile-responses sendfile-bytes heap-responses
heap 50 417850250 494.044 16.132 11.956 28.731 0 0 60
```

## AUTO/sendfile Result

```text
mode requests bytes MiB/s mean-ms p50-ms p95-ms sendfile-responses sendfile-bytes heap-responses
auto 50 417850250 1475.687 5.401 5.152 8.004 60 501665100 0
```

Observed comparison on this host: AUTO improved throughput from about 494 MiB/s to about 1476 MiB/s and reduced mean/p50/p95 latency. No CI percentage threshold is asserted.

## Transfer Metrics Evidence

- HEAP path recorded `sendfileResponses=0` and positive `heapResponses`
- AUTO path recorded positive `sendfileResponses` / `sendfileBytes` and `heapResponses=0`
- Real TCP test `ZeroCopyFetchIntegrationTest` asserts the same path separation

## Retention And Compaction Safety

- `SegmentedPartitionLogFetchPlanTest.openPlanDefersRetentionUntilClosed` proves open plans return `SKIPPED_READERS`
- `FetchResponseWriterTest.timeoutReleasesReaderLeaseAndClosesChannel` proves timeout releases the lease so log close can complete
- Compaction-hole and retained-start planning match heap `read` offsets

## Residual Limits

- TLS uses heap fallback in V2.2
- Transfer metrics are in-memory process counters only
- Benchmark results are host-dependent and not a CI gate
- mmap, Epoll, replication, and service discovery remain out of scope

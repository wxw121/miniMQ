# V4 Experimental LSM State Store Implementation Plan

Status: documentation only for the V3.0.1-V3.2 productionization pass.

Objective: build an optional experimental LSM state-store module without changing MiniMQ's default append-only segmented message-log write path.

## Phase 1: Module Boundary And API

1. Add an isolated `mq-state-store` module with no dependency from `mq-broker` unless a later integration gate explicitly allows it.
2. Define `StateStore`, `StateStoreIterator`, `StateStoreSnapshot`, and `StateStoreMaintenanceResult`.
3. Add API tests for `put`, `get`, `delete`, range iteration, snapshot immutability, and close behavior.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -am test
```

## Phase 2: MemTable And Serialization

1. Implement byte-comparable keys and immutable value copies.
2. Implement mutable and immutable MemTable flows.
3. Define an on-disk entry codec with record type, key length, value length, sequence number, and checksum.
4. Add malformed-codec tests for negative lengths, oversized payloads, checksum mismatch, and unknown record type.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=MemTableTest,SstEntryCodecTest test
```

## Phase 3: SSTable Flush And Reads

1. Write sorted SSTable files from immutable MemTables.
2. Add sparse block indexes, min/max key metadata, and checksum validation.
3. Implement point lookup and range iteration across MemTable plus SSTables.
4. Add tests for duplicate key precedence, tombstones, range boundaries, and corrupt block rejection.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=SstTableWriterTest,SstTableReaderTest,StateStoreReadPathTest test
```

## Phase 4: Manifest And Crash Recovery

1. Persist manifest generations with temp-file write, fsync, and atomic replace.
2. Recover by loading the latest manifest and validating all referenced SSTables.
3. Clean staging files not reachable from the manifest.
4. Add interrupted flush, corrupt manifest, missing SSTable, and startup cleanup tests.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=ManifestStoreTest,StateStoreRecoveryTest test
```

## Phase 5: Optional WAL Durability

1. Add a private state-store WAL only if flush-only durability is not enough for target experiments.
2. Keep this WAL separate from broker partition WAL files and replay logic.
3. Add replay tests for duplicate records, tombstones, truncated tail, and checksum mismatch.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=StateStoreWalTest,StateStoreWalRecoveryTest test
```

## Phase 6: Compaction

1. Implement explicit maintenance compaction that merges SSTables by key order.
2. Preserve newest values, retain tombstones until the safe-drop boundary, and validate input checksums.
3. Bound compaction by file count and bytes per run.
4. Add tests for multi-generation merge, tombstone handling, interrupted compaction cleanup, and manifest atomicity.

Verification command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=StateStoreCompactionTest test
```

## Phase 7: Benchmarks And Broker Isolation Gate

1. Add point lookup, range scan, flush, compaction, and recovery benchmarks.
2. Compare point lookup against equivalent compacted-log fixture scans.
3. Run broker regression tests with the state-store module absent from broker runtime.
4. Record benchmark results before proposing any broker-side integration.

Verification commands:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-state-store -Dtest=StateStoreBenchmarkTest test
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Djacoco.skip=true test
```

## Exit Criteria

1. The module remains optional and isolated.
2. No broker default write path depends on the LSM store.
3. Recovery and corruption tests pass for every persisted file type.
4. Benchmarks are recorded in a dedicated V4 test report.
5. Any future broker integration has a separate design review and rollback plan.

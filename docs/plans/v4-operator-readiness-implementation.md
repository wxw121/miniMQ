# V4.0 Operator Readiness Implementation Plan

Date: 2026-07-22
Status: Historical V4 implementation checklist. The local full-reactor V4 gate passed on 2026-07-29; the real-etcd gate remains environment-blocked. Current 5.0.0 tree evidence is [test-report-2026-08-14-batch-flush-defaults.md](../test-report-2026-08-14-batch-flush-defaults.md). Do not treat this plan's API keys 0-37 / error codes 0-19 ceiling as live.

## Objective

Implement the complete V4 contract: append-only protocol evolution, scoped operator authorization,
replication-aware physical and visible status, crash-safe topic deletion, audit and metrics,
retry/dead-letter links, delayed messages, operator CLI/Web flows, and a bounded three-Broker demo.
Delayed messages are a required V4 deliverable.

## Execution Rules

- Execute stages in the numbered order. A later stage may use only behavior made green by earlier
  stages.
- For every task, add the named public-behavior tests first and run the RED command. Confirm the new
  assertion fails for the missing behavior, not for compilation, dependency, or environment errors.
- Implement the smallest coherent vertical slice, run the identical GREEN command, then refactor
  only while it remains green.
- Keep protocol frame version 1 and preserve byte-for-byte positional layouts for keys 0-19.
- Keep API keys and error codes append-only. Do not renumber a value after a stage becomes green.
- Bound every collection, wait, retry, executor, process, and shutdown path.
- Close every Broker, client, Netty group, executor, file, Docker resource, and child process created
  by a test or script.
- Use `.mvn/settings.xml` for both Maven global and user settings in every command below.

## Dependency Order

1. Protocol and authorization foundation.
2. Status APIs and V2 replica-state reporting.
3. Topic administration and storage deletion.
4. Failover audit and V2 runtime metrics.
5. Retry/dead-letter metadata and client routing.
6. Delayed-message storage, replication, fetch, and client behavior.
7. CLI, Web, and local three-Broker scripts.
8. Documentation, release evidence, and full verification.

Stages 2-7 depend on Stage 1. Stage 3 depends on the metadata-v4 migration from Stage 2. Stage 4
depends on Stage 2 watermark calculation. Stage 5 depends on Stage 3 topic mutation. Stage 6 depends
on Stages 2 and 5. Stage 7 begins only after all server and client behavior is green.

## Stage 1: Protocol And Authorization Foundation

### Task 1.1: Freeze the append-only wire contract

**Files**

- Modify `mq-common/src/main/java/com/minimq/common/error/ErrorCode.java`.
- Modify `mq-protocol/src/main/java/com/minimq/protocol/ApiKeys.java`.
- Modify `mq-protocol/src/main/java/com/minimq/protocol/message/ProtocolMessages.java`.
- Modify `mq-protocol/src/main/java/com/minimq/protocol/ProtocolSerdes.java`.
- Modify `mq-client/src/main/java/com/minimq/client/NetworkClient.java`.
- Add `mq-protocol/src/test/java/com/minimq/protocol/V4ProtocolContractTest.java`.
- Modify `mq-protocol/src/test/java/com/minimq/protocol/ProtocolSerdesTest.java`.
- Add `mq-client/src/test/java/com/minimq/client/NetworkClientErrorMappingTest.java`.

**Implementation**

- Append keys 20 `OPS_STATUS`, 21 `GROUP_STATUS`, 22 `LIST_TOPICS`, 23 `DESCRIBE_TOPIC`, 24
  `DELETE_TOPIC`, 25 `INCREASE_PARTITIONS`, 26 `FAILOVER_AUDIT`, 27 `AUTH_STATUS`, 28
  `CONFIGURE_TOPIC_RELIABILITY`, 29 `MARK_BROKER_OFFLINE_ADMIN`, 30 `CREATE_TOPIC_ADMIN`, 31
  `PRODUCE_V2`, 32 `REPLICA_FETCH_V2`, 33 `TOPIC_DELETE_ACK`, 34 `FETCH_V2`, 35 `METADATA_V2`, 36
  `RUNTIME_METRICS_V2`, and 37 `UPDATE_REPLICA_STATE_V2` exactly as allocated in the design. Define
  request/response records for every V4 API in the exact field order from the design's V4 Wire Schema
  section, including explicit total/truncated fields immediately before each bounded list.
- Append `FORBIDDEN=17`, `UNKNOWN_BROKER=18`, and `STALE_TOMBSTONE=19`.
- Add request/response serdes for every new record and reject trailing bytes after every decoded
  body. Validate request limits and collection counts before allocation. Add boolean-present nullable
  string helpers for V4 optional strings and keep existing non-null string encoding unchanged.
- Add credential-prefix peeking for keys 20-30, 32, 33, 36, and 37 without consuming or validating
  the rest of the body. Keep full decode as the only source of typed request records after
  authorization.
- Add fixed hex golden-byte fixtures for keys 0, 1, 2, 3, 10, 11, 12, and 19 so later changes cannot
  alter their bodies. Each fixture must assert both decode-from-golden and encode-equals-golden. Keep
  `RUNTIME_METRICS_V2` exclusively on key 36.
- Ensure every V4 response record has a unique Java type, including empty acknowledgements, and make
  unknown response body types fail instead of writing an empty body.
- Map all appended errors through `MiniMqServerException` without collapsing their enum values.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-protocol,mq-client" -am -Dtest=V4ProtocolContractTest,ProtocolSerdesTest,NetworkClientErrorMappingTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-protocol,mq-client" -am -Dtest=V4ProtocolContractTest,ProtocolSerdesTest,NetworkClientErrorMappingTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Keys 0-37 and error codes 0-19 are unique and contiguous without changing existing values.
- Every V4 body round-trips, rejects truncation/trailing bytes, and enforces its documented bounds.
- Keys 31, 32, and 34 use the exact record item layouts from the design: scheduled produce records do
  not include offsets, replica V2 records include offset before key/value/timestamp, and fetch V2
  records include offset/key/value only.
- Golden V3 request and response bytes still decode to the same records and the encoder reproduces
  the same bytes exactly.
- Credential peeking returns only the first credential for credential-bearing V4 APIs and leaves full
  body validation to normal decode.

### Task 1.2: Add scoped local authorization before routing

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerConfig.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/auth/OperatorScope.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/auth/OperatorAuthorizer.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/MaintenanceAdminService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerServer.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/BrokerConfigTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/auth/OperatorAuthorizerTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/MaintenanceAdminServiceTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/EmbeddedBrokerChannelTest.java`.

**Implementation**

- Parse repeated `--operator-credential token=scope,scope`, `--cluster-token`,
  `--allow-legacy-unauthenticated-create`, and `--allow-legacy-internal-api`. Enforce the design's
  byte length, delimiter, duplicate, scope, count, and legacy-token rules at startup.
- Make `OperatorAuthorizer` compare credential bytes in constant time and return
  authenticated/effective-scope decisions without returning or logging a token.
- Map the legacy `--admin-token` to all four scopes. Route maintenance report/run through
  `MAINTENANCE`, maintenance/runtime metrics through `STATUS`, and authorize key 27 for any
  configured credential. Use the Task 1.1 credential-prefix peeker in `MqRequestHandler` so
  credential-bearing V4 APIs are authorized before full body decode, bounded list allocation, storage
  reads, snapshot assembly, or metadata mutation.
- Disable key 0 according to its create flag and keys 10, 11, and 12 according to the internal flag.
  Never authorize an internal API from `clientId`.
- Add `--cluster-token` parsing and constant-time matching helpers in this task, but make keys 32, 33,
  and 37 return safe default authorization/validation failures until the later replica-fetch,
  topic-delete-ack, and replica-state tasks introduce their full identity and epoch validation.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=BrokerConfigTest,OperatorAuthorizerTest,MaintenanceAdminServiceTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=BrokerConfigTest,OperatorAuthorizerTest,MaintenanceAdminServiceTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Missing/unknown credentials return `UNAUTHORIZED`; known credentials missing the required scope
  return `FORBIDDEN`; no rejected call reaches its service.
- `auth-status` reveals only the caller's scopes and target Broker id.
- Logs, exceptions, response records, and CLI-test captures contain no configured token.
- Legacy admin-token callers retain all scopes, while unsafe legacy APIs require explicit flags.
- A V4 request with a wrong credential and a huge/truncated list body is rejected before full body
  decode reaches list allocation.
- Keys 32, 33, and 37 have cluster-token plumbing in place but their exact broker/controller/leader
  epoch acceptance tests are deferred to Tasks 2.1, 3.2, and 6.2 where the required state exists.

## Stage 2: Status And Replica-State Reporting

### Task 2.1: Persist metadata format 4 and authoritative replica progress

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/ClusterMetadata.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/MetadataStore.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/MetadataSync.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/ClusterController.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/ControllerClient.java`.
- Replace `mq-broker/src/main/java/com/minimq/broker/replication/ReplicaLeoReporter.java` with
  `mq-broker/src/main/java/com/minimq/broker/replication/ReplicaStateReporter.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerServer.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/metadata/MetadataStoreTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/controller/ClusterControllerTest.java`.
- Replace `mq-broker/src/test/java/com/minimq/broker/replication/ReplicaLeoReporterTest.java` with
  `mq-broker/src/test/java/com/minimq/broker/replication/ReplicaStateReporterTest.java`.

**Implementation**

- Raise `metadataFormatVersion` to 4 and implement the exact format-3 migration. Store per-replica
  physical LEO, visible LEO, next-visible timestamp, report epochs, and last complete watermark
  snapshot per partition.
- Send key 37 after local state changes and on the existing bounded reporter interval. Authenticate
  with the cluster token and validate broker registration, membership, online state, broker epoch,
  Controller epoch, and Leader epoch before CAS mutation.
- Calculate physical/visible watermarks only from a complete current-ISR report set. Implement the
  empty-ISR, missing-report, frozen-known, frozen-unknown, and new-Leader invalidation rules exactly.
- Return `UNKNOWN_BROKER`, `BROKER_FENCED`, `CONTROLLER_FENCED`, or `STALE_LEADER_EPOCH` at the
  specified validation boundary without mutating metadata.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=MetadataStoreTest,ClusterControllerTest,ReplicaStateReporterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=MetadataStoreTest,ClusterControllerTest,ReplicaStateReporterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- A format-3 fixture loads as format 4 without losing Brokers, topics, assignments, groups, or
  epochs, and reports unknown visible watermarks until V4 reports arrive.
- Complete ISR reports calculate both minima; missing/empty ISR reports `-1`; freezing preserves
  only a previously complete snapshot.
- Stale reports are fenced and cannot advance either watermark.

### Task 2.2: Serve bounded operations and group status

**Files**

- Add `mq-broker/src/main/java/com/minimq/broker/status/BrokerStatusService.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/status/GroupStatusService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/GroupCoordinator.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerServer.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/status/BrokerStatusServiceTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/status/GroupStatusServiceTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/EmbeddedBrokerChannelTest.java`.

**Implementation**

- Serve keys 20 and 21 from one copied metadata snapshot and sort before applying each independent
  limit. Report totals and truncation for brokers, partitions, members, assignments, and lag rows.
- Implement committed-offset states `UNCOMMITTED`, normal, and `OFFSET_AHEAD`; implement readiness
  states `READY`, `FROZEN`, and `WATERMARK_UNKNOWN` without arithmetic on `-1`.
- Calculate physical and visible lag from the authoritative Controller snapshot. Report inactive
  members using the existing heartbeat timeout and deterministic member/partition ordering.
- Enforce the group/topic error mapping and `STATUS` authorization before snapshot assembly.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=BrokerStatusServiceTest,GroupStatusServiceTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=BrokerStatusServiceTest,GroupStatusServiceTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Status responses are deterministic, independently bounded, and tied to one metadata version.
- New/unassigned partitions, inactive members, missing commits, offset-ahead commits, known/unknown
  watermarks, and frozen partitions produce the documented values and states.

## Stage 3: Topic Administration And Storage Deletion

### Task 3.1: Add list, describe, create-admin, increase, and metadata refresh

**Files**

- Add `mq-broker/src/main/java/com/minimq/broker/admin/TopicAdminService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/ClusterController.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerConfig.java`.
- Modify `mq-client/src/main/java/com/minimq/client/Producer.java`.
- Modify `mq-client/src/main/java/com/minimq/client/Consumer.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/admin/TopicAdminServiceTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/controller/ClusterControllerTest.java`.
- Modify `mq-client/src/test/java/com/minimq/client/ProducerNotLeaderRetryTest.java`.
- Modify `mq-client/src/test/java/com/minimq/client/ConsumerNotLeaderRetryTest.java`.

**Implementation**

- Serve keys 22, 23, 25, and 30 with `TOPIC_ADMIN` authorization. Sort topic and partition output,
  preserve existing assignments, append consecutive ids, and use current round-robin placement.
- Reject unknown topics, no-op/shrink targets, targets above 10000, and replication factors above
  online/registered limits with their designed errors.
- Add a finite metadata cache maximum age to Producer and Consumer. Continue immediate invalidation
  on routing errors so increases and later deletion become visible without restart.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-broker,mq-client" -am -Dtest=TopicAdminServiceTest,ClusterControllerTest,ProducerNotLeaderRetryTest,ConsumerNotLeaderRetryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-broker,mq-client" -am -Dtest=TopicAdminServiceTest,ClusterControllerTest,ProducerNotLeaderRetryTest,ConsumerNotLeaderRetryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Existing partitions are byte-for-byte equivalent in metadata after an increase; only new
  consecutive partitions and one metadata-version increment appear.
- Clients discover the larger partition set within the cache maximum age and immediately after a
  stale-route error.

### Task 3.2: Implement versioned tombstones and crash-safe local cleanup

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/ClusterMetadata.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/MetadataStore.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/ClusterController.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/admin/TopicDeletionReconciler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/replication/LeaderEpochStateStore.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/PartitionManager.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerConfig.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WalRecordType.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WalRecord.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WriteAheadLog.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/RecoveryCheckpoint.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/CheckpointStore.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/LogManager.java`.
- Add `mq-storage/src/test/java/com/minimq/storage/LogManagerTopicDeletionTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/admin/TopicDeletionReconcilerTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/TopicDeletionIntegrationTest.java`.

**Implementation**

- Add persistent `DELETING`/`DELETED` tombstones with version, required Brokers, acknowledgements,
  and timestamps. Implement key 24 wait semantics, repeated-delete behavior, and key 23
  `includeDeleted` status lookup.
- Add WAL type ids 6 and 7 for topic-wide `TOPIC_DELETE_BEGIN` and `TOPIC_DELETE_COMMIT`; encode the
  tombstone version in the existing offset field and use partition id 0 as the documented topic-wide
  marker. Teach replay to process those markers before opening a partition log: a committed delete
  removes all earlier reconstructed records for that topic, and an unmatched begin completes cleanup
  and forces its commit before startup can become ACTIVE. Preserve all existing WAL type bodies.
- Add `LogManager.deleteTopic(topic, tombstoneVersion)` that appends/forces begin, closes exact
  logical-topic log entries, removes segment and legacy files, removes checkpoint/schedule entries,
  appends/forces commit, and checkpoints after commit. Match topics by parsed partition key, never by
  a raw path prefix.
- Reconcile installed tombstones off the metadata-watch thread, remove Leader epoch files, and send
  key 33 only after all local cleanup succeeds. Local cleanup failure sets a sticky storage fence and
  retries with bounded backoff; ACK rejection from a stale epoch only defers the acknowledgement, with
  lease heartbeat/self-fencing responsible for broker isolation.
- Authenticate key 33 with Broker/Controller epochs and cluster token. Apply the exact precedence for
  `UNKNOWN_BROKER`, fencing errors, and `STALE_TOMBSTONE`; transition to `DELETED` only after all
  required acknowledgements.
- Gate produce, fetch, both replica-fetch APIs, both metadata APIs, and topic description immediately
  after tombstone installation. Reserve the deleted name permanently.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=WriteAheadLogTest,CheckpointStoreTest,LogManagerTopicDeletionTest,TopicDeletionReconcilerTest,TopicDeletionIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=WriteAheadLogTest,CheckpointStoreTest,LogManagerTopicDeletionTest,TopicDeletionReconcilerTest,TopicDeletionIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Delete returns current `DELETING` on a normal wait timeout and `DELETED` after all required acks;
  repeats cause no new metadata mutation and return current state. If the mutation result cannot be
  determined inside the shared deadline, return `CONTROLLER_NOT_READY` rather than replaying it.
- Wrong tombstone versions never change acknowledgements. Offline/rejoining Brokers clean before
  ACTIVE, including Brokers outside the original required set.
- Restart from any crash point around begin/file removal/commit cannot resurrect topic data.
- Topic names containing hyphens do not cause another topic's files to be removed.

## Stage 4: Failover Audit And Runtime Metrics V2

### Task 4.1: Record a bounded failover audit stream

**Files**

- Add `mq-broker/src/main/java/com/minimq/broker/audit/FailoverAuditEventType.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/audit/FailoverAuditLog.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/ControllerRuntime.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/BrokerLeaseMonitor.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerLeaseService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/ClusterController.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/replication/LeaderActivationService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/audit/FailoverAuditLogTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/audit/FailoverAuditIntegrationTest.java`.

**Implementation**

- Record all nine specified event types at the state-transition owner, not by diffing status output.
  Keep audit failure isolated from the transition.
- Store 4096 entries, evict exactly the oldest when full, increment `overwrittenCount`, cap detail at
  1024 UTF-8 bytes without splitting a code point, and expose Broker id/start time.
- Serve key 26 newest-first with deterministic limit handling and `STATUS` authorization.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=FailoverAuditLogTest,FailoverAuditIntegrationTest,BrokerLeaseMonitorTest,BrokerLeaseServiceTest,LeaderActivationServiceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=FailoverAuditLogTest,FailoverAuditIntegrationTest,BrokerLeaseMonitorTest,BrokerLeaseServiceTest,LeaderActivationServiceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- The 4097th append removes sequence 1, retains sequences 2-4097, and reports
  `overwrittenCount=1`.
- A forced audit exception does not prevent takeover, fencing, ISR, or readiness transitions.

### Task 4.2: Add bounded expanded metrics on key 36

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/metrics/BrokerRuntimeMetrics.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/metrics/BrokerRuntimeMetricsSnapshot.java`.
- Add `mq-broker/src/main/java/com/minimq/broker/metrics/RuntimeMetricsV2Service.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/metrics/BrokerRuntimeMetricsTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/metrics/BrokerRuntimeMetricsIntegrationTest.java`.
- Modify `mq-protocol/src/test/java/com/minimq/protocol/V4ProtocolContractTest.java`.

**Implementation**

- Track cumulative request/error/throttle/latency counts, produce/fetch payload bytes, process start
  time, and audit overwrites. Derive rates from cumulative counts and uptime at snapshot time.
- Serve key 36 with independent sorted limits for counters, latency, and lag. Reuse
  `GroupStatusService` for lag rows and expose every total/truncation flag.
- Leave key 19's four-list response byte-compatible and exclude all V2 fields from its encoder.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=V4ProtocolContractTest,BrokerRuntimeMetricsTest,BrokerRuntimeMetricsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=V4ProtocolContractTest,BrokerRuntimeMetricsTest,BrokerRuntimeMetricsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Byte counts equal key/value payload bytes and rate denominators equal reported uptime.
- Key 36 truncates each family independently and includes audit overwrites; key 19 golden bytes are
  unchanged.

## Stage 5: Retry And Dead-Letter Topics

### Task 5.1: Persist and validate explicit reliability links

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/ClusterMetadata.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/metadata/MetadataStore.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/controller/ClusterController.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/admin/TopicAdminService.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/admin/TopicReliabilityTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/metadata/MetadataStoreTest.java`.

**Implementation**

- Serve key 28 with `TOPIC_ADMIN`. Atomically replace supplied retry/dead-letter links while
  preserving unspecified fields and rejecting set-plus-clear for one field.
- Require active existing targets, reject self-links and directed cycles across both link types, and
  persist only explicit names. Do not infer relationships from names.
- Clear inbound and outbound links atomically when a topic is tombstoned.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=TopicReliabilityTest,MetadataStoreTest,TopicAdminServiceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=TopicReliabilityTest,MetadataStoreTest,TopicAdminServiceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Set, replace, clear, restart, cycle rejection, missing target, and deletion cleanup all preserve one
  atomic metadata revision per successful mutation.
- Arbitrary target names work; similarly named unlinked topics are never selected.

### Task 5.2: Expose links through metadata and route producer helpers

**Files**

- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-client/src/main/java/com/minimq/client/Producer.java`.
- Modify `mq-client/src/main/java/com/minimq/client/model/ProducerRecord.java` only to add a copy-with-topic helper while preserving existing constructors.
- Add `mq-client/src/test/java/com/minimq/client/ReliabilityProducerTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/EmbeddedBrokerChannelTest.java`.

**Implementation**

- Serve key 35 with bounded active metadata and explicit retry/dead-letter links while leaving key 1
  unchanged.
- Add `sendToDeadLetter(originalTopic, record)` using ordinary key 2 and add
  `sendToRetry(originalTopic, record, deliverAtEpochMs)` that resolves the linked topic and emits key
  31. Preserve original key/value and apply normal partition routing to the target.
- Return `INVALID_REQUEST` before produce when the requested link is absent. Cache link metadata with
  the same finite maximum age and routing-error invalidation as ordinary metadata.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-broker,mq-client" -am -Dtest=ReliabilityProducerTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-broker,mq-client" -am -Dtest=ReliabilityProducerTest,EmbeddedBrokerChannelTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Mock-network tests prove helpers use only metadata links, preserve payload, select the target, and
  emit key 2 for dead letter and key 31 with the exact delivery timestamp for retry.
- Existing Producer methods and key-1 metadata behavior remain unchanged.

## Stage 6: Delayed Messages

### Task 6.1: Persist scheduling metadata and enforce Broker-wide capacity

**Files**

- Add `mq-storage/src/main/java/com/minimq/storage/DeliverySchedule.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WalRecordType.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WalRecord.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/WriteAheadLog.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/RecoveryCheckpoint.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/CheckpointStore.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/LogManager.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/PartitionManager.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/BrokerConfig.java`.
- Add `mq-storage/src/test/java/com/minimq/storage/DeliveryScheduleTest.java`.
- Add `mq-storage/src/test/java/com/minimq/storage/LogManagerDelayedMessageTest.java`.
- Modify `mq-storage/src/test/java/com/minimq/storage/WriteAheadLogTest.java`.
- Modify `mq-storage/src/test/java/com/minimq/storage/CheckpointStoreTest.java`.

**Implementation**

- Append WAL type id 8 `SCHEDULED_DATA_APPEND`; retain the existing `DATA_APPEND` body and append one
  UTC delivery timestamp to the new type's body. Store schedule maps and per-partition monotonic
  visible-prefix watermarks in the recovery checkpoint, keyed by logical topic, partition, and
  offset where applicable.
- Add synchronized batch admission in `LogManager`: reserve every future record against one
  Broker-wide count before any WAL append, append all or none on capacity rejection, and release
  reservations on pre-append failure.
- Let replication and recovery install committed future records even above the configured cap. Expose
  current count, capacity, and over-cap status; reject new leader future batches until the complete
  batch fits after the count falls below the limit.
- Parse `--max-scheduled-records` with default 100000 and allow zero only to disable new future
  admission while retaining replication/recovery correctness.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=DeliveryScheduleTest,LogManagerDelayedMessageTest,WriteAheadLogTest,CheckpointStoreTest,BrokerConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=DeliveryScheduleTest,LogManagerDelayedMessageTest,WriteAheadLogTest,CheckpointStoreTest,BrokerConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Restart reconstructs each schedule once; partial WAL tails remain recoverable; immediate records do
  not consume capacity.
- Capacity is global across partitions. Batch rejection appends nothing. Follower/recovery over-cap
  and restart with a lower configured limit preserve committed records and block only new future records.

### Task 6.2: Enforce visible-prefix fetch and protect future records from maintenance

**Files**

- Modify `mq-storage/src/main/java/com/minimq/storage/PartitionLog.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/FetchPlan.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/SegmentedPartitionLog.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/RetentionPlanner.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/SegmentCompactor.java`.
- Modify `mq-storage/src/main/java/com/minimq/storage/LogManager.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/FetchResponseWriter.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/replication/LeaderReplicaClient.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/replication/ReplicaFetcher.java`.
- Modify `mq-broker/src/main/java/com/minimq/broker/replication/ReplicaStateReporter.java`.
- Add `mq-storage/src/test/java/com/minimq/storage/SegmentedPartitionLogDelayedMaintenanceTest.java`.
- Add `mq-broker/src/test/java/com/minimq/broker/DelayedMessageIntegrationTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/handler/FetchResponseWriterTest.java`.
- Modify `mq-broker/src/test/java/com/minimq/broker/ReplicationIntegrationTest.java`.

**Implementation**

- Serve key 31 using the atomic storage batch and key 32 with timestamp-preserving replica records.
  Both enforce exact Leader epoch and tombstone gates. Replication bypasses client visibility but
  never loses timestamps.
- Restrict key 3 and key 34 reads, including zero-copy plans, to the contiguous visible prefix. Key 3
  retains physical high watermark and body layout; key 34 adds visible high watermark and
  `nextVisibleAtEpochMs` (`-1` when unblocked).
- Make visibility monotonic after due time. Notify key 37 reporting when the visible prefix changes.
- Pass future protected offsets into retention and compaction planning. Exclude a segment containing
  a future record from deletion and exclude any compaction replacement containing one. Once due, use
  ordinary policy and remove schedule/checkpoint state in the same maintenance cycle. Topic deletion
  remains the only operation that removes future records.
- Inject `Clock` in deterministic tests. Cover the documented UTC skew and rollback behavior without
  sleeping.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=SegmentedPartitionLogDelayedMaintenanceTest,DelayedMessageIntegrationTest,FetchResponseWriterTest,ReplicationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl "mq-storage,mq-broker" -am -Dtest=SegmentedPartitionLogDelayedMaintenanceTest,DelayedMessageIntegrationTest,FetchResponseWriterTest,ReplicationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- A future offset blocks every later offset before due and becomes visible at due time after restart,
  replication, and Leader promotion.
- Physical and visible watermarks plus next timestamp agree on leader, follower, Controller status,
  and promoted leader.
- Retention/compaction cannot remove a future record; ordinary maintenance can process it after due.

### Task 6.3: Make V4 consumers wait without busy polling

**Files**

- Modify `mq-client/src/main/java/com/minimq/client/Consumer.java`.
- Modify `mq-client/src/main/java/com/minimq/client/Producer.java`.
- Add `mq-client/src/test/java/com/minimq/client/DelayedProducerTest.java`.
- Add `mq-client/src/test/java/com/minimq/client/DelayedConsumerTest.java`.

**Implementation**

- Add delayed produce routing on key 31 while preserving ordinary key 2 sends.
- Switch V4 Consumer partition fetches to key 34. If an empty response has a non-negative
  `nextVisibleAtEpochMs`, wait for `min(time-until-due, remaining poll timeout)` using an injected
  sleeper/clock and retry the same offset once due. For `-1`, use the existing bounded empty-poll
  backoff. Never hold a Broker request open while waiting.
- Complete `sendToRetry` end-to-end using the Stage 5 target resolution and delayed produce path.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-client -am -Dtest=DelayedProducerTest,DelayedConsumerTest,ConsumerNotLeaderRetryTest,ProducerNotLeaderRetryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-client -am -Dtest=DelayedProducerTest,DelayedConsumerTest,ConsumerNotLeaderRetryTest,ProducerNotLeaderRetryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- A deterministic clock proves no repeated fetch occurs before the due time and polling remains
  bounded by the caller timeout.
- Immediate V4 records and all existing Producer calls retain existing behavior.

## Stage 7: CLI, Web, And Three-Broker Demo

### Task 7.1: Expose every operator and reliability workflow

**Files**

- Modify `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`.
- Modify `mq-demo/src/main/java/com/minimq/demo/web/MiniMqWebApp.java`.
- Modify `mq-demo/src/main/resources/web/index.html`.
- Modify `mq-demo/src/main/resources/web/app.js`.
- Modify `mq-demo/src/main/resources/web/styles.css`.
- Modify `mq-demo/src/test/java/com/minimq/demo/DemoCliTest.java`.
- Modify `mq-demo/src/test/java/com/minimq/demo/web/MiniMqWebAppTest.java`.

**Implementation**

- Add CLI commands `ops-status`, `group-status`, `list-topics`, `describe-topic`, `delete-topic`,
  `increase-partitions`, `failover-audit`, `auth-status`, `configure-topic-reliability`,
  `broker-offline`, `retry-produce`, `dead-letter-produce`, and `delayed-produce`.
- Move CLI/Web create to key 30 and runtime metrics to key 36. Keep the old runtime command name but
  render uptime, rates, bytes, truncation, lag, and audit overwrite values.
- Add Web endpoints and compact operator views for status, group lag, topic operations, audit,
  reliability links, and delayed produce. Use the existing static Web stack and preserve existing
  produce/consume flows.
- Validate required flags locally, never print credentials, use nonzero exit status for protocol
  errors, and show deletion state/pending Brokers and watermark unknown/frozen states explicitly.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am -Dtest=DemoCliTest,MiniMqWebAppTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am -Dtest=DemoCliTest,MiniMqWebAppTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**Acceptance**

- Tests cover dispatch, success rendering, malformed flags, every designed error state, list
  truncation, and credential redaction for every new command/endpoint.
- Web and CLI operate through protocol/client APIs rather than reaching into Broker objects.

### Task 7.2: Provide bounded local startup, failover, and shutdown scripts

**Files**

- Rewrite `scripts/start-brokers.ps1`.
- Add `scripts/stop-brokers.ps1`.
- Add `scripts/demo-v4-failover.ps1`.
- Modify `mq-demo/src/test/java/com/minimq/demo/MiniMqE2ETest.java`.
- Add `mq-demo/src/test/java/com/minimq/demo/V4OperatorWorkflowE2ETest.java`.

**Implementation**

- Start the existing three-member etcd compose stack and three V4 Brokers on configurable ports.
  Use one explicit run directory, one PID manifest, bounded readiness probes, and cleanup on partial
  startup. Never terminate a process not listed in that manifest.
- Make stop idempotent: signal owned processes, wait with a deadline, report survivors, remove the
  manifest only after every owned process exits, and leave Broker data unless an explicit
  run-directory cleanup flag is supplied.
- Automate create, produce, consume, status capture, current-Leader stop, failover wait, resumed
  produce/fetch, audit capture, and final cleanup. Every wait prints state on timeout.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am -Dtest=MiniMqE2ETest,V4OperatorWorkflowE2ETest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am -Dtest=MiniMqE2ETest,V4OperatorWorkflowE2ETest -Dsurefire.failIfNoSpecifiedTests=false test
powershell -NoProfile -Command "[scriptblock]::Create((Get-Content -Raw 'scripts/start-brokers.ps1')) | Out-Null; [scriptblock]::Create((Get-Content -Raw 'scripts/stop-brokers.ps1')) | Out-Null; [scriptblock]::Create((Get-Content -Raw 'scripts/demo-v4-failover.ps1')) | Out-Null"
```

**Acceptance**

- Deterministic E2E tests prove produce/fetch resumes after Leader failure and audit/status expose the
  transition.
- Script syntax is valid, startup cannot wait forever, and repeated stop leaves no owned Broker
  process running.

## Stage 8: Documentation And Full Verification

### Task 8.1: Synchronize public documentation and release version

**Files**

- Modify `pom.xml`.
- Modify `mq-common/pom.xml`.
- Modify `mq-storage/pom.xml`.
- Modify `mq-protocol/pom.xml`.
- Modify `mq-broker/pom.xml`.
- Modify `mq-client/pom.xml`.
- Modify `mq-demo/pom.xml`.
- Modify `README.md`.
- Modify `docs/README.md`.
- Modify `docs/design.md`.
- Modify `docs/architecture.md`.
- Modify `docs/testing.md`.
- Modify `docs/roadmap.md`.
- Modify `docs/troubleshooting.md`.
- Create `docs/test-report-2026-07-22-v4.0.0-operator-readiness.md` from fresh command results.

**Implementation**

- Update API/error tables, authorization scopes, metadata format, tombstone and delayed-record state
  machines, CLI/Web commands, three-Broker workflow, configuration defaults, upgrade constraints,
  and shutdown instructions from the implemented code.
- State that V3 clients remain wire-compatible, V3/V4 mixed Broker clusters are unsupported, deleted
  names remain reserved, retry/DLQ names are operator-selected, and delayed messages are required.
- Record exact command, timestamp, exit code, test count, and coverage for each verification run. Do
  not report a Docker-gated test as passed unless Failsafe actually executes it.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml clean verify
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml clean verify
```

**Acceptance**

- The full reactor is `BUILD SUCCESS`, every module has at least 90% JaCoCo line coverage, and every
  current-state document presents delayed messages as required V4 scope and uses V4 operator commands.
- The test report contains fresh results rather than copied historical counts.

### Task 8.2: Close the HA gate and perform the completion audit

**Files**

- Modify `docs/test-report-2026-07-22-v4.0.0-operator-readiness.md`.
- Modify `docs/known-issues-2026-07-20-v3.0.1-v3.2.md` only when the Docker gate remains blocked.

**Implementation**

- Run focused HA tests first. Check `docker info`; if available, run the real-etcd profile. If it is
  unavailable, make five bounded checks, record each result, and retain the gate as environment
  blocked rather than passed.
- Audit every V4 design section against code and a named test: API keys, errors, scopes, replica
  watermarks, topic lifecycle, audit overflow, metrics V2, reliability links, delayed capacity,
  visibility, maintenance protection, CLI/Web, scripts, and compatibility.
- Run whitespace and stale-wording checks after the final documentation edit.

**RED**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=ClusterControllerTest,ReplicaStateReporterTest,ReplicationIntegrationTest,TopicDeletionIntegrationTest,DelayedMessageIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**GREEN**

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=ClusterControllerTest,ReplicaStateReporterTest,ReplicationIntegrationTest,TopicDeletionIntegrationTest,DelayedMessageIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml clean verify
git diff --check
```

When Docker is available, also require:

```powershell
docker info
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

**Acceptance**

- Focused and full local gates pass with zero failures/errors and coverage at or above 90%.
- Real-etcd Failsafe tests pass, or the report contains five fresh bounded Docker-access failures and
  clearly leaves that environment gate open.
- Every design requirement has direct code and test evidence; no unverified requirement is marked
  complete.

## Definition Of Done

- All stages above are green in dependency order.
- API keys 0-37 and error codes 0-19 are unique; existing values and key 0-19 layouts are unchanged.
- Metadata format 4, V2 replica reports, physical/visible lag, and frozen watermark semantics survive
  restart and failover.
- Topic deletion reaches durable `DELETED`, is repeatable, and cannot resurrect local data.
- Audit overflow is observable; key 36 is bounded; key 19 remains compatible.
- Retry/dead-letter routing uses only explicit links; delayed records survive recovery and promotion,
  honor Broker-wide capacity, avoid busy polling, and are protected from maintenance until due.
- CLI, Web, and scripts exercise the implemented protocol and close every owned resource.
- Public documentation and fresh V4 release evidence match the shipped behavior.

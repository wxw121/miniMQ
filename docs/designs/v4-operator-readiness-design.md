# V4.0 Operator Readiness Design

Date: 2026-07-21
Status: Implemented in the working tree; final V4.0.0 release verification is tracked separately. This document is the decision-complete V4 contract.

## Goal

V4.0 consolidates the previously separate release-gate, multi-node operations, consumer-lag, failover-observability, admin-completeness, local-demo, and application-reliability ideas into one major version. The goal is to make MiniMQ easier to operate, diagnose, and demonstrate as a compact distributed message-queue system without replacing the existing append-only segmented-log architecture.

## Scope

1. **Release gate closure**: rerun focused V3.0.0/V3.0.1 failover evidence and attempt the real-etcd Failsafe gate when Docker API access is available.
2. **Broker and cluster operations status**: add authenticated read-only APIs and CLI output for availability, controller role, broker epoch, metadata version, transport mode, runtime metrics, maintenance state, leader readiness, ISR, and frozen partitions.
3. **Consumer group status and lag**: expose group membership, partition assignment, committed offsets, high watermarks, and lag per topic-partition.
4. **Topic administration completeness**: add list, describe, delete, and increase-partitions flows while preserving create-topic compatibility.
5. **Failover audit stream**: record controller takeover, broker lease expiry, self-fencing, leader transition, ISR shrink/expand, stale-epoch rejection, and leader-readiness transitions.
6. **Metrics and throughput visibility**: report produce/fetch request rates, bytes in/out, throttled requests, latency summaries, and per-partition lag in bounded snapshots.
7. **Local multi-node demonstration**: provide a bounded local three-broker harness and a repeatable demo flow for produce, consume, leader failure, failover, and resumed client traffic.
8. **Scoped operator permissions**: evolve the single admin token into scoped local permissions for read-only status, topic administration, maintenance, and broker-offline operations.
9. **Application reliability extensions**: add retry-topic and dead-letter-topic support first, then add delayed-message support after the operational surfaces and group lag are complete.

## Non-Goals

- Do not implement transactions or exactly-once semantics in V4.0.
- Do not replace the default broker write path with an LSM state store.
- Do not add unbounded chaos testing to normal developer runs.
- Do not make native epoll required for correctness; NIO remains the safe default.

## Operator API Direction

New admin APIs should be appended after the existing runtime-metrics API. Each API must use bounded lists, explicit counts, and defensive serdes validation.

| Area | API Intent | CLI Command |
| --- | --- | --- |
| Cluster status | Broker/controller/runtime/metadata/partition readiness snapshot | `ops-status` |
| Consumer group status | Members, assignment, offsets, high watermarks, lag | `group-status` |
| Topic admin | List, describe, delete, increase partitions | `list-topics`, `describe-topic`, `delete-topic`, `increase-partitions` |
| Failover audit | Recent bounded transition events | `failover-audit` |
| Permission check | Show effective scoped operator permissions | `auth-status` |

### API Allocation And Bounds

V4 appends protocol keys after `RUNTIME_METRICS = 19`; existing keys are not renumbered.

| Key | API | Request bound |
| ---: | --- | --- |
| 20 | `OPS_STATUS` | `brokerLimit` and `partitionLimit`, each in `1..1000` |
| 21 | `GROUP_STATUS` | required `groupId`, optional topic filter; `memberLimit` and `partitionLimit`, each in `1..1000` |
| 22 | `LIST_TOPICS` | `limit` in `1..1000` |
| 23 | `DESCRIBE_TOPIC` | one topic and `partitionLimit` in `1..1000` |
| 24 | `DELETE_TOPIC` | one topic |
| 25 | `INCREASE_PARTITIONS` | one topic and a strictly larger target count, at most `10000` |
| 26 | `FAILOVER_AUDIT` | `limit` in `1..1000` |
| 27 | `AUTH_STATUS` | one supplied credential |
| 28 | `CONFIGURE_TOPIC_RELIABILITY` | one source topic and explicit retry/DLQ links |
| 29 | `MARK_BROKER_OFFLINE_ADMIN` | one broker id |
| 30 | `CREATE_TOPIC_ADMIN` | topic, `1..10000` partitions, replication factor, token |
| 31 | `PRODUCE_V2` | topic/partition/epoch and at most `10000` timestamped records |
| 32 | `REPLICA_FETCH_V2` | one partition, bounded bytes, delivery timestamps |
| 33 | `TOPIC_DELETE_ACK` | one tombstone version and fenced broker identity |
| 34 | `FETCH_V2` | one partition, bounded bytes, physical and visible watermarks |
| 35 | `METADATA_V2` | optional topic filter; optional topic/tombstone start-after cursors; topic/partition limits in `1..10000` |
| 36 | `RUNTIME_METRICS_V2` | one credential; `counterLimit`, `latencyLimit`, and `lagLimit`, each in `1..1000` |
| 37 | `UPDATE_REPLICA_STATE_V2` | one partition and fenced broker/controller/leader identity, physical and visible LEOs, and the next visibility timestamp |

Keys 20 through 30 carry the operator credential as their first body field; key 36 does the same.
Keys 32, 33, and 37 carry the cluster credential instead. Keys 31, 34, and 35 are data/metadata APIs
and use the existing routing and epoch fencing rather than operator scopes. `DESCRIBE_TOPIC` also
carries `includeDeleted`; it is false by default in CLI/Web callers and true only for an explicit
deletion-status lookup.

All response lists are deterministically sorted before truncation. Every nested list has its own
total and truncation flag: ops status has brokers and partitions; group status has members,
assignments per member, and partition lag rows; topic description has partitions, replicas, and ISR;
audit has events; runtime metrics has every counter/latency/lag family. Replica and ISR lists are
bounded by the topic replication factor. Request validation allows replication factors up to
`10000`; placement is still capped by the registered broker count.
Member assignments are truncated by the request's `partitionLimit`. Key 36 supplies the expanded
runtime snapshot; key 19 keeps its V3 request and response body unchanged. Key 37 is internal-only
and has no list payload. Serdes reject negative counts, counts above the shared protocol maximum,
invalid request limits, trailing bytes, and frames above the existing 16 MiB frame limit.

### V4 Wire Schema

All V4 request and response bodies are positional and immutable once implemented. Primitive encodings
reuse the existing protocol helpers: i16, i32, and i64 are signed big-endian integers; bool is one
byte 0 or 1; string is a two-byte unsigned length followed by UTF-8 bytes; bytes is a four-byte
signed length followed by bytes, with -1 reserved for null byte arrays. V4 does not use a null string
sentinel because the existing string helper serializes null as an empty string. Nullable or optional
text is encoded as bool present followed by string value only when present. Optional topic filters
use present=false for absent and present=true plus a non-empty topic name for an exact filter; empty
string is never a topic filter. Key 35 also carries optional `topicStartAfter` and
`tombstoneStartAfter` cursors when the exact topic filter is absent. Each cursor is exclusive and
uses the same deterministic topic-name ordering as the response lists, so a follower can page active
topic summaries and tombstones independently without exceeding one frame's 10,000-item list bound.
Retry/DLQ link updates use explicit supplied and clear booleans, so absent, set, and clear are
distinct states.

Bounded lists are encoded as i32 total, bool truncated, i32 count, followed by count items. Total is
the untruncated number for that list only, after applying any exact filter or start-after cursor.
Count is the encoded item count and must be 0..10000, with
tighter request limits applied before allocation. Every V4 response record type is unique per API,
including empty acknowledgement records, because the existing response writer dispatches by Java body
type. The implementation must reject unknown response body types instead of silently writing an empty
body.

Credential-bearing V4 requests place the credential as the first body field so the Broker can peek
only that prefix before full body decoding. The prefix peek validates the API key, frame/body size,
and credential string bytes, then authorizes the required scope before decoding bounded lists,
assembling snapshots, reading storage, or mutating metadata. Full request decoding still validates
the complete body and rejects trailing bytes. Keys 20-30 and 36 use an operator credential prefix;
keys 32, 33, and 37 use a cluster credential prefix. Keys 31, 34, and 35 are data/metadata requests
without operator scopes and are decoded normally.

The canonical V4 body field order is:

| Key | Request body | Response body |
| ---: | --- | --- |
| 20 | string credential, i32 brokerLimit, i32 partitionLimit | i32 targetBrokerId, i64 startedAtEpochMs, i64 snapshotAtEpochMs, i64 metadataVersion, i32 metadataFormatVersion, string role, i64 brokerEpoch, i64 controllerEpoch, string transportMode, string maintenanceState, brokerList, partitionList |
| 21 | string credential, string groupId, bool topicFilterPresent, string topicFilter if present, i32 memberLimit, i32 partitionLimit | string groupId, bool topicNamePresent, string topicName if present, i32 generationId, memberList, partitionLagList |
| 22 | string credential, i32 limit | topicNameList |
| 23 | string credential, string topicName, i32 partitionLimit, bool includeDeleted | string topicName, i64 metadataVersion, bool active, bool retryTopicPresent, string retryTopic if present, bool deadLetterTopicPresent, string deadLetterTopic if present, topicPartitionList, bool tombstonePresent, tombstone if present |
| 24 | string credential, string topicName | string topicName, string tombstoneState, i64 tombstoneVersion, i64 deletedAtEpochMs, requiredBrokerIdList, acknowledgedBrokerIdList, pendingBrokerIdList |
| 25 | string credential, string topicName, i32 targetPartitionCount | string topicName, i32 previousPartitionCount, i32 newPartitionCount, i64 metadataVersion |
| 26 | string credential, i32 limit | i32 brokerId, i64 brokerStartedAtEpochMs, i64 overwrittenCount, auditEventList |
| 27 | string credential | i32 targetBrokerId, scopeList |
| 28 | string credential, string sourceTopicName, bool retryTopicSupplied, string retryTopicName if supplied, bool clearRetryTopic, bool deadLetterTopicSupplied, string deadLetterTopicName if supplied, bool clearDeadLetterTopic | string sourceTopicName, bool retryTopicPresent, string retryTopicName if present, bool deadLetterTopicPresent, string deadLetterTopicName if present, i64 metadataVersion |
| 29 | string credential, i32 brokerId | i32 brokerId, i64 metadataVersion |
| 30 | string credential, string topicName, i32 numPartitions, i32 replicationFactor | string topicName, i64 metadataVersion |
| 31 | string topicName, i32 partitionId, i64 leaderEpoch, scheduledProduceRecordList | string topicName, i32 partitionId, i64 baseOffset |
| 32 | string clusterCredential, string topicName, i32 partitionId, i32 replicaBrokerId, i64 brokerEpoch, i64 controllerEpoch, i64 leaderEpoch, i64 fetchOffset, i32 maxBytes | string topicName, i32 partitionId, i64 physicalHighWatermark, i64 visibleHighWatermark, i64 nextVisibleAtEpochMs, i64 leaderEpoch, i64 leaderLogEndOffset, scheduledReplicaRecordList |
| 33 | string clusterCredential, string topicName, i64 tombstoneVersion, i32 brokerId, i64 brokerEpoch, i64 controllerEpoch | empty TopicDeleteAckResponse body |
| 34 | string topicName, i32 partitionId, i64 leaderEpoch, i64 fetchOffset, i32 maxBytes | string topicName, i32 partitionId, i64 physicalHighWatermark, i64 visibleHighWatermark, i64 nextVisibleAtEpochMs, fetchedRecordList |
| 35 | bool topicFilterPresent, string topicFilter if present, bool topicStartAfterPresent, string topicStartAfter if present, bool tombstoneStartAfterPresent, string tombstoneStartAfter if present, i32 topicLimit, i32 partitionLimit | brokerList, topicList, topicTombstoneList, bool controllerPresent, controller if present, i64 metadataVersion, i32 metadataFormatVersion |
| 36 | string credential, i32 counterLimit, i32 latencyLimit, i32 lagLimit | i64 startedAtEpochMs, i64 snapshotAtEpochMs, i64 uptimeMs, requestMetricList, responseMetricList, latencyMetricList, throttledMetricList, i64 produceBytes, i64 fetchBytes, i64 auditOverwrittenCount, lagMetricList |
| 37 | string clusterCredential, string topicName, i32 partitionId, i32 replicaBrokerId, i64 brokerEpoch, i64 controllerEpoch, i64 leaderEpoch, i64 physicalLogEndOffset, i64 visibleLogEndOffset, i64 nextVisibleAtEpochMs | empty UpdateReplicaStateV2Response body |

Reusable item layouts are: broker = i32 brokerId, string host, i32 port, string state, i64 brokerEpoch,
i64 leaseDeadlineEpochMs; partition = string topicName, i32 partitionId, i32 leaderId,
i64 leaderEpoch, i64 physicalHighWatermark, i64 visibleHighWatermark, i64 nextVisibleAtEpochMs,
string readiness, bool frozen, replicaIdList, isrIdList; member = string memberId, bool active,
i64 lastHeartbeatEpochMs, assignmentList; assignment = string topicName, i32 partitionId;
partitionLag = string topicName, i32 partitionId, i64 committedOffset, string commitState,
i64 physicalHighWatermark, i64 physicalLag, i64 visibleHighWatermark, i64 visibleLag,
i64 nextVisibleAtEpochMs, string readiness, bool frozen; topic = string topicName, i32 partitionCount,
i32 replicationFactor, bool retryTopicPresent, string retryTopic if present, bool deadLetterTopicPresent,
string deadLetterTopic if present; topicPartition = i32 partitionId, i32 leaderId, i64 leaderEpoch,
replicaIdList, isrIdList, string readiness, bool frozen; tombstone = string state,
i64 tombstoneVersion, i64 deletedAtEpochMs, requiredBrokerIdList, acknowledgedBrokerIdList,
pendingBrokerIdList; topicTombstone = string topicName, tombstone; auditEvent = i64 sequence,
i64 timestampEpochMs, string eventType,
bool brokerIdPresent, i32 brokerId if present, bool topicNamePresent, string topicName if present,
bool partitionIdPresent, i32 partitionId if present, string oldValue, string newValue, string detail.
Metric items use i16 apiOrCode, i64 count; latency items use i16 apiKey, i64 count, i64 totalNanos,
double maxMillis; lag metric items use the partitionLag layout; id lists use the bounded-list envelope
with i32 items. Record-list items are fully fixed: scheduledProduceRecord = bytes key, bytes value,
i64 deliverAtEpochMs; scheduledReplicaRecord = i64 offset, bytes key, bytes value,
i64 deliverAtEpochMs; fetchedRecord = i64 offset, bytes key, bytes value. Key 34 intentionally keeps
plain fetchedRecord items and exposes delivery timing only through the response watermarks and
nextVisibleAtEpochMs.

V4 keeps frame version 1 and never changes an existing positional body. In particular, keys 0, 1,
2, 3, 10, 11, 12, and 19 retain their exact V3 layouts. Operator create uses key 30 and requires
`TOPIC_ADMIN`; CLI and Web move to key 30. Existing test/application callers of key 0 continue to work only when the Broker is started
with explicit `--allow-legacy-unauthenticated-create` (default false), which is logged once as an
unsafe migration mode. Delayed records use key 31; ordinary producers continue to use key 2.
Timestamp-preserving replication uses key 32; key 10 remains decodable but is enabled only by
`--allow-legacy-internal-api` and can carry only records without a delivery timestamp. Key 34 adds
visible-watermark fields without changing key 3. Key 35 exposes
retry/DLQ links, active topics, topic tombstones, paged metadata cursors, and bounded metadata without changing key 1. Legacy-mode followers page key 35 and then fetch exact key 1 topic metadata when the active topic set exceeds one metadata frame. Key 36 adds uptime, rates, bytes,
bounded lag, and truncation metadata without changing key 19. V4 Brokers use key 37 for replica
state; key 12 remains decodable for V3 compatibility but is disabled unless
`--allow-legacy-internal-api` is set and never establishes a visible LEO. Thus V3 and V4 clients can
share a Broker without positional-body ambiguity. A mixed V3/V4 Broker cluster is not supported; all
Brokers must be upgraded together because only V4 replica fetch propagates delivery schedules and
only V4 replica-state reports make visible watermarks authoritative.

Because changing the positional bodies of internal keys 10-12 would break V3, V4 disables all three
by default. `--allow-legacy-internal-api` (default false) enables them only for a legacy static-
controller deployment and logs one unsafe-mode warning. Normal V4 replication uses keys 32 and 37,
and automatic offline detection uses Controller lease expiry. Human operator requests always use key
29 and cannot bypass `BROKER_OFFLINE` authorization. The implementation removes existing uses of
keys 10-12 from V4 replication/failover tests. There is no client-id-based trust decision.

## Component Boundaries

V4 uses the existing modules and adds focused broker services instead of creating a new admin
module:

- `OperatorAuthorizer` owns constant-time credential matching and scope decisions. The configured
  legacy admin token maps to every scope. Repeated local scoped credentials map a token to one or
  more of `STATUS`, `TOPIC_ADMIN`, `MAINTENANCE`, and `BROKER_OFFLINE`.
- `BrokerStatusService` assembles one bounded snapshot from local broker state, the installed cluster
  metadata revision, runtime metrics, maintenance state, and partition readiness. It keeps
  `MqRequestHandler` from learning how each subsystem stores its state.
- `TopicAdminService` owns authorized list/describe/delete/increase/configure-reliability operations.
  `ClusterController` remains the metadata mutation boundary and revision-CAS authority.
- `FailoverAuditLog` is a thread-safe bounded ring with a monotonic sequence, event timestamp, event
  type, broker/topic/partition context, old/new values, and a short detail. It is explicitly local and
  best effort; takeover does not pretend to reconstruct events lost with a failed process.
- `ReplicaStateReporter` replaces `ReplicaLeoReporter` for a V4 cluster and sends physical/visible
  progress to `ClusterController`; the Controller owns replicated watermark calculation and frozen
  snapshots.
- `TopicDeletionReconciler` applies installed tombstones to local storage, fences the Broker on
  cleanup failure, and sends versioned acknowledgements without blocking metadata-watch threads.
- `DeliverySchedule` is storage-owned scheduling metadata keyed by topic, partition, and offset. The
  message itself remains an ordinary partition record.

## Authorization And Compatibility

`--admin-token` stays required and authorizes all four scopes. Optional repeated
`--operator-credential <token>=<scope,scope>` arguments add restricted local credentials. The first
`=` separates a UTF-8 token from a comma-separated ASCII scope list; tokens cannot contain
`=`, CR/LF, or NUL and must encode to `1..256` bytes. Duplicate tokens, unknown/duplicate/empty
scopes, credentials above `1000`, and credentials that repeat the legacy token are rejected at
startup. No token is returned in responses or written to CLI output/log messages.

`--cluster-token` authenticates keys 32, 33, and 37. It defaults to the
legacy admin token for upgrade compatibility, but production documentation recommends a distinct
shared value. These V4 requests carry broker id, broker epoch, controller epoch, and cluster token;
replica fetch and replica-state requests additionally carry leader epoch. The receiving Broker or
Controller validates the broker exists, is online, is a replica of the partition, and that all
supplied epochs exactly match current metadata before accepting a fetch, acknowledgement, or state
report. Operator credentials never authorize internal traffic.

Scoped credential configuration is broker-local. Operators must deploy the same mapping to all
brokers they intend to target; `auth-status` reports the target broker id so drift is visible. This
avoids writing secrets into shared metadata. Any syntactically valid configured credential may call
`auth-status` to inspect only its own scopes; no `STATUS` scope is required. Unknown credentials
return `UNAUTHORIZED`.

Authorization happens before controller mutation, storage access, or expensive snapshot assembly.
The mapping is:

| Scope | Operations |
| --- | --- |
| `STATUS` | ops status, group status, failover audit, runtime/maintenance metrics |
| `TOPIC_ADMIN` | create/list/describe/delete/increase/configure reliability |
| `MAINTENANCE` | maintenance report and run |
| `BROKER_OFFLINE` | explicit operator broker-offline request |

The legacy admin token is the compatibility mode, not an authorization bypass. Internal controller
traffic continues to use the existing epoch and broker fencing rules; the public key 29 is separate
from internal key 11.

Protocol error codes are append-only: `FORBIDDEN = 17`, `UNKNOWN_BROKER = 18`, and
`STALE_TOMBSTONE = 19`. No existing code is renumbered. Clients map all three to the existing
`MiniMqServerException` while preserving the exact `ErrorCode`, just as they do for `UNAUTHORIZED`.

## Snapshot Semantics

`ops-status` is broker-targeted. It reports that broker's availability, selected transport, role,
epoch, counters, maintenance state, and the broker's installed cluster snapshot. Cluster brokers and
partitions come from one copied `ClusterMetadata` value and therefore share one metadata version.

`group-status` is controller-served. It returns every partition of the group's source topic, including
unassigned/new partitions, then applies the request limit. Each V4 Broker periodically sends key 37
for every local replica after append, replication, recovery, truncation, schedule visibility change,
and on the existing bounded reporter interval. The report contains physical LEO, visible LEO (the
first not-yet-visible offset, or physical LEO when no future record blocks the prefix), and
`nextVisibleAtEpochMs` (`-1` when there is no blocked future record). The Controller persists the
latest report per replica with its broker, controller, and leader epochs; stale-epoch reports return
`STALE_LEADER_EPOCH` or `CONTROLLER_FENCED` and do not mutate metadata. A valid report satisfies
`0 <= visibleLeo <= physicalLeo`; `nextVisibleAtEpochMs` is `-1` exactly when the two LEOs are equal
and is non-negative when `visibleLeo < physicalLeo`. Violations return `INVALID_REQUEST`.

For each partition, the replication-aware physical high watermark is the minimum physical LEO among
the current ISR. The visible high watermark is the minimum visible LEO among the current ISR, capped
at the physical high watermark. An empty ISR, or any ISR member missing either required V4 LEO,
produces `WATERMARK_UNKNOWN`: both watermarks and both lag values are `-1`; the Controller never
substitutes a leader or zero. A frozen partition retains the last fully calculated physical and
visible watermarks as `FROZEN` snapshots, even though freezing clears the ISR. If no complete value
was calculated before freezing, it remains `WATERMARK_UNKNOWN`. Frozen values never advance until a
new leader is ready and a complete current-epoch ISR report set is available. Missing commits are
reported as `-1`, state `UNCOMMITTED`, and lag equal to the corresponding known watermark. Commits
above a known watermark report lag zero and `OFFSET_AHEAD`. Inactive members remain visible and
carry an `active` flag derived from the existing heartbeat timeout.

Group lag is physical replication-aware lag, not delayed-message deliverability lag. Each row also
reports `visibleHighWatermark`, `visibleLag`, and `nextVisibleAtEpochMs`. For a known visible
watermark, `nextVisibleAtEpochMs` is the maximum blocker timestamp among current-ISR replicas whose
visible LEO equals the calculated visible high watermark, because all replicas at that minimum must
advance before the replicated visible watermark can advance. It is `-1` when visible equals physical.
This makes the two meanings explicit rather than overloading one number.

Rates in key 36 are cumulative process counters divided by snapshot uptime, with `startedAtEpochMs`
and `snapshotAtEpochMs` included so the interpretation is explicit. Produce/fetch bytes count record
key and value payload bytes; request, error, throttling, and latency counters remain cumulative.
Bounded lag snapshots reuse the same lag calculation as `group-status`. Key 19 continues to expose
only its four existing V3 families.

V4 raises `ClusterMetadata.metadataFormatVersion` from 3 to 4. The format-3-to-4 migration is
deterministic: it adds empty tombstone and reliability-link maps, converts each existing
`replicaLogEndOffsets` value into physical progress with no current-epoch visible report, and leaves
all active topics, groups, brokers, assignments, and epochs unchanged. Until V4 reports arrive,
status therefore returns `WATERMARK_UNKNOWN` rather than inventing a visible watermark. Each
partition stores current-epoch replica progress plus the last complete physical/visible watermark
snapshot. A leader-epoch transition invalidates current report completeness before accepting new
key 37 reports, while retaining the last complete values solely for a possible frozen snapshot.

## Topic Mutation And Data Cleanup

Topic lists are sorted by name. Partition lists are sorted by id. Increasing partitions preserves
every existing partition and assignment, appends consecutive ids, and uses the current round-robin
replica placement rules. Shrinks and no-op targets are rejected.

Deleting a topic is a metadata CAS mutation that atomically removes the active topic, removes its
consumer groups, clears every reliability link that references it, and installs a persistent
tombstone in state `DELETING`. A tombstone contains the topic name, monotonically increasing
`tombstoneVersion`, deletion timestamp, sorted `requiredBrokerIds` copied from brokers that were
online at mutation time, and sorted cleanup acknowledgements. Deleted names stay reserved and cannot
be recreated in V4. This deliberately avoids old data becoming visible when a previously offline
broker rejoins.

The Controller changes `DELETING` to `DELETED` with a second metadata CAS only when every required
broker has acknowledged the exact tombstone version; an empty required set transitions immediately.
The tombstone remains durable in `DELETED`. Repeating delete in `DELETING` performs no new mutation,
waits on the same tombstone for the caller's remaining timeout, and returns current state and pending
broker ids. Repeating delete in `DELETED` immediately returns that completed tombstone. If the
delete CAS or final read cannot be determined within the caller budget, the API returns
`CONTROLLER_NOT_READY` rather than replaying an uncertain mutation. A name that is neither active
nor tombstoned returns `UNKNOWN_TOPIC`. `describe-topic --include-deleted` returns the tombstone
state, version, required ids, acknowledged ids, and pending ids, so completion is queryable without
repeating a mutation; ordinary describe and list commands show active topics only. The tombstone
version is the logical metadata version assigned by the first deletion CAS, so a retried request
cannot allocate another version.

Every Broker gates produce, fetch, and replica fetch as soon as it installs the tombstone; those
requests and ordinary topic description return `UNKNOWN_TOPIC`, while explicit
`describe-topic --include-deleted` returns the tombstone. It then removes cached partition logs,
segment/legacy files, checkpoint references,
leader-epoch files, and delivery schedules before sending key 33 so the Controller can persist its
broker id in the tombstone's cleanup acknowledgements.
The delete API waits up to `--topic-delete-timeout-ms` (default 10 seconds) for acknowledgements from
all required brokers. `TopicAdminService` implements this as a `CompletionStage` backed by separate
bounded waiter and mutation executors; the Netty event loop never sleeps. The same deadline covers
queueing, metadata mutation, Controller monitor entry, repository read/CAS/final-read work, and
polling. It returns the tombstone state, version, and pending broker ids; timeout after a known
tombstone is a successful `DELETING` response, not a rollback. Rejoining brokers, including brokers
not in the required set, must reconcile every tombstone before becoming ACTIVE or serving data and
may acknowledge a matching `DELETED` tombstone without changing its state.

Local storage deletion appends `TOPIC_DELETE_BEGIN`, closes and removes cached partition logs and
segment/legacy files, drops checkpoint high-watermark and delivery-schedule entries, then appends and
forces `TOPIC_DELETE_COMMIT`. The next checkpoint records a WAL position after the commit. Older WAL
appends are not rewritten; full replay after a lost checkpoint processes them and the later commit
removes the reconstructed topic again. Recovery that finds an unmatched begin completes the same
idempotent cleanup and forces the matching commit before the Broker can become ACTIVE. Repeating
begin/commit is idempotent. Deletion is applied on every broker as installed metadata is reconciled.
Only local cleanup failure sets the sticky storage fence and keeps the Broker out of ACTIVE while the
bounded reconciler retries. Stale or rejected ACK attempts do not by themselves fence the Broker;
lease heartbeat and self-fencing remain the authority for epoch isolation. Client metadata caches
receive a finite maximum age, and routing errors still force an immediate refresh, so partition
increases and deletions become observable without process restart.

## Reliability Extensions

A source topic can link to one retry topic and one dead-letter topic. Linked topics must already
exist, cannot equal the source, and cannot form any directed cycle. They remain ordinary MiniMQ
topics. `configure-topic-reliability --topic <source> [--retry-topic <name>|--clear-retry]
[--dead-letter-topic <name>|--clear-dead-letter]` atomically replaces the supplied links and preserves
unspecified links. Providing both set and clear for one link is invalid. MiniMQ imposes no retry or
dead-letter naming prefix, suffix, or convention: operators choose existing topic names, and the
persisted metadata links are the sole source of relationship truth.

MiniMQ does not infer application failure. Producer helpers `sendToRetry(originalTopic, record,
deliverAt)` and `sendToDeadLetter(originalTopic, record)` plus matching CLI commands
`retry-produce` and `dead-letter-produce` resolve the configured target through key 35, preserve
the original key/value, and fail with `INVALID_REQUEST` when the link is absent. Normal consumers and
producers remain compatible because targets are ordinary topics.

Delayed produce key 31 adds an absolute UTC `deliverAtEpochMs` to each record. The
`--max-scheduled-records` limit is one Broker-wide count across all topics and partitions, not a
per-partition allowance. On the leader, each record whose timestamp is in the future reserves one
global schedule slot before WAL append; a batch is admitted atomically only when all its future
records fit. WAL append, partition append, and schedule entry use one synchronized storage operation.
Failure before partition append releases the reservation; failure after durable WAL append is
recovered idempotently from that WAL record. The partition log stores the record normally; WAL and
recovery checkpoint persist the delivery schedule keyed by assigned offset. The checkpoint also
persists each partition's monotonic visible-prefix watermark so already-due records do not become
hidden again after schedule pruning, restart, or wall-clock rollback.

Follower replication and startup recovery never reject an already committed record because the
local schedule count exceeds the configured cap. They install every replicated or recovered future
entry and expose an `overScheduledCapacity` status when the global count is above the configured
limit. V4 does not add live configuration reload; lowering the setting takes effect on restart or
redeploy. When that lower value is below the recovered count, the Broker is temporarily over cap:
immediate records and replication/recovery continue, but leaders reject every new future record with
`THROTTLED` before WAL append until expirations or deletion reduce the count below the limit.
Admission then resumes only when the whole incoming batch fits.

Fetch exposes only the contiguous prefix whose delivery time has arrived, so a future record
preserves partition order and blocks later offsets until it is due. Existing key 3 keeps its response
layout and reports the physical `highWatermark`; it may return zero records while a future offset is
blocked. Key 34 additionally reports `visibleHighWatermark` and `nextVisibleAtEpochMs`. The latter is
the absolute timestamp of the first future record blocking the visible prefix, and is exactly `-1`
when no future record blocks the partition. After receiving no records with a non-negative next
timestamp, a V4 client waits until that timestamp, capped by its configured poll interval, then
fetches the same offset again; ordinary empty fetches with `-1` retain the client's normal bounded
poll backoff. This prevents a zero-record busy loop without turning Broker fetch into a long-held
request. When a due check advances the visible prefix, storage persists that prefix in the recovery
checkpoint before returning newly visible records; visibility therefore remains monotonic across a
later wall-clock rollback or restart. Status responses expose all three values.
Replica-fetch key 32 propagates delivery timestamps, allowing a promoted replica to enforce the same
schedule. Key 31 also works for immediate records by sending zero or a past timestamp.

Retention and compaction must not delete or compact away any record whose delivery timestamp is still
in the future according to the Broker's UTC clock; maintenance excludes that record and any segment
deletion that would contain it. Once due, the record follows ordinary retention and compaction rules,
which remove its schedule entry in the same maintenance/checkpoint cycle. Topic deletion is the sole
exception and removes future records and every schedule entry before acknowledging local cleanup.
Expired schedule entries are pruned after their offsets become visible and checkpointed. Recovery
uses offset keys, so replay replaces the same schedule entry instead of duplicating it.

All delivery timestamps are Unix epoch milliseconds in UTC. Brokers are expected to use NTP or an
equivalent source and remain within 1000 ms of each other. A Broker never exposes a record before its
own UTC clock reaches `deliverAtEpochMs`; failover delivery can therefore move by the actual clock
skew, and deployments needing tighter timing must enforce a tighter clock bound. Wall-clock rollback
before a record is due can postpone visibility; after a schedule has transitioned to visible that
transition is monotonic and rollback does not hide it again. Wall-clock advance may make a record
visible early relative to another host, within the deployment's clock-skew error.

## Failure Handling

- Unknown credentials return `UNAUTHORIZED`; insufficient scope returns `FORBIDDEN`.
- Domain lookup failures are deterministic:

  | Condition | Error |
  | --- | --- |
  | Active topic absent and no tombstone is relevant | `UNKNOWN_TOPIC` |
  | Partition absent from an active topic | `UNKNOWN_PARTITION` |
  | Group absent, including `GROUP_STATUS` | `GROUP_NOT_FOUND` |
  | `GROUP_STATUS` topic filter names no active topic | `UNKNOWN_TOPIC` |
  | `GROUP_STATUS` filter is active but differs from the group's source topic | `INVALID_REQUEST` |
  | Any operator/internal request names an unregistered broker | `UNKNOWN_BROKER` |
  | Tombstone acknowledgement does not match the current topic/version | `STALE_TOMBSTONE` |

  Authentication and syntax validation run first. After that, broker identity is checked before
  topic/tombstone version, so an unknown broker deterministically returns `UNKNOWN_BROKER` even when
  both fields are wrong. Malformed limits, invalid links, shrink/no-op partition changes, and invalid
  timestamps return `INVALID_REQUEST`.
- Key 33 accepts an acknowledgement only for the current tombstone version. An older, newer, absent,
  or topic-mismatched version returns `STALE_TOMBSTONE`; an unknown broker returns `UNKNOWN_BROKER`;
  a known broker with stale broker/controller epoch returns `BROKER_FENCED` or
  `CONTROLLER_FENCED`. No rejection changes the tombstone or its acknowledgement set.
- Topic deletion is idempotent in both `DELETING` and `DELETED`; deleting a name that has never
  existed returns `UNKNOWN_TOPIC` and never creates storage paths. Creating or increasing a
  tombstoned name returns `INVALID_REQUEST` in V4.
- Audit recording never makes a control-plane transition fail. Each Broker owns a ring of `4096`
  entries; `failover-audit` queries the targeted Broker only, defaults to 100 results, sorts newest
  first, and caps detail text at 1024 UTF-8 bytes. Sequences start at one on each process restart and
  responses include broker id/start time so they are not mistaken for cluster-global ids. Event types
  are `CONTROLLER_TAKEOVER`, `BROKER_LEASE_EXPIRED`, `BROKER_SELF_FENCED`, `LEADER_CHANGED`,
  `ISR_SHRUNK`, `ISR_EXPANDED`, `STALE_EPOCH_REJECTED`, `LEADER_READY`, and `LEADER_NOT_READY`.
  When the ring is full, append evicts exactly the oldest entry, never blocks the transition, and
  increments a process-lifetime `overwrittenCount`. Every audit response and key 36 metrics response
  exposes that counter so operators can distinguish an empty history from a truncated one.
- Delayed records reject negative timestamps. Zero or a past timestamp is immediately visible.
  Outstanding future schedules are capped Broker-wide by `--max-scheduled-records` (default
  `100000`); leader admission beyond the cap returns `THROTTLED` without appending any record from
  the request, while follower replication and recovery always restore committed records.
- Every background reconciler, scheduler-related store, test broker, Netty group, executor, and
  child process has a bounded close path.

## Data Model Notes

- For a known watermark and committed offset at or above zero, lag is
  `max(0, replication-aware high watermark - committed offset)`. Missing commits and unknown
  watermarks use the explicit states defined above rather than arithmetic on `-1`.
- Failover audit entries are best-effort operational evidence, not a durable consensus log.
- Retry and dead-letter targets are ordinary MiniMQ topics selected by the operator; only explicit
  metadata links define the relationship, and names carry no semantics.
- Delayed messages are persisted as ordinary records plus scheduling metadata so restart behavior is testable.

## Validation Strategy

V4.0 should be accepted only when focused protocol, broker, client, storage, and demo tests pass for each feature group. Full `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml` should be run before release when local resources allow it. Docker/Testcontainers work remains gated: check Docker access first, run real-etcd tests only when accessible, and record an environment-blocked result after five bounded attempts if it still cannot run.

Implementation proceeds as vertical slices: one failing public-behavior test, the minimum code to
pass it, then refactoring while green. Each new protocol record has request/response round-trip and
malformed-input coverage, including unique API-key and error-code assertions. Status tests exercise
complete ISR, missing replica reports, empty ISR, frozen-known, and frozen-unknown watermarks. Topic
deletion tests exercise `DELETING`/`DELETED`, repeated requests, stale acknowledgements, crash replay,
and restart. Partition increase tests cover producer/consumer refresh. Audit tests fill the ring and
assert oldest-entry eviction plus `overwrittenCount`. Metrics tests prove key 19 is byte-compatible
and key 36 is bounded. Delay tests cover before-due, after-due, `nextVisibleAtEpochMs`, no-busy-poll
client behavior, Broker-global capacity, over-cap recovery and configuration reduction, retention/
compaction protection, restart, replication, clock skew, and promoted-leader behavior. The
three-broker demo has bounded startup, explicit PID ownership, failover validation, and an idempotent
stop script.

## Resource-Control Rules

- Use bounded startup waits for every local multi-node test.
- Close brokers, clients, Netty event loops, executors, and Docker resources started by the test or demo.
- Keep benchmark and failover validation opt-in rather than part of the ordinary fast test loop.
- Prefer deterministic embedded-channel tests before real-network tests.

## Future Version Notes

After V4.0 is complete, future versions can be planned as design-first work:

1. Optional experimental LSM state-store module isolated from the broker default write path.
2. Transactional produce and exactly-once research.
3. Larger-scale benchmark automation and longer soak testing.
4. Multi-tenant authentication and ACL integration beyond local scoped tokens.

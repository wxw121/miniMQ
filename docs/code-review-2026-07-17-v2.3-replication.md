# Code Review Fix Log / 代码审查修复记录 - V2.3 Replication / 2026-07-17

> Branch / 分支：`dev/v2.3-replication`  
> Release target / 发布目标：`2.3.0` (`v2.3.0-replication`)  
> Follow-up patch line / 后续补丁线：`2.3.1`

## Scope / 范围

Reviewed the V2.3 replication stack across `mq-broker`, `mq-storage`, `mq-protocol`, and integration tests before the `2.3.0` release.  
本次审查覆盖 V2.3 副本机制在 `mq-broker`、`mq-storage`、`mq-protocol` 与集成测试中的实现，目标是在 `2.3.0` 发布前收口 P0 问题。

Severity legend / 严重级别：

| Level | Meaning / 含义 |
| --- | --- |
| P0 | Data loss, availability break, or correctness violation / 数据丢失、可用性中断或语义错误 |
| P1 | Serious issue under realistic failure or concurrency / 真实故障或并发下的严重问题 |
| P2 | Robustness, performance, or coverage gap / 健壮性、性能或测试覆盖缺口 |

## Findings Before Fix / 修复前问题记录

### P0 — Fixed in 2.3.0 / 已在 2.3.0 修复

| ID | Area / 区域 | Finding / 问题 | Fix / 修复 | Commit / 提交 |
| --- | --- | --- | --- | --- |
| R3-P0-01 | Client FETCH / 客户端 FETCH | Heap and sendfile paths returned records at or above replication high watermark, so clients could read data not yet replicated to all ISR members. / heap 与 sendfile 路径会返回不低于 replication high watermark 的记录，客户端可能读到尚未复制到全部 ISR 成员的数据。 | `FetchResponseWriter` caps both heap and sendfile responses with `effectiveHighWatermark`; `SegmentedPartitionLog.planFetch` accepts exclusive HW. / `FetchResponseWriter` 在 heap 与 sendfile 路径均按 `effectiveHighWatermark` 截断；`SegmentedPartitionLog.planFetch` 支持 exclusive HW。 | `8278a39` |
| R3-P0-02 | REPLICA_FETCH / 副本拉取 | Capping replica fetch records at replication HW created a chicken-and-egg problem: followers could never catch up. / 将 `REPLICA_FETCH` 记录按 replication HW 截断会造成 follower 永远无法追平。 | `REPLICA_FETCH` returns leader log bytes bounded by leader HW only; replication HW is returned in the response field for visibility. / `REPLICA_FETCH` 仅按 Leader HW 返回日志字节；响应字段仍返回 replication HW 供观测。 | `8278a39` |
| R3-P0-03 | Sendfile HW mismatch / sendfile HW 不一致 | Sendfile path planned fetch with leader HW while response declared replication HW, causing record count / byte mismatch. / sendfile 路径按 Leader HW 规划但响应声明 replication HW，导致记录数/字节不一致。 | `planFetch(offset, maxBytes, exclusiveHighWatermark)` used consistently in sendfile writer. / sendfile writer 统一使用 `planFetch(offset, maxBytes, exclusiveHighWatermark)`。 | `8278a39` |
| R3-P0-04 | Controller metadata / 控制器元数据 | `markBrokerOffline` could partially mutate ISR/leader state before rejecting invalid requests. / `markBrokerOffline` 可能在拒绝非法请求前已部分修改 ISR/Leader 状态。 | Validate all partitions first, then apply offline mutation atomically. / 先校验全部分区，再原子应用离线变更。 | `8278a39` |
| R3-P0-05 | Controller metadata / 控制器元数据 | `normalizePartition` could restore offline brokers back into ISR during metadata reads. / `normalizePartition` 在元数据读取时可能把离线 Broker 重新加回 ISR。 | Remove offline brokers from ISR and do not re-add them during normalization. / 从 ISR 移除离线 Broker，normalize 时不再恢复。 | `8278a39` |
| R3-P0-06 | ReplicaStateManager / 副本 LEO | Unknown ISR follower LEO defaulted to `0`, collapsing replication HW after leader failover even when followers already held pre-failover data. / 未知 ISR follower LEO 默认为 `0`，Leader failover 后即使 follower 已持有故障前数据，replication HW 仍被压到 0。 | Election grace period: before new writes after leadership change, unreported followers are treated as caught up; after new writes, strict min-over-ISR semantics resume. Leadership transitions reset follower LEO state via `noteLeadership`. / 选举宽限期：领导权变更后、无新写入前，未上报 follower 视为已追平；有新写入后恢复严格 min-over-ISR 语义；`noteLeadership` 在领导权变更时重置 follower LEO 状态。 | `241943d` |
| R3-P0-07 | REPLICA_FETCH / 副本拉取 | `updateReplicaLeo` used `put`, so retried/stale `fetchOffset` could lower follower LEO and drop replication HW. / `updateReplicaLeo` 使用 `put`，重试/乱序 `fetchOffset` 可能拉低 follower LEO 并压低 replication HW。 | `merge(..., Math::max)`; remove pre-response LEO update in `handleReplicaFetch`. / 改用 `merge(..., Math::max)`；移除 `handleReplicaFetch` 中的预更新。 | `241943d` |

### P1 — Fixed in 2.3.1 / 已在 2.3.1 修复

| ID | Area / 区域 | Finding / 问题 | Fix / 修复 | Commit / 提交 |
| --- | --- | --- | --- | --- |
| R3-P1-01 | Leader election / Leader 选举 | `markBrokerOffline` elected the lowest Broker ID in ISR instead of following `replicaBrokerIds`. / `markBrokerOffline` 按 ISR 中最小 Broker ID 选举，而非遵循 `replicaBrokerIds` 顺序。 | Validate and retain the first online ISR member in configured replica order, then apply the atomic offline mutation without sorting ISR. / 按配置副本顺序预校验并保留首个在线 ISR 成员，再在不排序 ISR 的前提下原子应用离线变更。 | `ad3dd54` |
| R3-P1-06 | Replica LEO / 副本 LEO | Follower LEO was broker-local memory only and unavailable to Controller after restart. / Follower LEO 仅存在于 Broker 本地内存，重启后 Controller 无法访问。 | Persist monotonic per-replica LEO in partition metadata; report non-Controller progress through API 12 and sync it in metadata responses. / 在分区元数据中持久化单调副本 LEO；非 Controller Broker 通过 API 12 上报，并由 metadata response 同步。 | `0c36053`, `7356d0d`, `6a37081`, `bbedc20` |
| R3-P1-07 | Leader election / Leader 选举 | Promotion did not compare replica LEO lag. / 提升 Leader 时不比较副本 LEO 滞后。 | Elect the online ISR member with the highest known LEO; retain configured replica order as the tie-breaker. / 选举已知 LEO 最高的在线 ISR 成员；等值时保持配置副本顺序。 | `510c9a2` |
| R3-P1-02 | ISR lifecycle | Lagging live replicas stayed in ISR and caught-up replicas were not restored. | Reconcile ISR from persisted replica LEO: shrink lagging followers and re-expand online replicas once caught up, preserving replica order with a bounded lag tolerance. | `b9bfdd4`, `060f169` |
| R3-P1-03 | Storage concurrency | `SegmentFile.append` was unsynchronized while `planRead`/`read` were synchronized. | Synchronize append, size, flush, and close with read/plan paths so sendfile planning cannot race an active tail append. | `ef06d90` |
| R3-P1-04 | Sendfile failure | Exception after `ctx.write(prefix)` could leave a truncated frame on the wire. | Track queued prefix state and close the channel on sendfile setup failure after prefix bytes are queued. | `cc17bcc` |
| R3-P1-08 | Tests | No dedicated unit tests for `ReplicaFetcher`, `LeaderReplicaClient`, or direct `REPLICA_FETCH` behavior. | Add focused `LeaderReplicaClientTest`, `ReplicaFetcherTest`, and keep direct `REPLICA_FETCH` handler coverage in `EmbeddedBrokerChannelTest`. | `2d7c68f` |

### P1 - Deferred after 2.3.1

| ID | Area / 区域 | Finding / 问题 | Planned fix / 计划修复 |
| --- | --- | --- | --- |
| R3-P1-05 | ReplicaFetcher / 副本拉取 | New TCP connection and `NioEventLoopGroup` per partition per loop. / 每轮循环、每分区新建 TCP 连接与 `NioEventLoopGroup`。 | Pool connections per leader broker. / 按 Leader Broker 复用连接。 |

### P2 — Known limitations / 已知限制（暂不排期）

| ID | Finding / 问题 |
| --- | --- |
| R3-P2-01 | No automatic broker failure detection; operators must call `MARK_BROKER_OFFLINE` manually. / 无自动 Broker 故障检测，需运维手动调用 `MARK_BROKER_OFFLINE`。 |
| R3-P2-02 | No fencing; a stale old leader can still accept writes if left running. / 无 fencing；旧 Leader 若仍在运行可能继续接受写入。 |
| R3-P2-03 | `ensureLeader` may block Netty worker threads on synchronous metadata sync (up to client timeout). / `ensureLeader` 可能因同步元数据刷新阻塞 Netty worker 线程。 |
| R3-P2-04 | `REPLICA_FETCH` response `highWatermark` field uses replication HW while records are bounded by leader HW — semantics are correct but easy to misread without docs. / `REPLICA_FETCH` 响应的 `highWatermark` 字段为 replication HW，而记录按 Leader HW 截断；语义正确但易误读。 |
| R3-P2-05 | `LeaderReplicaClient` silently drops responses when its queue overflows. / `LeaderReplicaClient` 在队列满时静默丢弃响应。 |

## Regression Tests Added / 新增回归测试

| Test / 测试 | Covers / 覆盖 |
| --- | --- |
| `FetchResponseWriterTest` heap/sendfile HW cap tests | R3-P0-01, R3-P0-03 |
| `ClusterControllerTest.markBrokerOfflineRejectsLastIsrLeaderWithoutMutatingState` | R3-P0-04 |
| `ClusterControllerTest.normalizeDoesNotRestoreOfflineReplicaToIsr` | R3-P0-05 |
| `ReplicationIntegrationTest.clientFetchWaitsForReplicationHighWatermark` | R3-P0-01 |
| `ReplicaStateManagerTest.staleReplicaFetchOffsetDoesNotLowerHighWatermark` | R3-P0-07 |
| `ReplicaStateManagerTest.unreportedFollowerDoesNotBlockReadsBeforeNewWritesAfterLeaderChange` | R3-P0-06 |
| `ReplicaStateManagerTest.newWritesBlockUnreportedFollowersAfterLeaderChange` | R3-P0-06 |
| `ReplicationIntegrationTest.clientFetchAfterFailoverWithoutWaitingForFollowerFetch` | R3-P0-06 |
| `MetadataStoreTest.saveAndLoadPreservesReplicaLogEndOffsets` | R3-P1-06 |
| `MetadataSyncTest.applyMetadataCopiesReplicaLogEndOffsets` | R3-P1-06 |
| `ReplicaLeoReporterTest.followerReporterUpdatesRemoteControllerMetadata` | R3-P1-06 |
| `ClusterControllerTest.markBrokerOfflineElectsHighestLeoIsrReplica` | R3-P1-07 |
| `ClusterControllerTest.laggingLiveReplicaShrinksOutOfIsr` | R3-P1-02 |
| `ClusterControllerTest.caughtUpLiveReplicaReExpandsIntoIsrInReplicaOrder` | R3-P1-02 |
| `SegmentFileTest.appendWaitsForSegmentMonitorHeldByReadPlanning` | R3-P1-03 |
| `FetchResponseWriterTest.sendfileSetupFailureAfterPrefixQueuedClosesChannel` | R3-P1-04 |
| `LeaderReplicaClientTest` | R3-P1-08 |
| `ReplicaFetcherTest` | R3-P1-08 |

## Verification / 验证

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Latest result before `2.3.0` tag / `2.3.0` 打标签前最新结果：`BUILD SUCCESS`, JaCoCo line coverage gate passed.

Focused replication command / 聚焦副本验证命令：

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=ClusterControllerTest,ReplicaStateManagerTest,ReplicationIntegrationTest,FetchResponseWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## Release Decision / 发布结论

- **2.3.0** ships with all P0 items above fixed. / **2.3.0** 发布时包含上述全部 P0 修复。
- **2.3.1** starts with the P1 backlog in the table above. / **2.3.1** 从上述 P1 待办开始迭代。

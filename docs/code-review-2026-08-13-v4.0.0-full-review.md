# Code Review / 全量代码审查 - V4.0.0 / 2026-08-13

> Branch / 分支：`cursor/full-review-and-test-15db`  
> Base / 基线：`main` @ `b6b83ab` (`fix: merge v4 readiness review findings`)  
> Scope / 范围：全量模块审查（`mq-common` / `mq-protocol` / `mq-storage` / `mq-broker` / `mq-client` / `mq-demo` / `scripts` / Web 前端）

This is a fresh full-tree review after the 2026-07-29 V4 readiness pass. It looks for new correctness, fencing, crash-safety, and operator-console issues rather than restating already-fixed items.

本次审查发生在 2026-07-29 V4 readiness 修复合入之后，目标是发现新的正确性、fencing、崩溃安全和运维控制台问题，而不是重复已经修过的项。

Severity legend / 严重级别：

| Level | Meaning / 含义 |
| --- | --- |
| P0 | Data loss, availability break, or correctness violation / 数据丢失、可用性中断或语义错误 |
| P1 | Serious issue under realistic failure or concurrency / 真实故障或并发下的严重问题 |
| P2 | Robustness, performance, or coverage gap / 健壮性、性能或测试覆盖缺口 |

## P0

None found. / 未发现 P0。

The previous V4 pass left protocol V4, topic tombstones, client correlation/cleanup, lease fencing, and operator auth in good shape. Remaining issues are either crash-window / HA races or operator-console semantics.

上一轮 V4 修复后，协议、tombstone、客户端关联清理、租约 fencing 与运维鉴权整体扎实。剩余问题集中在崩溃窗口、HA 竞态和运维控制台语义。

## P1 — Fixed in this review / 本次已修复

| ID | Area / 区域 | Finding / 问题 | Fix / 修复 |
| --- | --- | --- | --- |
| R4-P1-01 | Web AUTO fallback / AUTO 回退 | `Consumer.subscribe()` failures were treated as side effects only when `topicCreateAttempted` was true. If the topic already existed, JOIN_GROUP / SYNC_GROUP could mutate consumer-group state and AUTO still returned mock PASS. / 仅在刚创建 Topic 时把 subscribe 失败当成副作用。Topic 已存在时，JOIN_GROUP 仍可能改消费组状态，AUTO 却会 mock 成功。 | Always wrap subscribe failures in `FlowSideEffectException`. Added `autoFlowDoesNotFallbackWhenSubscribeFailsForExistingTopic`. / subscribe 失败一律视为副作用，并补充已存在 Topic 回归。 |
| R4-P1-02 | Metadata watch tests / Watch 测试 | Two races: (1) `fullReadsAndRebuildsWatchAfterFailure` asserted `source.starts == 2` immediately after install latch countdown; `reloadAndWatch()` installs before `source.watch()`. (2) `etcdSourceParsesEventsAndClosesWatcher` asserted `installedRevision() == 2` from the installer callback, but production assigns `installedRevision` *after* `installer.accept()`. Under full-suite load this failed as `expected 2 but was 1`. / 两处竞态：install 完成不等于 watch 已重建；installer 回调返回时 `installedRevision` 可能尚未写入。 | Await `source.restarted` before start counts, and await `installedRevision()` after the installer latch. / 断言前等待第二次 `watch()`，并等待 revision 真正落盘。 |
| R4-P1-03 | Test log flood / 测试日志洪水 | `ReplicaFetcher` logs `WARN` plus stack on every 50ms loop during failover. Surefire captured multi-million-line XML and the first full `verify` on this machine died while broker tests were still finishing. / failover 期间每 50ms 打一次 WARN 堆栈，Surefire XML 膨胀，第一次全量 verify 在 broker 模块收尾时被掐断。 | Set `ReplicaFetcher` and Netty to ERROR in `mq-broker` `logback-test.xml`. / 测试日志把 ReplicaFetcher/Netty 降到 ERROR。 |
| R4-P1-09 | Consumer poll timeout / poll 超时 | `poll()` treated any remaining nanos as enough time for FETCH_V2. Under full-suite load a 300ms follow-up poll could reach `requirePollTimeRemaining` with sub-millisecond budget, then throw `Request timeout for FETCH_V2 after 0 ms` instead of returning empty. / 剩余不足 1ms 仍发 FETCH，超时被截成 0ms 并抛错，而不是按有界等待返回空列表。 | Require at least 1ms before another poll RPC; if FETCH times out after the poll deadline is exhausted, return collected records. / 再次 RPC 前至少保留 1ms；poll 截止后的 FETCH 超时改为返回已有记录。 |

## P1 — Open / 仍待修复

### R4-P1-04 — Frozen-partition restore skips v4 leader activation

**Files:** `ClusterController.restoreFrozenPartitions` (~863–874), `MqRequestHandler.ensureLocalLeaderReady` (~1396–1402)

No-ISR lease expiry freeze is documented: the original broker re-registers with a higher epoch and is restored. `restoreFrozenPartitions` also sets `leaderReady=true` without running `LeaderActivationService` truncation or the `leaderReady()` LEO check.

`expireLeaderActivationTimeouts` reuses the same `frozen` flag. A candidate that missed activation, later went offline, then re-registered is marked ready in metadata; `ensureLocalLeaderReady` returns immediately because `partition.leaderReady` is already true.

Suggested fix: distinguish no-ISR recovery from activation-timeout freeze, or restore with `leaderReady=false` and force the normal activation path. `ClusterControllerTest.expiringSoleLeaderFreezesUntilOriginalBrokerRegistersAgain` currently does not assert `leaderReady`.

无 ISR 冻结后由原 Broker 更高 epoch 重新注册恢复，这是文档约定。但 `restoreFrozenPartitions` 会直接把 `leaderReady` 设为 true，跳过截断和 LEO 检查。激活超时冻结复用了同一个 `frozen` 标志，因此超时未激活的 Leader 在下线再注册后可能绕过 v4 激活路径。

### R4-P1-05 — Etcd-mode `MetadataSync.refresh` is a no-op after remote `leaderReady`

**Files:** `MqRequestHandler.ensureLocalLeaderReady` (~1411–1424), `MetadataSync.refresh` (~47–49)

After a successful remote `leaderReady` RPC, non-controller brokers call `MetadataSync.refresh`, which returns immediately unless `--legacy-controller` is set. Local metadata can still have `leaderReady=false` until `MetadataWatchService` delivers the revision, so the same request then fails `validateLeaderEpoch` with `LEADER_NOT_READY`.

Suggested fix: optimistically patch local readiness, or block until the installed etcd revision catches up, with a bounded deadline.

非 Controller Broker 在远程 `leaderReady` 成功后调用 `MetadataSync.refresh`，etcd 模式下这是空操作。本地元数据可能仍是 `leaderReady=false`，同一请求随后返回 `LEADER_NOT_READY`。

### R4-P1-06 — Segment append is not crash-atomic

**File:** `SegmentFile.append` (~149–165)

The log record is written before the sparse index and in-memory `lastOffset` update. If `index.append()` fails after a successful log write, a retry at the same offset writes a second physical record with the same logical offset.

Suggested fix: on post-write failure, truncate the log back to the pre-append position before rethrowing.

日志先写、索引后更新。索引失败后按同一 offset 重试会写出重复物理记录。

### R4-P1-07 — Truncate rewrites the index in place

**Files:** `SegmentFile.truncateTo` (~201–205), `SparseOffsetIndex.replace` (~88–102)

`replace()` truncates the index file to 0 then rewrites entries. A crash or I/O error mid-loop leaves a partial on-disk index while in-memory entries stay old until the loop completes. Partition-level `truncateTo` also saves the catalog after mutating the log, so a crash between those steps desynchronizes catalog metadata from segment files.

Suggested fix: write replacement index/catalog to a temp file and rename, matching `CheckpointStore` / `SegmentCatalogStore`.

索引原地 truncate-then-write，中途失败会留下残缺索引；分区 truncate 也是先改日志再写 catalog。

### R4-P1-08 — `replicateAt` bypasses scheduled-record capacity

**File:** `LogManager.replicateAt` (~181–203)

Leader `append()` calls `ensureScheduledCapacity()` for future-delivery records. Followers ingest the same records through `replicateAt` with no cap, so a leader that already holds a large delayed backlog can still OOM followers.

Suggested fix: enforce the same capacity check on replicate, and fail/throttle the replica fetch.

Leader 写入会检查延迟消息容量，follower 复制路径不会，副本可能被延迟消息打满内存。

## P2 — Open / 仍待修复

| ID | Area / 区域 | Finding / 问题 |
| --- | --- | --- |
| R4-P2-01 | Metadata HA | Followers have no etcd-mode metadata poll fallback; `ensureLeader` / `ReplicaFetcher` depend only on the watch. / etcd 模式下 follower 没有元数据拉取回退，只依赖 watch。 |
| R4-P2-02 | Topic delete | `deleteTopicInternal` is allowed during controller `recovering`; create/increase/reliability are not. / 恢复窗口内允许删 Topic，但禁止创建/扩分区。 |
| R4-P2-03 | GroupCoordinator | `offsetCommit` creates a group without join/generation checks; heartbeat ignores generation. Comment already marks member timeout as future work. / offset 提交可无加入地创建组；心跳不校验 generation。 |
| R4-P2-04 | Operator offline | `markBrokerOffline` returns `INVALID_REQUEST` when no ISR replacement exists; lease expiry freezes instead. / 无替代 ISR 时手动下线失败，租约过期则会冻结。 |
| R4-P2-05 | Delivery recovery | WAL/checkpoint replay can restore more scheduled records than `maxScheduledRecords`. / 恢复可以把延迟消息数量恢复到超过上限。 |
| R4-P2-06 | Compaction | Empty leading segments are dropped without bumping `logStartOffset` (retention does bump it). / 压缩删除空 leading segment 时不更新 `logStartOffset`。 |
| R4-P2-07 | DeliverySchedule | Empty schedule map ignores stored visible watermark and returns physical HW. / 调度表为空时忽略可见水位，直接返回物理 HW。 |
| R4-P2-08 | FETCH v1 | Sendfile V1 publishes physical/replication HW in the prefix while bytes are capped to visible HW. / V1 sendfile 响应 HW 与可见截断不一致。 |
| R4-P2-09 | Client SDK | `Producer`/`Consumer` `brokerClients` HashMap is check-then-act without synchronization; concurrent use can leak Netty clients. CLI/Web currently serialize. / 连接缓存非线程安全，并发可能泄漏 Netty 客户端。 |
| R4-P2-10 | Web operator API | Loopback Host + optional Origin; missing Origin is allowed. Local processes can invoke destructive operator routes with the server-side admin token. Acceptable for a local demo panel; not a remote-auth control plane. / 本机进程可调用带服务端 token 的破坏性运维接口。对本地演示面板可接受。 |
| R4-P2-11 | Web limits | Operator JSON limits are not server-clamped; consume-after-produce waits a fixed 750ms. / 运维 limit 无服务端上限；消费等待固定 750ms。 |
| R4-P2-12 | Dev tokens | `web-dev-admin` / `demo-admin` defaults remain in Web/dev scripts. CLI operator commands still require an explicit token. / Web/开发脚本仍有默认 token。 |

## Areas reviewed with no new P0/P1 / 未发现新 P0/P1 的区域

| Area | Assessment |
| --- | --- |
| Protocol V4 APIs 20–37, PRODUCE_V2, FETCH_V2 | Encode/decode field order and bounded-list validation match tests. |
| TopicNames | Path escape, reserved names, NFC/case folding remain solid. |
| OperatorAuthorizer | Constant-time compare and admin-token-to-all-scopes mapping are appropriate for v4. |
| ControllerClient / NetworkClient | Request-id correlation, late-response discard, and buffer release from the prior pass still hold. |
| TopicAdminService / TopicDeletionReconciler | Bounded waiters, tombstones, and storage fence on cleanup failure look correct. |
| BrokerLeaseService / BrokerAvailabilityGuard | Self-fence budget and sticky storage fence are coherent. |
| Web SSRF / JSON | Bootstrap allowlist, Host/Origin checks, duplicate-key and unknown-field rejection are covered by `MiniMqWebAppTest`. |
| scripts/*.ps1 | Ownership markers, PID+start-time manifests, and bounded stop timeouts look correct. |

## Residual operational risks / 运行风险（非新 bug）

1. Real-etcd Failsafe (`-Petcd-it`) remains environment-gated. This machine has no Docker. Do not mark `ControllerFailoverIT` / `LeaderEpochFailoverIT` passed.
2. Unclean election is still intentionally absent; no-ISR partitions stay frozen until the original broker returns.
3. Web frontend is a same-origin local operator panel, not a multi-user authenticated console.

## Recommended next patches / 建议后续补丁顺序

1. Split frozen-partition restore from leader-activation freeze (R4-P1-04).
2. Make etcd-mode `MetadataSync.refresh` wait for the committed revision (R4-P1-05).
3. Make segment append/truncate/index replacement crash-safe (R4-P1-06, R4-P1-07).
4. Enforce scheduled-record capacity on `replicateAt` (R4-P1-08).

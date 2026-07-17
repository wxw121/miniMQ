# V2.1.2 Maintenance Observability Closeout Test Report / V2.1.2 维护可观测性收口测试报告

Date / 日期：2026-07-17
Branch / 分支：`dev/v2.1.2-maintenance-observability`
Base commit / 基础提交：`97340deb783249b90ad05e57379c3826903945de`
Version / 版本：`2.1.2-SNAPSHOT`

## Scope / 范围

V2.1.2 closes the requested V2.1 maintenance hardening sequence:

V2.1.2 完成约定的 V2.1 维护加固序列：

1. `LogManager.dryRunMaintenance()` reports retention and compaction actions,
   original segment bytes, and first/last affected offsets without changing
   files, catalogs, checkpoints, or logical records.
   `LogManager.dryRunMaintenance()` 报告保留策略与压缩动作、原始 segment 字节数和受影响的首尾 offset，且不修改文件、catalog、检查点或逻辑记录。
2. Dry-run and execution share retention-then-compaction planning, so previews
   do not report compaction for segments that retention would remove first.
   Dry-run 与真实执行共享“先 retention、后 compaction”的规划，因此预览不会为本应先被保留策略删除的 segment 报告压缩动作。
3. `LogMaintenanceMetrics` exposes real runs, dry-runs, deleted bytes/segments,
   compacted bytes/segments, reader-skipped attempts, cleanup failures, and the
   last real run status.
   `LogMaintenanceMetrics` 暴露真实运行次数、dry-run 次数、删除字节/segment、压缩字节/segment、因 reader 跳过的尝试、清理失败和最近真实运行状态。
4. Real maintenance checks active readers before compaction planning. It returns
   `SKIPPED_READERS` without mutation or waiting when a read snapshot is active.
   真实维护在 compaction 规划前检查活跃 reader；存在读快照时返回 `SKIPPED_READERS`，不修改数据也不等待。
5. Compaction supports segment and original-byte limits per run. Append-triggered
   maintenance supports normal, overnight, and all-day UTC windows; manual runs
   bypass the window.
   Compaction 支持每次运行的 segment 数量与原始字节数限制。按 append 触发的维护支持普通、跨午夜和全天 UTC 窗口；手动运行绕过窗口限制。
6. `PartitionManager` exposes Java entrypoints for dry-run, manual maintenance,
   and metric snapshots. Broker CLI validates compaction limits and paired
   maintenance-window arguments.
   `PartitionManager` 暴露 dry-run、手动维护和指标快照的 Java 入口。Broker CLI 校验 compaction 限制和成对的维护窗口参数。

## Full Verification / 完整验证

Command / 命令：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result / 结果：

The requested pre-fix verification baseline finished at
`2026-07-17T14:16:50+08:00` in `07:18 min` with 202 tests. The final
post-fix verification below supersedes that baseline with the added
multi-partition status regression test.

按要求记录的修复前验证基线于 `2026-07-17T14:16:50+08:00` 完成，耗时
`07:18 min`，共执行 202 项测试。以下修复后最终验证增加了多分区状态回归测试，
并取代该基线作为发布证据。

| Metric / 指标 | Value / 数值 |
| --- | ---: |
| Maven result / Maven 结果 | BUILD SUCCESS |
| Finished at / 完成时间 | 2026-07-17T14:31:51+08:00 |
| Total time / 总耗时 | 07:17 min |
| Tests run / 测试总数 | 203 |
| Failures / 失败 | 0 |
| Errors / 错误 | 0 |
| Skipped / 跳过 | 0 |

## Test Counts By Module / 各模块测试数

| Module / 模块 | Tests / 测试数 | Failures / 失败 | Errors / 错误 | Skipped / 跳过 |
| --- | ---: | ---: | ---: | ---: |
| `mq-common` | 10 | 0 | 0 | 0 |
| `mq-storage` | 99 | 0 | 0 | 0 |
| `mq-protocol` | 15 | 0 | 0 | 0 |
| `mq-broker` | 34 | 0 | 0 | 0 |
| `mq-client` | 24 | 0 | 0 | 0 |
| `mq-demo` | 21 | 0 | 0 | 0 |
| Total / 总计 | 203 | 0 | 0 | 0 |

## JaCoCo Coverage / JaCoCo 覆盖率

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 | Class / 类 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Overall | 92.60% | 80.87% | 91.66% | 96.95% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-storage` | 91.11% | 83.42% | 91.09% | 98.61% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-broker` | 94.31% | 78.21% | 92.38% | 94.59% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |

All modules meet the Maven-enforced 90 percent line-coverage gate.

所有模块均满足 Maven 强制执行的 90% 行覆盖率门禁。

## TDD Evidence / TDD 证据

The closeout fixes were observed red before implementation:

收口修复均在实现前观察到 RED 失败：

- Dry-run reported both retention and compaction for the same segments before
  simulated retention ordering was added.
  在增加模拟 retention 顺序前，dry-run 会为同一批 segment 同时报告 retention 与 compaction。
- Dry-run replaced `lastRunStatus` before dry-run metrics were separated from
  real-run status.
  在 dry-run 指标与真实运行状态分离前，dry-run 会覆盖 `lastRunStatus`。
- Maintenance timed out behind an active read snapshot before non-blocking
  reader skip was added.
  在增加非阻塞 reader 跳过前，维护会在活跃读快照后等待并超时。
- A single CLI maintenance-window endpoint was accepted before paired option
  validation was added.
  在增加成对参数校验前，CLI 会接受单独设置的维护窗口端点。
- A mixed multi-partition run reported `SKIPPED_READERS` even when another
  partition completed compaction before successful-action precedence was added.
  在增加成功动作优先级前，多分区混合运行即使已有其他分区完成 compaction，
  仍会报告 `SKIPPED_READERS`。

Each focused regression test passed after its minimal implementation, followed
by storage, broker-reactor, and full Maven verification.

每条定向回归测试都在最小实现后通过，随后 storage、broker reactor 和完整 Maven 验证也全部通过。

## Operational Boundaries / 运维边界

- Metrics are process-local and reset when the broker restarts.
  指标仅保存在进程内，Broker 重启后会重置。
- The manual trigger is currently a Java `PartitionManager` entrypoint; no
  external admin protocol, HTTP endpoint, or user-facing CLI command is included.
  手动触发当前是 Java `PartitionManager` 入口，不包含外部管理协议、HTTP 端点或面向用户的 CLI 命令。
- `maxBytesPerRun` bounds the original bytes of selected sealed segments that
  may be rewritten. Compaction may still read newer segments to determine the
  latest value for each key.
  `maxBytesPerRun` 限制可被重写的已选 sealed segment 原始字节数；compaction 仍可能读取较新的 segment，以确定每个 key 的最新值。
- The full build retains the existing Maven Shade overlapping-resource warnings;
  they are unchanged by V2.1.2 and do not fail verification.
  完整构建保留既有 Maven Shade 资源重叠警告；V2.1.2 未改变这些警告，且它们不会导致验证失败。

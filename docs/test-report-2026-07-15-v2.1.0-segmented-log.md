# V2.1.0 Segmented Log Closeout Test Report / V2.1.0 分段日志收口测试报告

Date / 日期：2026-07-15
Branch / 分支：`dev/v2.1.0-segmented-log`
Version / 版本：`2.1.0-SNAPSHOT`

## Scope / 范围

This report closes the V2.1.0 segmented-log milestone:

本报告完成 V2.1.0 分段日志里程碑的收口：

- Broker CLI/config wiring for `--segment-max-bytes` and `--sparse-index-interval-records`.
  完成 `--segment-max-bytes` 与 `--sparse-index-interval-records` 的 Broker CLI/配置装配。
- Segmented partition directories under `{dataDir}/segments/{topic}-{partition}`.
  在 `{dataDir}/segments/{topic}-{partition}` 下使用分段分区目录。
- `catalog.json`, segment `.log` files, sparse `.index` files, and index rebuild.
  支持 `catalog.json`、segment `.log` 文件、稀疏 `.index` 文件及索引重建。
- Legacy single-file partition log migration and interrupted `.migrating` staging cleanup.
  支持旧单文件分区日志迁移和中断后 `.migrating` 暂存目录清理。
- WAL replay and checkpoint validation on the segmented layout.
  支持分段布局上的 WAL 重放与检查点校验。

Retention and compaction are intentionally not part of V2.1.0. They move to the next V2.1.x milestone.

V2.1.0 有意不包含保留策略与压缩；这些能力移至下一个 V2.1.x 里程碑。

## Verification / 验证

Final full command / 最终完整命令：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Final result / 最终结果：

| Metric / 指标 | Value / 数值 |
| --- | ---: |
| Tests run / 测试总数 | 174 |
| Failures / 失败 | 0 |
| Errors / 错误 | 0 |
| Skipped / 跳过 | 0 |
| Build result / 构建结果 | SUCCESS |
| Finished at / 完成时间 | 2026-07-15T14:29:56+08:00 |
| Total time / 总耗时 | 06:48 min |

An earlier full run with a 240 second command timeout reached `mq-demo` and was stopped by the tool timeout, not by Maven failure. The final rerun with a longer timeout completed successfully.

此前一次完整运行设置了 240 秒命令超时，执行到 `mq-demo` 时被工具超时终止，并非 Maven 失败。随后使用更长超时重新运行并成功完成。

## Module Coverage / 模块覆盖率

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 | Class / 类 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-broker` | 94.56% | 76.90% | 92.69% | 97.14% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-storage` | 91.28% | 83.68% | 91.36% | 100.00% | 100.00% |
| Overall | 93.00% | 80.05% | 91.94% | 97.66% | 100.00% |

All modules satisfy the configured JaCoCo line coverage gate of at least 90%.

所有模块均满足配置的 JaCoCo 行覆盖率不低于 90% 的门禁。

## Storage-Focused Evidence / 存储专项证据

Targeted checks run during closeout / 收口期间执行的定向检查：

```powershell
mvn -pl mq-broker -am '-Dtest=BrokerConfigTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=LogManagerSegmentRecoveryTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=SegmentFileTest,SegmentedPartitionLogTest,SparseOffsetIndexTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
mvn -pl mq-storage -am '-Dtest=SparseOffsetIndexTest,CheckpointStoreTest,SegmentedPartitionLogTest' '-Dsurefire.failIfNoSpecifiedTests=false' -gs .mvn/settings.xml -s .mvn/settings.xml test
```

The added tests cover / 新增测试覆盖：

- Segment rolling and cross-segment reads.
  segment 滚动与跨 segment 读取。
- Sparse index reload, replacement, corrupt index rejection, and closed behavior.
  稀疏索引重载、替换、损坏索引拒绝与关闭行为。
- Segment catalog validation and corrupt/missing catalog handling.
  segment catalog 校验及损坏/缺失 catalog 处理。
- Segment file read-budget behavior, missing segment logs, non-monotonic offsets, corrupt index rebuild, and closed behavior.
  segment 文件读取预算、segment 日志缺失、非单调 offset、损坏索引重建与关闭行为。
- Legacy single-file migration into the segmented layout.
  旧单文件向分段布局迁移。
- Interrupted `.migrating` staging cleanup before retrying migration.
  重试迁移前清理中断遗留的 `.migrating` 暂存目录。
- WAL/checkpoint replay against segmented partition logs.
  针对分段分区日志的 WAL/检查点重放。
- Broker CLI propagation of segment sizing and sparse-index interval settings into `LogManagerConfig`.
  将 Broker CLI 的 segment 大小与稀疏索引间隔配置传递到 `LogManagerConfig`。

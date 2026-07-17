# V2.1.1 Retention And Compaction Closeout Test Report / V2.1.1 保留策略与压缩收口测试报告

Date / 日期：2026-07-15
Branch / 分支：`dev/v2.1.1-retention-compaction`
Version / 版本：`2.1.1-SNAPSHOT`

## Scope / 范围

V2.1.1 closes the segmented-log maintenance track:

V2.1.1 完成分段日志维护链路的收口：

1. Sealed-segment retention by time and/or total sealed bytes.
   按时间和/或已封闭 segment 总字节数执行保留策略。
2. Sealed-segment compaction by latest non-null key while preserving null-key records and logical offsets.
   按最新非 null key 压缩已封闭 segment，同时保留 null-key 记录与逻辑 offset。
3. Atomic catalog publication with recovery-aware startup cleanup for interrupted `.cleaned`, `.obsolete`, and temporary catalog files.
   原子发布 catalog，并在启动恢复时清理中断遗留的 `.cleaned`、`.obsolete` 和临时 catalog 文件。
4. Read-snapshot protection so in-flight fetches can finish before old segment files are physically deleted.
   使用读快照保护，让进行中的 fetch 在旧 segment 文件物理删除前完成。
5. LogManager maintenance wiring, append-count trigger support, checkpointing, and WAL replay behavior after retained offsets or compacted offset holes.
   完成 LogManager 维护装配、按追加次数触发、检查点，以及保留删除或压缩产生 offset 空洞后的 WAL 重放行为。
6. Broker CLI wiring for retention, compaction, and maintenance cadence options.
   完成保留策略、压缩和维护节奏选项的 Broker CLI 装配。

Replicas, service discovery, circuit breakers, Epoll, mmap/sendfile zero-copy, and LSM storage remain out of scope for V2.1.1 and stay on later productionization milestones.

副本、服务发现、熔断器、Epoll、mmap/sendfile 零拷贝和 LSM 存储不属于 V2.1.1 范围，继续保留在后续生产化里程碑中。

## Full Verification / 完整验证

Command / 命令：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result / 结果：

| Metric / 指标 | Value / 数值 |
| --- | ---: |
| Maven result / Maven 结果 | BUILD SUCCESS |
| Finished at / 完成时间 | 2026-07-15T19:06:35+08:00 |
| Total time / 总耗时 | 07:09 min |
| Tests run / 测试总数 | 193 |
| Failures / 失败 | 0 |
| Errors / 错误 | 0 |
| Skipped / 跳过 | 0 |

## Test Counts By Module / 各模块测试数

| Module / 模块 | Tests / 测试数 | Failures / 失败 | Errors / 错误 | Skipped / 跳过 |
| --- | ---: | ---: | ---: | ---: |
| `mq-common` | 10 | 0 | 0 | 0 |
| `mq-storage` | 89 | 0 | 0 | 0 |
| `mq-protocol` | 15 | 0 | 0 | 0 |
| `mq-broker` | 34 | 0 | 0 | 0 |
| `mq-client` | 24 | 0 | 0 | 0 |
| `mq-demo` | 21 | 0 | 0 | 0 |
| Total / 总计 | 193 | 0 | 0 | 0 |

## JaCoCo Coverage / JaCoCo 覆盖率

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 | Class / 类 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Overall | 92.96% | 80.57% | 91.97% | 97.72% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-storage` | 91.59% | 83.58% | 91.50% | 99.58% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-broker` | 94.59% | 77.26% | 92.81% | 97.17% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |

All modules remain above the 90% line-coverage gate enforced by `mvn verify`.

所有模块均保持在 `mvn verify` 强制执行的 90% 行覆盖率门禁之上。

## Notes / 备注

- `git push origin dev/v2.1.0-segmented-log` succeeded after retry on 2026-07-15, updating `origin/dev/v2.1.0-segmented-log` from `6346818` to `18f412a`.
  `git push origin dev/v2.1.0-segmented-log` 于 2026-07-15 重试后成功，将 `origin/dev/v2.1.0-segmented-log` 从 `6346818` 更新到 `18f412a`。
- The current local branch for this report is `dev/v2.1.1-retention-compaction`.
  本报告对应的当前本地分支为 `dev/v2.1.1-retention-compaction`。
- The 19:06 verification includes review fixes for interrupted compaction replacement recovery, WAL replay over compacted offset holes, and combined time/byte retention accounting.
  19:06 的验证包含以下评审修复：中断压缩替换恢复、跨压缩 offset 空洞的 WAL 重放，以及时间/容量组合保留策略计数。

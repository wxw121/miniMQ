# 证据、性能与复测记录

> 这页用于核对简历和口述中的数字。没有日期、命令、环境和范围的数字，不作为面试结论。

## 1. 当前可引用的历史验证

| 项目 | 结果 | 适用范围 | 来源 |
| --- | --- | --- | --- |
| 日期 / 提交 | 2026-08-14，`cdaa377` | 当时的 V5.0.0 工作树 | [全量报告](../../test-report-2026-08-14-batch-flush-defaults.md) |
| 命令 | `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify` | Maven 全 reactor | 同上 |
| Surefire | 823 tests，0 failures，6 skipped | 六个模块的单元、集成、E2E/演示相关测试 | 同上 |
| JaCoCo | overall line 91.78%（12273 / 13372） | 每模块 line ≥ 90% 门槛通过 | 同上 |
| Failsafe | 0 tests run | Docker-gated `EtcdClusterIT` 未执行 | 同上 |

**正确说法：** “2026-08-14、`cdaa377` 的全量验证记录为 823 tests、0 failures、整体 line coverage 91.78%。这是历史基线。”

**必须补充：** 当前分支已有报告后的 Web 相关变更；面试前应重新执行下面的当前版本验证，不得把这份历史报告描述成当前提交的结果。

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml clean verify
```

复测结束后，把命令、开始/结束时间、Git SHA、测试数、失败数和每模块 JaCoCo 结果填入本页或新建带日期报告。

## 2. real-etcd 验证边界

real-etcd Failsafe 集成验证是 **环境受限的未完成项**。历史 Docker 检查无法访问本机 Docker API，因此不能标记为已通过。Docker 恢复可用时的验证入口：

```powershell
docker version
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

只有 `ControllerFailoverIT`、`EtcdControllerCoordinationStoreIT`、`LeaderEpochFailoverIT` 和 `EtcdClusterMetadataRepositoryIT` 实际运行并记录新结果后，才能说 real-etcd gate 已关闭。来源：[已知问题](../../known-issues-2026-07-20-v3.0.1-v3.2.md)。

## 3. 历史性能证据：只作方法证明

2026-07-17、V2.2.0、`dev/v2.2-read-performance` 的 Linux 6.12.94 x86_64 / OpenJDK 21.0.10 主机上，`benchmark-fetch` 使用 256 条 32 KiB value、8 MiB fetch、10 次 warmup、50 次测量、1 MiB segment 的结果如下：

| 模式 | 吞吐 | mean | p50 | p95 | 说明 |
| --- | ---: | ---: | ---: | ---: | --- |
| HEAP | 494.044 MiB/s | 16.132 ms | 11.956 ms | 28.731 ms | `sendfileResponses=0` |
| AUTO/sendfile | 1475.687 MiB/s | 5.401 ms | 5.152 ms | 8.004 ms | `sendfileResponses=60` |

该结果证明 benchmark 与 heap/sendfile 路径可观测，不证明当前 V5 的性能，也不是 CI 门槛。来源：[V2.2 性能报告](../../test-report-2026-07-17-v2.2-read-performance.md)。

## 4. 当前版本性能复测模板

先打包当前分支，再执行相同参数。将 jar 版本替换为根 `pom.xml` 中的实际版本。

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am package -DskipTests
java -jar mq-demo/target/mq-demo-5.0.0.jar benchmark-fetch --records 256 --value-bytes 32768 --fetch-bytes 8388608 --warmup 10 --iterations 50 --segment-bytes 1048576 --network-transport nio
```

| 字段 | 本次结果（面试前填写） |
| --- | --- |
| 日期 / Git SHA | **待复测** |
| OS / CPU / 内存 | **待复测** |
| JDK / Maven | **待复测** |
| jar 版本 | **待复测** |
| records / value / fetch / iterations | **待复测** |
| HEAP MiB/s / mean / p95 | **待复测** |
| AUTO MiB/s / mean / p95 | **待复测** |
| `sendfileResponses` / `heapResponses` | **待复测** |
| 结论与波动说明 | **待复测；仅限本机指定参数，不和 Kafka 跨环境直接比较。** |

若要比较 auto-batch、`acks=LEADER|ALL` 或并发生产，必须为每组参数记录相同硬件、消息大小、并发、warmup 与样本数；目前不能从仓库的历史报告推导出这些当前版本数字。

## 5. 面试时的证据优先级

1. 当前提交的全量验证报告（若已复跑）。
2. [架构与设计](../../architecture.md)、[测试策略](../../testing.md) 和可运行演示。
3. 带环境的历史性能报告。
4. 未复测或 Docker 受限的项：主动标为限制，说明下一步验证方案。

禁止用“生产级 Kafka”“已完成 real-etcd 验证”“exactly-once / 精确一次”描述当前项目。

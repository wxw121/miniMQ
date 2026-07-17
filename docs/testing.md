# MiniMQ Testing Guide / MiniMQ 测试文档

> This document records how to run tests, how coverage is measured, what the current baseline is, and where to find reports.
> 本文记录测试运行方式、覆盖率统计方式、当前质量基线以及报告位置。

## 1. Test Philosophy / 测试理念

MiniMQ tests are designed around risk. Low-level protocol and storage code receives direct unit tests because small bugs there can corrupt messages or break network compatibility. Broker and client workflows receive integration tests because correctness depends on multiple modules interacting through real protocol frames. The demo module receives end-to-end tests to verify the whole user-facing path.

MiniMQ 的测试按风险组织。协议和存储属于低层核心模块，小 bug 可能导致消息损坏或网络不兼容，因此使用直接单元测试。Broker 与 Client 的正确性依赖多模块协作，因此使用集成测试。Demo 模块则通过端到端测试验证用户可见路径。

## 2. Test Stack / 测试技术栈

| Tool / 工具 | Purpose / 用途 |
| --- | --- |
| JUnit 5 | Unit and integration test framework. / 单元测试和集成测试框架。 |
| Maven Surefire | Runs tests and writes test reports. / 运行测试并写出测试报告。 |
| JaCoCo | Generates code coverage reports. / 生成代码覆盖率报告。 |
| Netty EmbeddedChannel | Tests channel handlers without opening real sockets. / 在不打开真实 socket 的情况下测试 channel handler。 |
| TempDir | Isolates storage and metadata files per test. / 为每个测试隔离存储文件和元数据文件。 |
| BrokerTestHarness | Starts real test brokers on free local ports. / 在空闲本地端口启动真实测试 Broker。 |

## 3. Commands / 测试命令

Run all tests / 运行全部测试：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Run tests quietly / 静默运行测试：

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml verify
```

Run one module and its dependencies / 运行单个模块及其依赖模块：

```powershell
mvn -q -pl mq-broker -am test -gs .mvn/settings.xml -s .mvn/settings.xml
```

Run one test class / 运行单个测试类：

```powershell
mvn -q -pl mq-protocol -am -Dtest=ProtocolSerdesTest test -gs .mvn/settings.xml -s .mvn/settings.xml
```

## 4. Current Quality Baseline / 当前质量基线

Latest verified command / 最近验证过的命令：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Latest result / 最近结果：

| Metric / 指标 | Value / 数值 |
| --- | ---: |
| Tests run / 测试总数 | 203 |
| Failures / 失败数 | 0 |
| Errors / 错误数 | 0 |
| Skipped / 跳过数 | 0 |
| Overall line coverage / 总体行覆盖率 | 91.66% |
| Overall instruction coverage / 总体指令覆盖率 | 92.60% |
| Overall branch coverage / 总体分支覆盖率 | 80.87% |
| Overall method coverage / 总体方法覆盖率 | 96.95% |
| Overall class coverage / 总体类覆盖率 | 100.00% |

## 5. Coverage By Module / 模块覆盖率

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 | Class / 类 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-broker` | 94.31% | 78.21% | 92.38% | 94.59% | 100.00% |
| `mq-client` | 92.26% | 76.52% | 90.06% | 92.68% | 100.00% |
| `mq-common` | 96.84% | 84.38% | 97.84% | 100.00% | 100.00% |
| `mq-demo` | 95.06% | 74.56% | 92.14% | 95.16% | 100.00% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% | 100.00% |
| `mq-storage` | 91.11% | 83.42% | 91.09% | 98.61% | 100.00% |

```mermaid
xychart-beta
    title "Line Coverage By Module / 模块行覆盖率"
    x-axis ["broker/代理", "client/客户端", "common/公共", "demo/演示", "protocol/协议", "storage/存储"]
    y-axis "Line % / 行覆盖率 %" 0 --> 100
    bar [92.38, 90.06, 97.84, 92.14, 91.88, 91.09]
```

```mermaid
xychart-beta
    title "Overall Coverage / 总体覆盖率"
    x-axis ["Instruction/指令", "Branch/分支", "Line/行", "Method/方法", "Class/类"]
    y-axis "Coverage % / 覆盖率 %" 0 --> 100
    bar [92.60, 80.87, 91.65, 96.95, 100.00]
```


## 6. Key Flow Test Evidence / 关键流程测试结果截图

The numeric baseline above and the closeout report are the source of truth for the latest verified Surefire and JaCoCo results. The screenshots below remain visual aids for the core flows, so a new reader can inspect the main testing shape without opening every `target` report directory.

上面的数字基线与收口报告是最近一次通过验证的 Surefire 与 JaCoCo 结果来源。下面截图保留为核心流程的可视化辅助，新读者无需逐个打开 `target` 报告目录，也能快速看到关键测试形态。

### 6.1 Full Build Verification / 完整构建验证

![Full Maven Verify / 完整 Maven 验证](assets/test-results/full-maven-verify.png)

### 6.2 Protocol And Storage / 协议与存储

![Protocol And Storage Unit Tests / 协议与存储单元测试](assets/test-results/protocol-storage-unit.png)

### 6.3 Broker And Client Integration / Broker 与客户端集成

![Broker And Client Integration Tests / Broker 与客户端集成测试](assets/test-results/broker-client-integration.png)

### 6.4 CLI And End-To-End Flow / CLI 与端到端流程

![CLI And End-To-End Tests / CLI 与端到端测试](assets/test-results/demo-e2e.png)

### 6.5 Coverage Summary / 覆盖率汇总

![Coverage Summary / 覆盖率汇总](assets/test-results/coverage-summary.png)

## 7. Test Suite Map / 测试套件地图

| Module / 模块 | Test class / 测试类 | Type / 类型 | Focus / 关注点 |
| --- | --- | --- | --- |
| `mq-common` | `Murmur2PartitionerTest` | Unit / 单元测试 | Stable partitioning and invalid partition count. / 稳定分区与非法分区数校验。 |
| `mq-common` | `RangeAssignorTest` | Unit / 单元测试 | Range assignment and assignment serialization boundaries. / Range 分配与 assignment 序列化边界。 |
| `mq-common` | `CommonModelTest` | Unit / 单元测试 | Records and error code mapping. / Record 模型与错误码映射。 |
| `mq-storage` | `FilePartitionLogTest` | Unit / 单元测试 | Append/read/reopen recovery, truncated-tail recovery, structural corruption rejection, close behavior, and `LogManager` caching. / 追加、读取、重开恢复、尾部截断恢复、结构性损坏拒绝、关闭行为与 `LogManager` 缓存。 |
| `mq-storage` | `WriteAheadLogTest` | Unit / 单元测试 | WAL encode/decode, checksum mismatch, truncated-tail recovery, unknown record type rejection, and payload copy safety. / WAL 编解码、校验和不匹配、尾部截断恢复、未知记录类型拒绝与 payload 拷贝安全。 |
| `mq-storage` | `CheckpointStoreTest` | Unit / 单元测试 | Recovery checkpoint safe write, load, corrupt-checkpoint fallback, and temp-file cleanup. / 恢复 checkpoint 安全写入、加载、损坏 checkpoint 回退与临时文件清理。 |
| `mq-storage` | `LogManagerRecoveryTest` | Unit / 单元测试 | WAL replay without checkpoint, replay when partition logs fall behind checkpoint, and duplicate replay idempotence. / 无 checkpoint WAL 重放、分区日志落后 checkpoint 时重放，以及重复重放幂等。 |
| `mq-storage` | `FlushPolicyTest` | Unit / 单元测试 | Flush policy decisions and invalid storage config. / 刷盘策略判断与非法存储配置。 |
| `mq-storage` | `LogManagerFlushPolicyTest` | Unit / 单元测试 | Append-count checkpoint cadence, explicit checkpoint refresh, concurrent append offsets, and close behavior. / 按 append 次数触发 checkpoint、显式 checkpoint 刷新、并发 append offset 与关闭行为。 |
| `mq-storage` | `SegmentedLogConfigTest` | Unit / 单元测试 | Segment size and sparse-index interval validation. / Segment 大小与稀疏索引间隔校验。 |
| `mq-storage` | `PartitionLogRecordCodecTest` | Unit / 单元测试 | Shared partition-record framing, invalid offsets, invalid lengths, and incomplete payloads. / 共享分区记录帧格式、非法 offset、非法长度与不完整 payload。 |
| `mq-storage` | `SparseOffsetIndexTest` | Unit / 单元测试 | Sparse index lookup, reload, replacement, invalid entries, closed behavior, and corrupt index rejection. / 稀疏索引查找、重载、替换、非法条目、关闭行为与损坏索引拒绝。 |
| `mq-storage` | `SegmentCatalogStoreTest` | Unit / 单元测试 | Segment catalog safe write/load, missing/corrupt catalog handling, and descriptor validation. / Segment catalog 安全写入/加载、缺失/损坏 catalog 处理和 descriptor 校验。 |
| `mq-storage` | `SegmentFileTest` | Unit / 单元测试 | Segment file append/read budgets, corrupt index rebuild, missing logs, non-monotonic offsets, and closed behavior. / Segment 文件追加、读取预算、损坏索引重建、缺失日志、非单调 offset 与关闭行为。 |
| `mq-storage` | `SegmentedPartitionLogTest` | Unit / 单元测试 | Segment rolling, cross-segment reads, index rebuild, active-tail truncation, sealed-segment corruption rejection, idempotent replay, and overlapping catalog rejection. / Segment 滚动、跨 segment 读取、索引重建、活跃尾部截断、sealed segment 损坏拒绝、幂等重放和重叠 catalog 拒绝。 |
| `mq-storage` | `LogManagerSegmentRecoveryTest` | Unit / 单元测试 | Legacy single-file migration, WAL/checkpoint replay on segmented layout, legacy cleanup, and interrupted staging cleanup. / 旧单文件迁移、新分段布局上的 WAL/checkpoint 重放、旧文件清理和中断 staging 清理。 |
| `mq-storage` | `LogManagerConfigTest` | Unit / 单元测试 | Maintenance policy validation and compatibility constructors. / 维护策略校验与兼容构造器。 |
| `mq-storage` | `RetentionPlannerTest` | Unit / 单元测试 | Time and size retention selection, combined time/byte accounting, and active-segment protection. / 按时间和容量选择保留删除目标、组合时间/容量计数，并保护活跃 segment。 |
| `mq-storage` | `SegmentedPartitionLogRetentionTest` | Unit / 单元测试 | Sealed-segment retention, catalog `logStartOffset`, file cleanup, and replay after retained offsets are removed. / sealed segment 保留、catalog `logStartOffset`、文件清理，以及保留 offset 被删除后的重放。 |
| `mq-storage` | `SegmentCompactorTest` | Unit / 单元测试 | Latest-key compaction planning, null-key preservation, and bounded segment rewrites. / 最新 key 压缩规划、null-key 保留和有界 segment 重写。 |
| `mq-storage` | `SegmentedPartitionLogCompactionTest` | Unit / 单元测试 | Sealed-only compaction, atomic catalog publication, interrupted replacement recovery, cleaned-file cleanup, and offset-hole reads. / 仅对 sealed segment 压缩、原子 catalog 发布、中断替换恢复、cleaned 文件清理和 offset 空洞读取。 |
| `mq-storage` | `SegmentedPartitionLogSnapshotTest` | Unit / 单元测试 | Concurrent read safety, reader-aware maintenance skips, retry after readers drain, and startup cleanup for interrupted maintenance files. / 并发读取安全、reader 感知维护跳过、reader 退出后的重试，以及中断维护文件的启动清理。 |
| `mq-storage` | `LogManagerMaintenanceTest` | Unit / 单元测试 | Accurate dry-run ordering, action/byte/offset reports, metrics, reader skips, UTC windows, manual execution, checkpointing, and WAL replay after retention or compaction holes. / 精确 dry-run 顺序、动作/字节/offset 报告、指标、reader 跳过、UTC 窗口、手动执行、checkpoint，以及 retention 或 compaction 空洞后的 WAL replay。 |
| `mq-protocol` | `ProtocolSerdesTest` | Unit / 单元测试 | Request/response round trips, malformed collection counts, frame limits, encoder. / 请求/响应往返、畸形集合 count、帧限制和编码器。 |
| `mq-broker` | `BrokerConfigTest` | Unit / 单元测试 | Broker argument parsing and validation, including flush/checkpoint, segment, retention, compaction, and maintenance options. / Broker 参数解析与校验，包括刷盘、checkpoint、segment、保留策略、压缩和维护选项。 |
| `mq-broker` | `MetadataStoreTest` | Unit / 单元测试 | Metadata safe writes, load validation, corrupt-file failure, and quarantine recovery. / 元数据安全写入、加载校验、损坏文件失败与隔离恢复。 |
| `mq-broker` | `ClusterControllerTest` | Unit / 单元测试 | Topic creation, metadata, snapshot isolation. / Topic 创建、元数据、快照隔离。 |
| `mq-broker` | `GroupCoordinatorTest` | Unit / 单元测试 | Join/sync assignment, heartbeat, offset commit/fetch. / 加组、同步分配、心跳、offset 提交/拉取。 |
| `mq-broker` | `EmbeddedBrokerChannelTest` | Component / 组件测试 | Handler behavior through Netty `EmbeddedChannel`. / 通过 Netty `EmbeddedChannel` 验证 handler 行为。 |
| `mq-broker` | `ProduceFetchIntegrationTest` | Integration / 集成测试 | Real broker produce/fetch and not-leader behavior. / 真实 Broker 生产、拉取与非 leader 行为。 |
| `mq-broker` | `TcpSmokeTest` | Smoke / 冒烟测试 | Basic TCP request/response without noisy stdout. / 无冗余 stdout 的基础 TCP 请求/响应。 |
| `mq-broker` | `BrokerShutdownTest` | Integration / 集成测试 | Broker stop/start resource cleanup on the same port. / Broker 在同一端口停止、重启与资源释放。 |
| `mq-client` | `ClientValidationTest` | Unit / 单元测试 | Bootstrap validation and unconnected close. / bootstrap 校验与未连接关闭。 |
| `mq-client` | `NetworkClientRetryTest` | Unit / 单元测试 | Connect retry attempt budget and fixed backoff validation. / 连接重试次数预算与固定退避参数校验。 |
| `mq-client` | `ProducerIntegrationTest` | Integration / 集成测试 | Producer offsets increase across sends. / Producer 多次发送后 offset 递增。 |
| `mq-client` | `ConsumerIntegrationTest` | Integration / 集成测试 | Consumer poll and commit behavior. / Consumer poll 与提交行为。 |
| `mq-demo` | `DemoCliTest` | Component/Integration / 组件/集成测试 | CLI validation and create/produce/consume path. / CLI 校验与创建/生产/消费链路。 |
| `mq-demo` | `MiniMqE2ETest` | E2E / 端到端测试 | Three-broker cluster with two consumers. / 三节点 Broker 集群与两个 Consumer。 |
| `mq-demo` | `MiniMqWebAppTest` | Web integration / Web 集成测试 | Static assets, real Web flow, mock fallback, HTTP errors, runtime reuse, concurrent request isolation, stale runtime rebuild, and LRU runtime eviction. / 静态资源、真实 Web 主流程、mock fallback、HTTP 错误、runtime 复用、并发请求隔离、旧 runtime 重建和 LRU runtime 淘汰。 |

### 7.1 V2.0 Storage Recovery Closeout / V2.0 存储恢复收口

The V2.0 storage recovery closeout is recorded in `docs/test-report-2026-07-10-v2-storage.md`.
V2.0 存储恢复收口记录见 `docs/test-report-2026-07-10-v2-storage.md`。

### 7.2 V2.1.0 Segmented Log Closeout / V2.1.0 分段日志收口

The V2.1.0 segmented-log closeout is recorded in `docs/test-report-2026-07-15-v2.1.0-segmented-log.md`.
V2.1.0 分段日志收口记录见 `docs/test-report-2026-07-15-v2.1.0-segmented-log.md`。

### 7.3 V2.1.1 Retention And Compaction Closeout / V2.1.1 保留策略与压缩收口

The V2.1.1 retention and compaction closeout is recorded in `docs/test-report-2026-07-15-v2.1.1-retention-compaction.md`.
V2.1.1 保留策略与压缩收口记录见 `docs/test-report-2026-07-15-v2.1.1-retention-compaction.md`。

### 7.4 Web Runtime Regression Coverage / Web Runtime 回归覆盖

The Web backend has dedicated regression tests for the three runtime risks fixed in the current version.

Web 后端已为当前版本修复的三个 runtime 风险补充专门回归测试。

| Test / 测试 | Why It Exists / 为什么要测 | Verified Behavior / 验证行为 |
| --- | --- | --- |
| `isolatesConcurrentRealFlowRequestsOnSharedRuntime` | A shared Consumer can otherwise let two simultaneous requests consume each other's messages. / 共享 Consumer 否则可能让两个并发请求消费到对方消息。 | Two concurrent real-mode requests on the same runtime each receive their own `consumedValue`. / 同一 runtime 上两个并发 real 请求分别拿到自己的 `consumedValue`。 |
| `rebuildsCachedRuntimeAfterRuntimeFailure` | A closed runtime used to stay cached and could make later requests timeout. / 已关闭 runtime 曾可能继续留在缓存里，导致后续请求超时。 | Closing the cached runtime causes the next real request to evict, rebuild, retry, and still return `mode=real`. / 关闭缓存 runtime 后，下一次真实请求会驱逐、重建、重试，并仍返回 `mode=real`。 |
| `evictsRuntimeCacheWhenLimitIsExceeded` | Runtime cache without a cap can leak connections and heartbeat threads during exploratory Web testing. / 无上限 runtime 缓存在探索式 Web 测试中会泄露连接和心跳线程。 | Creating more than 4 distinct runtimes keeps `runtimeCount()` at the configured cap. / 创建超过 4 个不同 runtime 后，`runtimeCount()` 保持在配置上限。 |

Latest focused verification command / 最近一次聚焦验证命令：

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am '-Dtest=MiniMqWebAppTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
```

Latest focused result / 最近一次聚焦结果：

| Test class / 测试类 | Tests / 数量 | Failures / 失败 | Errors / 错误 | Result / 结果 |
| --- | ---: | ---: | ---: | --- |
| `MiniMqWebAppTest` | 10 | 0 | 0 | Passed / 通过 |

## 8. Test Data And Report Locations / 测试数据与报告位置

Surefire text and XML reports / Surefire 文本和 XML 报告：

```text
mq-common/target/surefire-reports
mq-protocol/target/surefire-reports
mq-storage/target/surefire-reports
mq-broker/target/surefire-reports
mq-client/target/surefire-reports
mq-demo/target/surefire-reports
```

JaCoCo HTML, XML, and CSV reports / JaCoCo HTML、XML 和 CSV 报告：

```text
mq-common/target/site/jacoco
mq-protocol/target/site/jacoco
mq-storage/target/site/jacoco
mq-broker/target/site/jacoco
mq-client/target/site/jacoco
mq-demo/target/site/jacoco
```

## 9. Coverage Gate / 覆盖率门槛

The current agreed gate is:

当前约定门槛：

```text
Per-module line coverage >= 90% for modules covered by JaCoCo check / 接入 JaCoCo check 的模块行覆盖率均 >= 90%
```

The project currently satisfies this gate at 91.97% overall line coverage, with JaCoCo `check` bound to Maven `verify`. Blocking process entrypoints (`BrokerMain`, `MiniMqWebMain`, and `MiniMqWebDevMain`) are excluded from the report and gate because their behavior is covered indirectly through CLI, broker integration, and Web launcher tests.

项目当前总体行覆盖率为 91.97%，并且 JaCoCo `check` 已绑定到 Maven `verify`。阻塞式进程入口（`BrokerMain`、`MiniMqWebMain`、`MiniMqWebDevMain`）的行为已通过 CLI、Broker 集成测试和 Web 启动器测试间接覆盖，因此从报告和门禁统计中排除。

## 10. Slow Tests / 慢测试说明

The slowest tests are integration and E2E tests that start brokers, open Netty channels, and wait for consumer poll windows. They are valuable because they catch wiring issues that pure unit tests cannot.

耗时较长的测试主要是启动 Broker、打开 Netty 通道、等待 Consumer poll 窗口的集成/端到端测试。这类测试价值较高，因为它们能发现纯单元测试无法覆盖的模块连接问题。

| Test / 测试 | Reason for cost / 耗时原因 |
| --- | --- |
| `MiniMqE2ETest` | Starts a three-broker cluster and two consumers. / 启动三节点 Broker 集群和两个 Consumer。 |
| `ProduceFetchIntegrationTest` | Starts real brokers and verifies leader routing. / 启动真实 Broker 并验证 leader 路由。 |
| `ConsumerIntegrationTest` | Includes subscribe, heartbeat scheduling, poll, and commit. / 包含订阅、心跳调度、poll 和提交。 |
| `DemoCliTest` | Runs CLI create/produce/consume against a real test broker. / 通过真实测试 Broker 运行 CLI 创建、生产、消费流程。 |

## 11. Terms / 测试名词解释

| Term / 术语 | 中文解释 | English explanation / 英文解释 |
| --- | --- | --- |
| Unit test | 只测试小范围类或函数，不依赖真实网络或完整系统。 | A small-scope test of a class/function without full system dependencies. |
| Integration test | 多模块协作测试，可能使用真实网络、本地文件、测试 Broker。 | A test that verifies multiple modules working together. |
| E2E test | 从用户入口到系统输出的端到端路径测试。 | A test that exercises a complete user-facing workflow. |
| Coverage | 测试执行过的代码比例。 | The percentage of code executed by tests. |
| Branch coverage | 条件分支被测试覆盖的比例。 | Percentage of conditional branches executed. |
| Smoke test | 快速确认核心功能没有完全坏掉的测试。 | A quick test that confirms basic system health. |
| Fixture | 测试中预先准备的数据或环境。 | Test setup data or environment. |
